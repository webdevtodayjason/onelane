#!/usr/bin/env python3
"""OneLane — let two applications share one Tiiny without fighting over it.

    pip install nothing.  Copy this file next to yours.

WHY THIS EXISTS
---------------
A Tiiny Pocket performs ONE inference at a time. Issue a second call while another
is in flight and the device answers:

    {"code": 150004, "message": "The operation failed to complete."}

Inside a single program that is easy — queue your own calls. It stops being easy the
moment a second program touches the same device, because your queue and its queue know
nothing about each other. Two well-behaved applications, each perfectly serialised on
its own, will still collide constantly.

We learned this running a 24/7 news board and a bedtime storyteller against one device.
This module is that lesson, extracted and cleaned up so nobody else has to rediscover it.

WHAT IT DOES
------------
* Serialises every device call across PROCESSES, not just threads, using an advisory
  file lock. If a holder crashes, the kernel releases the lock — no stale-lock cleanup,
  no heartbeat, no daemon to run.
* Retries the two failures that mean "busy, try again" — device code 150004 and HTTP
  502/503/504 — with exponential backoff and jitter.
* Optionally tracks the 100-unit model residency budget in a small shared ledger, so an
  application can ask "will my model fit?" before it evicts somebody else's.
* Finds the device without being told: TIINY_BASE, then ~/.tiinyapps/device.json from
  the farm, then TIINY_HOST. It then asks the box which port serves the gateway, because
  1.0 firmware refuses 8800 from another machine and serves everything on 80 instead.
* Names the lock file after the device's SERIAL NUMBER, not its address. An address is a
  DHCP lease and the same box also answers on its USB /30, so two apps reaching one
  device by two routes used to take out two locks and coordinate with nobody.

WHAT IT DOES NOT DO
-------------------
It is not a daemon, a scheduler, or a priority system. It is a onelane: one at a time,
first come first served, nobody gets stuck. That is deliberately the smallest thing that
solves the actual problem.

USAGE
-----
    from onelane import OneLane

    t = OneLane()                       # finds the device and the key by itself

    reply = t.chat("deepreinforce-ai/Ornith-1.0-35B",
                   [{"role": "user", "content": "Say hello"}])

    png = t.image("Tongyi-MAI/Z-Image-Turbo", "a lighthouse at dusk", seed=7)

    with t.hold("rendering a page"):       # several calls, nothing interleaves
        a = t.chat(...)
        b = t.image(...)

Everything blocks until the device is free. That is the point.
"""

import contextlib
import errno
import fcntl
import hashlib
import json
import os
import random
import socket
import stat
import sys
import tempfile
import threading
import types
import time
import urllib.error
import urllib.parse
import urllib.request
import weakref

__all__ = ["OneLane", "DeviceBusy", "DeviceError", "Budget", "who",
           "device_from_env"]

__version__ = "0.1.1"

# Unauthenticated device metadata. The one endpoint that answers the same way on
# every firmware and carries the serial number, which is the only name for a box
# that does not change when its address does.
DISCO_PORT = 39218

# What the farm hands an app when it gives it a box: {"base": ..., "key": ...}.
FARM_DEVICE = os.path.join(os.path.expanduser("~"), ".tiinyapps", "device.json")


def _farm_device():
    """What `farm device` wrote, or an empty dict."""
    try:
        with open(FARM_DEVICE, "rb") as fh:
            got = json.load(fh)
        return got if isinstance(got, dict) else {}
    except Exception:  # noqa: BLE001 - absent or unreadable is just "nothing there"
        return {}


def _split_base(base):
    """A base URL or a bare address as (host, explicit port or None).

    Accepts what the farm and the environment actually contain: "1.2.3.4",
    "http://1.2.3.4", "http://1.2.3.4:8800/v1", a hostname, or a hostname and a
    port. We want the host on its own because this module builds its own URLs.
    """
    base = (base or "").strip()
    if not base:
        return None, None
    if "//" not in base:
        base = "http://" + base
    try:
        parts = urllib.parse.urlsplit(base)
        return (parts.hostname or None), parts.port
    except ValueError:
        return None, None


def device_from_env():
    """(host, port, key) from the places a Tiiny's address is written down.

    TIINY_BASE is what the farm CLI exports, ~/.tiinyapps/device.json is what
    `farm device` writes, and TIINY_HOST is what this module documented before
    either of those existed. All three still work, in that order, so an app
    planted by the farm needs no configuration and an app that already sets
    TIINY_HOST keeps working untouched.
    """
    key = (os.environ.get("TIINY_KEY") or _farm_device().get("key") or "").strip()
    for value in (os.environ.get("TIINY_BASE"), _farm_device().get("base"),
                  os.environ.get("TIINY_HOST")):
        host, port = _split_base(value)
        if host:
            return host, port, key
    return None, None, key


def _gateway_port(host, timeout=2.0):
    """Which port serves the AI gateway on this device.

    Firmware 1.0.0 moved it. The gateway now binds 172.17.0.1:8800, the docker
    bridge only, and serves the same surface on port 80. Older firmware keeps it
    on 8800 and uses 80 for device management, so a plain TCP probe cannot tell
    the two apart - port 80 answers on both. Asking for an AI route can: the
    firmware that does not serve it 404s.

    TIINY_PORT overrides, for anyone who has put it somewhere else.
    """
    env = os.environ.get("TIINY_PORT")
    if env:
        return int(env)
    for port in (80, 8800):
        try:
            req = urllib.request.Request(
                "http://%s:%d/v1/models" % (host, port),
                headers={"Authorization": "Bearer probe"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                if r.status != 404:
                    return port
        except urllib.error.HTTPError as exc:
            if exc.code != 404:      # 401 still means the gateway is here
                return port
        except Exception:            # noqa: BLE001 - unreachable; try the next
            continue
    return 80


# Where the gateway used to be, kept only so an old caller that imports it still
# works. Nothing in this module reads it: leave port=None and the gateway is
# probed per host, because 1.0 firmware refuses 8800 from another machine.
DEFAULT_PORT = 8800
NPU_TOTAL = 100

# The two responses that mean "the device is busy", as opposed to "you asked for
# something impossible". 150004 is the device's own concurrency error. 502/503/504 is
# the same condition seen through the gateway — notably, a model that is still loading
# produces 502 for roughly twelve seconds after /start returns 200.
BUSY_DEVICE_CODE = 150004
BUSY_HTTP = (502, 503, 504)

def _default_lock_dir():
    """A directory BOTH applications can see, which is the whole point.

    Not tempfile.gettempdir(): on macOS that is per-user (/var/folders/...), so two
    applications running as different users would take out two different lock files and
    coordinate with nobody — silently, which is the worst way for a lock to fail. Real
    deployments do run as different users; a service account and a login account sharing
    one device is the normal case, not an exotic one. /tmp is shared on Linux and macOS
    alike, so prefer it and fall back only if it is unusable.
    """
    override = os.environ.get("ONELANE_DIR") or os.environ.get("TURNSTILE_DIR")
    if override:
        return override
    base = "/tmp" if os.path.isdir("/tmp") and os.access("/tmp", os.W_OK) \
        else tempfile.gettempdir()
    # A directory of our own, sticky like /tmp itself, so one user cannot delete or
    # rename another's lock file and a hostile pre-create is contained.
    den = os.path.join(base, "onelane")
    try:
        os.mkdir(den, 0o1777)
        os.chmod(den, 0o1777)                 # defeat the umask on the mode above
    except FileExistsError:
        pass
    except OSError:
        return base
    if os.path.isdir(den) and os.access(den, os.W_OK):
        return den
    return base


# One lock file per device, so two Tiinys never block each other. Set ONELANE_DIR when
# your processes are in containers that do not share /tmp.
LOCK_DIR = _default_lock_dir()


def _in_container():
    """Best-effort: are we inside a container? Cheap, and only ever used to explain."""
    if os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"):
        return True
    try:
        with open("/proc/1/cgroup", "rb") as fh:
            blob = fh.read(4096)
    except OSError:
        return False
    return any(m in blob for m in (b"docker", b"containerd", b"lxc", b"kubepods"))


def unshared_reason(path=None):
    """Why `path` is probably NOT the same directory the other processes see, or None.

    A lock coordinates only the processes that open the SAME FILE, and nothing about
    flock tells you whether they do. When the directory is private the lock does not
    fail - it succeeds, instantly, for everyone, and the collisions it was installed to
    prevent come back with no error to explain them. That is the worst way for a lock to
    break, and it has now happened twice in real deployments:

      * macOS gives each user their own /var/folders temp directory, so a service
        account and a login account silently took out two different locks.
      * systemd PrivateTmp=yes gives each unit its OWN mount namespace for /tmp, so two
        units of the same application silently took out two different locks.

    Both were found by reading unit files and namespace ids by hand, after the fact. This
    is the check that would have caught either one in a second.
    """
    p = os.path.realpath(path or LOCK_DIR)
    # realpath matters here: /var is a symlink to /private/var on macOS, so the per-user
    # temp directory resolves to /private/var/folders/... and a naive prefix match on
    # /var/folders/ misses the exact case this check exists for.
    if sys.platform == "darwin" and p.startswith(("/var/folders/", "/private/var/folders/")):
        return ("%s is macOS's PER-USER temp directory - processes running as a different "
                "user get a different one and will not see this lock. Set ONELANE_DIR to "
                "a path every participant can reach." % p)
    try:
        # Resolve /tmp rather than matching the literal string: it is a symlink to
        # /private/tmp on macOS, and realpath() above has already followed it.
        tmp = os.path.realpath("/tmp")
        if not (p == tmp or p.startswith(tmp + os.sep)):
            return None                       # not the default /tmp: not our business
        # A private mount namespace means our /tmp is not the /tmp anyone else has.
        if os.readlink("/proc/self/ns/mnt") != os.readlink("/proc/1/ns/mnt"):
            return ("this process is in a private mount namespace (systemd "
                    "PrivateTmp=yes), so %s is NOT the /tmp other units see. Set "
                    "ONELANE_DIR to a shared path - and on a hardened unit, grant it "
                    "with ReadWritePaths=." % p)
        # A container's entrypoint IS pid 1 of its own namespace, so the check above
        # matches and tells us nothing. /tmp is still private to the container.
        if _in_container():
            return ("this process is in a container, so %s is private to it and other "
                    "containers cannot see this lock. Set ONELANE_DIR to a path shared "
                    "between them (a bind mount or a shared volume)." % p)
    except OSError:
        pass                                  # no /proc: not Linux, nothing to check
    return None


def check_shared(path=None):
    """Raise if the lock directory cannot coordinate anybody. Call it at startup.

    Cheap enough to run on every boot, and it turns the silent failure above into a loud
    one before any work depends on the lock being real.
    """
    why = unshared_reason(path)
    if why:
        raise RuntimeError("onelane: this lock would coordinate nothing - " + why)
    return True


# The automatic check, evaluated once and then remembered: the answer cannot change
# while the process runs.
_auto_lock = threading.Lock()
_auto_reason = "unset"                        # sentinel: not computed yet
_auto_warned = False


def _auto_unshared(dirname):
    """The reason to complain about `dirname` automatically, or None.

    Deliberately silent when ONELANE_DIR was set explicitly. Someone who set it has
    thought about which directory their processes share; someone on the default has not,
    and is exactly who this protects. That single condition also removes the false alarms
    worth caring about - a hardened unit bind-mounting a shared directory under /tmp is
    doing precisely the right thing and must not be told otherwise.
    """
    global _auto_reason
    if _auto_reason != "unset":
        return _auto_reason
    with _auto_lock:
        if _auto_reason == "unset":           # recheck: another thread may have won
            _auto_reason = None if os.environ.get("ONELANE_DIR") or os.environ.get("TURNSTILE_DIR") \
                else unshared_reason(dirname)
    return _auto_reason

# The lock file must be openable by every user sharing the device. Default umask would
# create it 0644, so the second user to arrive gets PermissionError instead of a lock.
LOCK_MODE = 0o666

# The holder record is written at a fixed width so a reader never sees a partial one.
RECORD_BYTES = 512

# A dashboard read is bounded no matter what is in the queue directory.
MAX_WAITERS = 256


class DeviceBusy(RuntimeError):
    """The device stayed busy for the whole retry budget."""


class DeviceError(RuntimeError):
    """The device refused the request for a reason retrying will not fix."""


# --------------------------------------------------------------------------- #
# the lock
# --------------------------------------------------------------------------- #

_IDENTITY = {}


def _serial_path():
    """Where a learned serial is shared, beside the lock files it protects."""
    return os.path.join(_default_lock_dir(), "onelane-serials.json")


def _serial_known(addr):
    """A serial another process already learned for this address, or None."""
    try:
        with open(_serial_path(), "rb") as fh:
            got = json.load(fh)
        value = got.get(addr) if isinstance(got, dict) else None
        return value if isinstance(value, str) and value else None
    except Exception:  # noqa: BLE001 - absent, unreadable or corrupt is just "no"
        return None


def _serial_remember(addr, serial):
    """Publish addr -> serial where the other processes can read it.

    This file is a safety net, not a source of truth. Without it, one process
    that reached :39218 and one that did not would pick two different identities
    for the same box and coordinate with nobody, which is precisely the silent
    failure this module exists to prevent. Written atomically; every failure is
    ignored, because being unable to cache is not a reason to stop working.
    """
    if _serial_known(addr) == serial:
        return
    try:
        try:
            with open(_serial_path(), "rb") as fh:
                got = json.load(fh)
            got = got if isinstance(got, dict) else {}
        except Exception:  # noqa: BLE001
            got = {}
        got[addr] = serial
        path = _serial_path()
        fd, temporary = tempfile.mkstemp(prefix=".onelane-serials-",
                                         dir=os.path.dirname(path))
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(got, fh)
            # Readable by every user sharing the device, writable only by us: a
            # reader that cannot write simply re-probes.
            os.chmod(temporary, 0o644)
            os.replace(temporary, path)
        except Exception:  # noqa: BLE001
            try:
                os.unlink(temporary)
            except OSError:
                pass
    except Exception:  # noqa: BLE001
        pass


def _device_serial(addr, timeout=1.0, tries=2):
    """The serial number of the box at this address, or None.

    :39218/device.json is unauthenticated and answers on every firmware, so this
    costs one small request and needs no key. Tried twice, because a single blip
    deciding the identity of a lock is not a trade worth making.
    """
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(
                    "http://%s:%d/device.json" % (addr, DISCO_PORT),
                    timeout=timeout) as r:
                got = json.load(r)
            serial = got.get("serial_number") if isinstance(got, dict) else None
            if isinstance(serial, str) and serial.strip():
                return serial.strip()
            return None
        except Exception:  # noqa: BLE001
            if attempt + 1 >= tries:
                return None
    return None


def _device_identity(host):
    """Reduce however this box was reached to one identity per device.

    A lock coordinates only the processes that open the same file, so the name we
    hash has to be one-to-one with the hardware. Two things break that:

      * Spelling. 'localhost' and '127.0.0.1' are the same Tiiny, but as raw
        strings they hash to two lock files and coordinate with nobody, the
        silent failure again, the same shape as the per-user temp directory.
      * The address itself. A box's LAN address is a DHCP lease and it moves, and
        the same box answers on its USB /30 as well, so one device can present
        three different addresses in a week. An address is not an identity.

    So the serial number is the identity, read from :39218/device.json, and it is
    published for the other processes so a probe that fails in one of them cannot
    split the lock. Resolving the name pins the spellings together and is the
    fallback when the box will not say who it is: over-serialising two names that
    turn out to be one device only costs throughput, while under-serialising is a
    race.
    """
    got = _IDENTITY.get(host)
    if got is None:
        # Case and a trailing root dot are not distinctions the device knows about.
        name = (host or "").strip().rstrip(".").lower() or "127.0.0.1"
        try:
            addr = socket.gethostbyname(name)
        except (socket.gaierror, UnicodeError, OSError):
            addr = name
        serial = _device_serial(addr)
        if serial:
            _serial_remember(addr, serial)
        else:
            serial = _serial_known(addr)
        got = serial or addr
        _IDENTITY[host] = got
    return got


def _open_shared(path):
    """Open the lock file so any user sharing the device can lock it — carefully.

    The path is predictable and, by default, lives in a world-writable directory. That
    combination plus a chmod is a classic symlink attack: someone pre-creates the path
    as a link to a file you own, and your own process obligingly makes that file
    world-writable. O_NOFOLLOW stops the link being followed, and the mode is only ever
    changed on a plain file that we actually own with no other names pointing at it.

    A hostile pre-created file is refused loudly rather than retried forever, because
    the useful thing to tell someone in that situation is which path to move off.
    """
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, LOCK_MODE)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.EMLINK):
            raise DeviceError(
                "lock path %s is a symlink; refusing to follow it. "
                "Set ONELANE_DIR to a directory you control." % path)
        if exc.errno in (errno.EACCES, errno.EPERM):
            raise DeviceError(
                "cannot open lock file %s (permission denied). Another user may own it; "
                "set ONELANE_DIR to a directory you share deliberately." % path)
        raise
    try:
        # Age-based cleaners (systemd-tmpfiles, macOS periodic) delete /tmp files that
        # look untouched. Refreshing the timestamp on every acquire keeps an actively
        # used lock out of their way. It is a mitigation, not a guarantee — see the
        # README note on ONELANE_DIR for long-lived services.
        try:
            os.utime(fd, None)
        except OSError:
            pass
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise DeviceError("lock path %s is not a regular file" % path)
        # Only widen permissions on a file that is unambiguously ours. Someone else's
        # file either already allows this or is their decision to make, and a file with
        # extra hard links is not one we can reason about.
        if st.st_uid == os.geteuid() and st.st_nlink == 1 \
                and stat.S_IMODE(st.st_mode) != LOCK_MODE:
            try:
                os.fchmod(fd, LOCK_MODE)
            except OSError:
                pass
    except BaseException:
        os.close(fd)
        raise
    return fd


def _same_file(path, fd):
    """Does `path` still name the inode behind `fd`?"""
    try:
        a, b = os.stat(path), os.fstat(fd)
    except OSError:
        return False                          # the name is gone; ours is a stale inode
    return (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)


def _alive(pid):
    """Is that process still running? Signal 0 checks without disturbing it."""
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True                       # someone else's process, but it exists
    except OSError:
        return True                       # be conservative: do not erase a live waiter


def _holder_path(lock_path):
    return lock_path                      # the lock file itself carries the record


def _waiters_dir(lock_path):
    return lock_path + ".waiting"


def who(host=None, port=None, lock_key=None, path=None):
    """Who has the device right now, and who is queued behind them.

    Readers do not take the lock — they test it non-blockingly and read the record the
    holder wrote. That means a dashboard can poll this as often as it likes without ever
    delaying an actual inference, which is the only way a monitor is allowed to work.

        {"held": True, "owner": "daybreak", "pid": 8412, "why": "enrich #1471",
         "held_for_s": 4.2, "waiting": [{"owner": "reverie", "pid": 913, "for_s": 1.1}]}
    """
    if path is None:
        t = OneLane.__new__(OneLane)   # no connection, we only want the path
        t.host = host or device_from_env()[0] or "127.0.0.1"
        ident = lock_key or _device_identity(t.host)
        safe = "".join(c if c.isalnum() else "-" for c in ident).strip("-") or "device"
        key_id = "%s-%s" % (safe[:32], hashlib.sha1(ident.encode()).hexdigest()[:8])
        path = os.path.join(_default_lock_dir(), "onelane-%s.lock" % key_id)

    # A dashboard is where someone actually looks. stderr on a systemd unit goes to a
    # journal nobody tails until something is already broken, which is exactly how a
    # silently unshared lock stays silent. Surface it here so the board can render it.
    out = {"held": False, "owner": None, "pid": None, "why": None, "held_for_s": None,
           "waiting": [], "lock": path,
           "lock_warning": _auto_unshared(os.path.dirname(path) or ".")}
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return out                        # no lock file yet: nobody has ever used it
    try:
        try:
            rec = json.loads(os.read(fd, RECORD_BYTES).decode("utf-8", "replace") or "{}")
        except ValueError:
            rec = {}
    finally:
        os.close(fd)
    if not isinstance(rec, dict):
        rec = {}
    # Deliberately NO flock probe here. Testing the lock with LOCK_EX|LOCK_NB briefly
    # TAKES it whenever the device is free, so a polling dashboard made real callers see
    # contention and back off — measured at 6% of uncontended acquires failing first try
    # while a monitor ran. A monitor that perturbs what it measures is worse than no
    # monitor. Held is derived from the record instead: the holder clears it on release,
    # so a record that is still present belongs either to a live holder or to one that
    # died without cleaning up, and the pid tells us which.
    pid = rec.get("pid")
    try:
        pid = int(pid) if pid is not None else None
    except (TypeError, ValueError):
        pid = None
    if rec.get("owner") and _alive(pid):
        out["held"] = True
        out.update(owner=rec.get("owner"), pid=pid, why=rec.get("why"))
        try:
            out["held_for_s"] = round(time.time() - float(rec.get("since") or 0), 2)
        except (TypeError, ValueError):
            pass
    now = time.time()
    try:
        den = _waiters_dir(path)
        # The queue directory is world-writable, so its contents are untrusted input:
        # bounded in number, opened without following symlinks, and read with a size cap
        # so a planted file cannot turn a dashboard poll into a large read.
        for fn in sorted(os.listdir(den))[:MAX_WAITERS]:
            full = os.path.join(den, fn)
            try:
                wfd = os.open(full, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    raw = os.read(wfd, RECORD_BYTES)
                finally:
                    os.close(wfd)
                w = json.loads(raw.decode("utf-8", "replace") or "{}")
                if not isinstance(w, dict):
                    continue
                pid = int(w.get("pid") or 0)
            except (OSError, ValueError, TypeError):
                continue
            if not _alive(pid):
                # A waiter killed with -9 never runs its finally, so its file would
                # otherwise sit there forever and the queue would show a phantom. The
                # reader sweeps it: whoever notices the corpse buries it.
                try:
                    os.unlink(full)
                except OSError:
                    pass
                continue
            out["waiting"].append({"owner": w.get("owner"), "pid": pid,
                                   "for_s": round(now - float(w.get("since") or now), 2)})
    except OSError:
        pass
    out["waiting"].sort(key=lambda w: -(w["for_s"] or 0))
    out["queue"] = len(out["waiting"])
    return out


def _reset_after_fork():
    """A forked child inherits the parent's lock, which it does not own.

    The fd, the depth, the owner ident and the RLock all survive fork, and the child's
    fd refers to the SAME open file description — so the child believes it holds the
    device, and worse, when the child's nesting unwinds it drops the flock out from
    under a parent that is still mid-inference. The thread ident guard cannot catch this
    because the child's main thread reuses the parent's ident.

    Closing the child's copy of the descriptor does not disturb the parent's flock: the
    lock lives on the open file description and survives while the parent's fd is open.
    """
    for lk in list(_ALL_LOCKS):
        if lk._fh is not None:
            try:
                os.close(lk._fh)
            except OSError:
                pass
        lk._fh = None
        lk._depth = 0
        lk._owner = None
        lk._local = threading.RLock()       # the parent's RLock state is meaningless here


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


# The registry lives in sys.modules rather than in this module, because the shipping
# model is "copy this file next to yours". Two copies of the file in one program would
# otherwise mean two registries, two lock objects for one path, two open file
# descriptions — and the self-deadlock this registry exists to prevent, permanently,
# since call() waits forever by default.
_REG_NAME = "_turnstile_lock_registry_v1"
_registry = sys.modules.get(_REG_NAME)
if _registry is None:
    _registry = types.ModuleType(_REG_NAME)
    _registry.locks = {}
    _registry.guard = threading.Lock()
    _registry.all_locks = weakref.WeakSet()
    sys.modules[_REG_NAME] = _registry
_LOCKS = _registry.locks
_LOCKS_GUARD = _registry.guard
# Every lock ever built, so the fork handler can reset all of them. Weak, so a lock that
# goes out of scope is not kept alive by this. _LOCKS alone was not enough: a lock
# constructed directly rather than through _lock_for would have been missed.
_ALL_LOCKS = _registry.all_locks


def _lock_for(path):
    """One lock object per lock file per process — and it has to be exactly one.

    fcntl.flock is per open file description, not per process, so two lock objects in
    one program hold two descriptions and genuinely block each other. An application
    that builds a OneLane in two different modules would then deadlock against
    itself, forever, with the default of waiting indefinitely. Sharing the object makes
    that case reentrant instead, which is what a caller means by it.
    """
    with _LOCKS_GUARD:
        got = _LOCKS.get(path)
        if got is None:
            got = _LOCKS[path] = _CrossProcessLock(path)
        return got


class _CrossProcessLock:
    """An advisory file lock, reentrant within a process.

    fcntl.flock is per-file-descriptor and released by the kernel when the process
    exits, which is exactly the ownership semantics we want: a crashed application
    must not wedge the device for everybody else. It does mean the lock is advisory —
    a program that ignores OneLane still collides. That is a social problem, not a
    technical one.
    """

    def __init__(self, path, owner=None):
        self.path = path
        self.owner = owner or os.environ.get("ONELANE_OWNER") or os.environ.get("TURNSTILE_OWNER") or "app"
        self.why = None
        self._fh = None
        self._depth = 0
        self._owner = None
        self._local = threading.RLock()
        _ALL_LOCKS.add(self)

    def acquire(self, timeout=None):
        # Two gates: the in-process RLock keeps our own threads in line, the file lock
        # keeps us in line with everybody else. The timeout has to cover both or a
        # caller that asked to wait 5s can block forever behind a sibling thread.
        deadline = None if timeout is None else time.time() + timeout
        if not self._local.acquire(timeout=-1 if timeout is None else max(0.0, timeout)):
            return False
        if self._depth:                      # already ours; just nest
            self._depth += 1
            return True
        # Everything from here until _depth is set must hand the RLock back on the way
        # out, or one bad open() wedges this process for good. That includes the check
        # below: it goes INSIDE the try, so a raise from it cannot strand the RLock and
        # leave this object reading _depth=0, _fh=None while every other thread hangs.
        fh = None
        queued = None
        try:
            # A lock in a directory nobody else can see is worse than no lock at all: it
            # reports success while the device is being double-booked.
            why = _auto_unshared(os.path.dirname(self.path) or ".")
            if why:
                # Strict is evaluated EVERY time, not once. It is the one path that has
                # to fail closed, and a caller that catches the error and retries must
                # not be handed a working-looking lock on the second attempt.
                if os.environ.get("ONELANE_STRICT") or os.environ.get("TURNSTILE_STRICT"):
                    raise RuntimeError(
                        "onelane: this lock would coordinate nothing - " + why)
                global _auto_warned
                if not _auto_warned:
                    _auto_warned = True
                    sys.stderr.write(
                        "onelane: WARNING - this lock may coordinate nothing: %s\n" % why)
            fh = _open_shared(self.path)
            while True:
                try:
                    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    # The lock lives on the inode, not the name. A tmp cleaner can
                    # delete the file while a long-lived holder still has it open; the
                    # next process then creates a NEW inode at the same path, locks it
                    # happily, and two processes both believe they hold the device. A
                    # 24/7 service is exactly the profile this happens to, so confirm
                    # the name still points at the thing we just locked.
                    if not _same_file(self.path, fh):
                        fcntl.flock(fh, fcntl.LOCK_UN)
                        os.close(fh)
                        fh = _open_shared(self.path)
                        continue
                    self._fh = fh
                    self._owner = threading.get_ident()
                    self._depth = 1
                    self._stamp()
                    fh = None                # owned by self now; don't close it below
                    return True
                except OSError as exc:
                    if exc.errno not in (errno.EACCES, errno.EAGAIN):
                        raise
                    if deadline is not None and time.time() >= deadline:
                        return False
                    if queued is None:
                        # Only announce ourselves once we are genuinely blocked. An
                        # uncontended acquire should not touch the filesystem twice.
                        queued = self._queued()
                        queued.__enter__()
                    time.sleep(0.05 + random.random() * 0.05)
        finally:
            if queued is not None:
                queued.__exit__(None, None, None)
            if self._depth == 0:             # we are leaving without the lock
                if fh is not None:
                    os.close(fh)
                self._local.release()

    def _stamp(self):
        """Record who we are, inside the file we just locked.

        Writing under the lock means the record and the lock can never disagree. A stale
        record left by a crashed holder is harmless: readers only trust it when the file
        is actually locked, and it gets overwritten by the next holder.
        """
        self._write_record({"owner": self.owner, "pid": os.getpid(),
                            "why": self.why, "since": time.time()})

    def _write_record(self, rec):
        """Replace the holder record with a single fixed-width write.

        Padded to RECORD_BYTES and never truncated, so a reader can never catch the file
        empty or half-rewritten. ftruncate-then-write left exactly that window: a
        dashboard polling at the wrong microsecond saw an empty file and reported the
        device free while it was in use.
        """
        try:
            # default=str so a caller passing a Path, a UUID or anything else non-JSON
            # as `why` cannot raise here, and a bare `except Exception` behind it so
            # nothing else can either. This is not defensive padding: _stamp() runs
            # AFTER the flock is taken and _depth is set, so an exception escaping it
            # left acquire() holding the lock with no context manager to release it —
            # the device stayed wedged for every process until the holder died.
            # Observability must never break the lock; that has to be structurally
            # true, not merely intended.
            blob = json.dumps(rec, default=str).encode()[:RECORD_BYTES - 1]
            blob = blob + b" " * (RECORD_BYTES - len(blob))
            os.lseek(self._fh, 0, os.SEEK_SET)
            os.write(self._fh, blob)
        except Exception:
            pass

    @contextlib.contextmanager
    def _queued(self):
        """Announce that we are waiting, so a monitor can show the queue."""
        den = _waiters_dir(self.path)
        mine = None
        try:
            os.makedirs(den, 0o1777, exist_ok=True)
            try:
                os.chmod(den, 0o1777)
            except OSError:
                pass
            # Only publish into a directory we own. In a world-writable place the
            # path is guessable, and plain open() follows symlinks — a link planted at
            # <pid>-<tid>.json redirected this write into an arbitrary file the user
            # owned. The lock file already used O_NOFOLLOW; this one did not.
            if os.stat(den).st_uid != os.geteuid():
                raise OSError("queue directory belongs to someone else")
            mine = os.path.join(den, "%d-%d.json" % (os.getpid(), threading.get_ident()))
            blob = json.dumps({"owner": self.owner, "pid": os.getpid(),
                               "why": self.why, "since": time.time()},
                              default=str).encode()[:RECORD_BYTES]
            wfd = os.open(mine, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
            try:
                os.write(wfd, blob)
            finally:
                os.close(wfd)
        except Exception:
            mine = None
        try:
            yield
        finally:
            if mine:
                try:
                    os.remove(mine)
                except OSError:
                    pass

    def release(self):
        if not self._depth:
            return
        # Refuse before touching anything. Without this, a thread releasing a lock it
        # does not hold would unlock the FILE first and only then hit the RLock's own
        # RuntimeError — handing the device to everyone else while the real owner is
        # still mid-inference and believes it holds the lock.
        if self._owner != threading.get_ident():
            raise RuntimeError("release() from a thread that does not hold the lock")
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            self._write_record({})        # released: leave nothing claiming the device
            fh, self._fh = self._fh, None
            # No explicit LOCK_UN: closing the descriptor drops the flock, and the
            # separate unlock call was the only thing here that could raise. When it
            # did, this method exited before releasing the RLock, leaving the object
            # reading _depth=0 / _owner=None / _fh=None — completely free — while every
            # other thread in the process hung on it forever. Nothing between the
            # decrement and the RLock release is allowed to throw.
            try:
                os.close(fh)
            except OSError:
                pass
        self._local.release()

    @contextlib.contextmanager
    def held(self, timeout=None):
        if not self.acquire(timeout):
            raise DeviceBusy("could not take the device lock within %ss" % timeout)
        try:
            yield
        finally:
            self.release()


# --------------------------------------------------------------------------- #
# the residency ledger
# --------------------------------------------------------------------------- #

class Budget:
    """A shared view of the 100-unit model residency budget.

    The device is the source of truth; this is a cache plus a note of who asked for
    what, so an application can be polite instead of merely lucky. Ask `fits()` before
    loading a model and you stop being the app that evicts somebody's chat model in the
    middle of their sentence.
    """

    def __init__(self, onelane, path=None):
        self.t = onelane
        self.path = path or os.path.join(LOCK_DIR, "onelane-%s.budget.json" % onelane.key_id)

    def resident(self):
        """[(model_id, units, status)] straight from the device."""
        data = self.t.get("/api/v1/models/npu/status")
        return [(m.get("model_id"), int(m.get("npu_usage") or 0), m.get("status"))
                for m in (data.get("models") or [])]

    def used(self):
        return sum(u for _, u, s in self.resident() if s == "running")

    def free(self):
        return NPU_TOTAL - self.used()

    def fits(self, units):
        return self.free() >= int(units)

    def note(self, model_id, units, owner):
        """Record that `owner` wants `model_id` resident. Advisory, for humans."""
        # The whole read-modify-write needs to be exclusive. A unique temp name stops
        # two writers corrupting one file, but it does not stop a lost update: both read
        # the same book, both add their own entry, and the second rename discards the
        # first. Measured before this: 6 concurrent writers, 1 surviving entry.
        with _lock_for(self.path + ".lock").held(timeout=10):
            return self._note_locked(model_id, units, owner)

    def _note_locked(self, model_id, units, owner):
        try:
            with open(self.path) as fh:
                book = json.load(fh)
        except Exception:
            book = {}
        book[str(model_id)] = {"units": int(units), "owner": str(owner), "ts": time.time()}
        # mkstemp rather than a pid-derived name: a pid is not unique across threads in
        # one process, and two containers sharing a mounted ONELANE_DIR can both be
        # pid 1. Either way one writer renames a half-written file over the other's.
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.path) or ".",
                                   prefix=".onelane-budget-")
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(book, fh, indent=1)
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        return book


# --------------------------------------------------------------------------- #
# the onelane
# --------------------------------------------------------------------------- #

class OneLane:
    def __init__(self, host=None, key=None, port=None,
                 tries=6, base_delay=2.0, max_delay=30.0, timeout=300.0,
                 on_wait=None, lock_key=None, settle_s=60.0, owner=None):
        env_host, env_port, env_key = device_from_env()
        if host:
            # An explicit host means an explicit box; a port left over in
            # TIINY_BASE has nothing to do with it.
            self.host, env_port = host, None
        else:
            self.host = env_host or "127.0.0.1"
        self.key = key or env_key
        # None means "work it out": 1.0 firmware serves the gateway on port 80
        # behind a Host-header router and refuses 8800 from another machine,
        # older firmware serves it on 8800. A port passed here, or carried in
        # TIINY_BASE, pins it.
        if port is None:
            port = env_port or _gateway_port(self.host)
        self.port = int(port)
        self.base = "http://%s:%d" % (self.host, self.port)
        self.tries = int(tries)
        self.base_delay = float(base_delay)
        self.max_delay = float(max_delay)
        self.timeout = float(timeout)
        # After a read timeout we keep the device to ourselves until it answers again,
        # for at most this long. It bounds the damage when a request is genuinely lost:
        # without a cap a single dropped connection would wedge the device forever.
        self.settle_s = float(settle_s)
        # on_wait(attempt, delay, why) — hook so an application can say "device busy"
        # in its own UI instead of going silent.
        self.on_wait = on_wait
        # Identifies the lock FILE, so it has to be one-to-one with the device. Keeping
        # the readable host in the name helps when someone is staring at a temp dir, but
        # stripping the dots alone collides (192.0.2.177 and 192.0.21.77 both become
        # 1920217) and would make two separate devices queue behind each other.
        # The port is deliberately NOT included: one Tiiny exposes several ports and
        # they all contend for the same NPU, so they must share a lock.
        # Both halves must come from the RESOLVED identity. Deriving the readable half
        # from the spelling instead put 'localhost' and '127.0.0.1' on different paths
        # even though they hash the same, which quietly undid the whole fix.
        ident = lock_key or _device_identity(self.host)
        safe = "".join(c if c.isalnum() else "-" for c in ident).strip("-") or "device"
        self.key_id = "%s-%s" % (safe[:32], hashlib.sha1(ident.encode()).hexdigest()[:8])
        # A label so a monitor can say WHO has the device, not just that it is busy.
        self.owner = owner or os.environ.get("ONELANE_OWNER") or os.environ.get("TURNSTILE_OWNER") or "app"
        self._lock = _lock_for(os.path.join(_default_lock_dir(),
                                            "onelane-%s.lock" % self.key_id))
        self._lock.owner = self.owner
        self.budget = Budget(self)

    # -- plumbing ---------------------------------------------------------- #

    def _request(self, path, body=None, method=None, timeout=None, raw=False):
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": "Bearer " + self.key}
        if data:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
            blob = resp.read()
            ctype = resp.headers.get("Content-Type", "")
        if raw and "json" not in ctype:
            return blob
        try:
            return json.loads(blob or b"{}")
        except ValueError:
            return blob if raw else {}

    @staticmethod
    def _inband_error(payload):
        """The device reports failure with HTTP 200 and an error object in the body.

        Returns a message if this payload is one, else None. Without this, a declined
        image came back as a bytes blob that happened to be JSON and was handed to the
        caller as a PNG, and a declined chat quietly became an empty string. Both fail
        far away from the cause.
        """
        obj = payload
        if isinstance(obj, (bytes, bytearray)):
            if obj[:1] != b"{":
                return None                    # real binary, e.g. an actual PNG
            try:
                obj = json.loads(obj)
            except ValueError:
                return None
        if not isinstance(obj, dict):
            return None
        err = obj.get("error")
        if isinstance(err, dict):
            return str(err.get("message") or err)[:200]
        # A success body carries a payload key; an error body carries a nonzero code.
        if any(k in obj for k in ("choices", "data", "results", "models")):
            return None
        code = obj.get("code")
        try:
            if code is not None and int(code) != 0:
                return "code %s: %s" % (code, str(obj.get("message") or "")[:160])
        except (TypeError, ValueError):
            pass
        return None

    @staticmethod
    def _busy_reason(exc_or_payload):
        """Is this 'try again' or 'you are wrong'? Returns a reason string or None."""
        if isinstance(exc_or_payload, urllib.error.HTTPError):
            if exc_or_payload.code in BUSY_HTTP:
                return "http %d" % exc_or_payload.code
            return None
        if isinstance(exc_or_payload, (urllib.error.URLError, TimeoutError, OSError)):
            # A reset or a timeout mid-inference is indistinguishable from contention
            # from out here, and retrying is safe because inference has no side effects.
            # A name that does not resolve is different: it will still not resolve in
            # thirty seconds, so a typo in TIINY_HOST should fail now rather than after
            # the full backoff ladder.
            inner = getattr(exc_or_payload, "reason", exc_or_payload)
            if isinstance(inner, socket.gaierror):
                return None
            # Nothing is listening on that port. Six retries will not change that, and
            # the wrong port is a far more common cause than a device mid-restart.
            if isinstance(inner, OSError) and inner.errno == errno.ECONNREFUSED:
                return None
            return "transport: %s" % str(exc_or_payload)[:60]
        if isinstance(exc_or_payload, dict):
            code = exc_or_payload.get("code") or (exc_or_payload.get("error") or {}).get("code")
            try:
                if int(code) == BUSY_DEVICE_CODE:
                    return "device %d" % BUSY_DEVICE_CODE
            except (TypeError, ValueError):
                pass
        if isinstance(exc_or_payload, (bytes, bytearray)) and exc_or_payload[:1] == b"{":
            try:
                return OneLane._busy_reason(json.loads(exc_or_payload))
            except ValueError:
                return None
        return None

    @staticmethod
    def _is_timeout(exc):
        """Did we give up on the socket, as opposed to the device answering us?

        The distinction decides who owns the device during the backoff. A 150004 is the
        device replying 'not now' — it is idle and the next caller should have it. A
        read timeout means our request is very likely STILL RUNNING on the device, and
        letting go would hand the lock to somebody who then collides with our own
        in-flight inference.
        """
        inner = getattr(exc, "reason", None)
        return isinstance(exc, (TimeoutError, socket.timeout)) or \
            isinstance(inner, (TimeoutError, socket.timeout))

    def call(self, path, body=None, method="POST", raw=False, timeout=None):
        """One device call, serialised and retried. This is the whole library.

        Note what happens after a read timeout. Giving up on the socket does not stop
        the device, so our request is probably still running on it. From that moment we
        stay 'stuck to' the lock: we do not let go between retries, and — the part that
        is easy to get wrong — a subsequent 150004 no longer counts as evidence that the
        device is free, because the thing keeping it busy is most likely our own orphaned
        request. We hold until the device actually answers us or settle_s runs out.
        """
        last = None
        settled = False        # we have already paid the settle wait once
        held = False
        attempt = 0
        try:
            while attempt < self.tries:
                attempt += 1
                settle_now = False
                if not held:
                    self._lock.owner = self.owner
                    if self._lock.why is None:
                        self._lock.why = path
                    if not self._lock.acquire(timeout=None):
                        raise DeviceBusy("could not take the device lock")
                    held = True
                free_after = True          # may we hand the device on after this attempt?
                try:
                    out = self._request(path, body, method, timeout=timeout, raw=raw)
                    why = self._busy_reason(out)
                    if why is None:
                        bad = self._inband_error(out)
                        if bad:
                            raise DeviceError("device declined: %s" % bad)
                        return out
                    last = why
                    free_after = True
                except urllib.error.HTTPError as exc:
                    body_txt = b""
                    try:
                        body_txt = exc.read()[:400]
                    except Exception:
                        pass
                    why = self._busy_reason(exc) or self._busy_reason(body_txt)
                    if why is None:
                        raise DeviceError("HTTP %s: %s" % (exc.code, body_txt[:200].decode("utf-8", "replace")))
                    last = why
                    free_after = True
                except (urllib.error.URLError, TimeoutError, OSError) as exc:
                    why = self._busy_reason(exc)
                    if why is None:
                        raise
                    last = why
                    if self._is_timeout(exc) and not settled:
                        # Our request is probably still running on the device. Keep the
                        # lock and simply WAIT — do not re-issue, because a retry while
                        # the first one is still going is itself the collision we are
                        # trying to prevent. Once is enough; a second timeout after a
                        # full settle means something is properly wrong.
                        settled = True
                        settle_now = True
                        free_after = False
                    else:
                        free_after = True
                if free_after and held:
                    self._lock.release()
                    held = False
                if settle_now:
                    delay = self.settle_s
                else:
                    delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
                    delay += random.random() * (delay * 0.25)
                if self.on_wait:
                    try:
                        self.on_wait(attempt, delay,
                                     ("settling after timeout: " + str(last)) if settle_now else last)
                    except Exception:
                        pass
                time.sleep(delay)
        finally:
            if held:
                self._lock.release()
        raise DeviceBusy("device stayed busy across %d attempts (%s)" % (self.tries, last))

    def get(self, path, timeout=None):
        return self.call(path, None, method="GET", timeout=timeout)

    @contextlib.contextmanager
    def hold(self, why="", wait=None):
        """Hold the device across several calls so nothing interleaves.

        Use it when a sequence must not be broken up — writing a page and immediately
        illustrating it, say. Keep it short: everyone else is waiting on you.

        `wait` bounds how long we queue for our turn, not how long we may keep it.
        None waits forever; a number raises DeviceBusy if the door never opens.
        """
        prev = self._lock.why
        self._lock.owner, self._lock.why = self.owner, (why or None)
        try:
            with self._lock.held(timeout=wait):
                yield self
        finally:
            self._lock.why = prev

    # -- the calls people actually make ------------------------------------ #

    def chat(self, model, messages, max_tokens=800, temperature=0.2, **kw):
        """Chat completion. Returns the assistant text.

        NOTE the Ornith quirk this handles for you: the model puts its chain of thought
        in `reasoning_content`, and that COUNTS AGAINST max_tokens. Ask for a small
        budget and `content` comes back empty with finish_reason 'length'. We ask for a
        real budget and, if content is still empty, hand back the reasoning so the
        caller can salvage something rather than seeing a mysterious blank.
        """
        body = {"model": model, "messages": messages,
                "max_tokens": int(max_tokens), "temperature": temperature}
        body.update(kw)
        out = self.call("/v1/chat/completions", body)
        msg = ((out.get("choices") or [{}])[0].get("message") or {})
        return (msg.get("content") or "").strip() or (msg.get("reasoning_content") or "").strip()

    def embed(self, model, text):
        out = self.call("/v1/embeddings", {"model": model, "input": text})
        return (out.get("data") or [{}])[0].get("embedding")

    def rerank(self, model, query, documents):
        out = self.call("/v1/rerank", {"model": model, "query": query, "documents": documents})
        return out.get("results") or []

    def image(self, model, prompt, seed=0, negative_prompt="", steps=8, size=512):
        """Text to image. Returns PNG bytes.

        `size` is 512 and you should leave it there: on current firmware every other
        dimension fails with 150004 after about thirty seconds, which looks exactly
        like contention and is not.
        """
        return self.call("/v1/image/generate", {
            "model": model, "prompt": prompt, "negative_prompt": negative_prompt,
            "width": int(size), "height": int(size), "seed": int(seed), "steps": int(steps),
        }, raw=True)

    def speak(self, model, text, fmt="mp3"):
        """Text to speech. Returns audio bytes.

        Send no `voice` field for the CustomVoice model — it needs none, and its Base
        sibling rejects every speaker name we have been able to find.
        """
        return self.call("/v1/audio/speech",
                         {"model": model, "input": text, "response_format": fmt}, raw=True)

    # -- model residency --------------------------------------------------- #

    def start_model(self, model_id, wait=True, timeout=180.0):
        """Load a model, and actually wait for it.

        /start returns 200 within a fraction of a second and then loads asynchronously.
        A call issued immediately after hits the runtime mid-reallocation and comes back
        502 for roughly twelve seconds. So we poll until the device says 'running'.
        """
        self.call("/api/v1/models/%s/start" % model_id.replace("/", "%2F"), {})
        if not wait:
            return True
        deadline = time.time() + min(float(timeout), float(os.environ.get("ONELANE_START_TIMEOUT") or os.environ.get("TURNSTILE_START_TIMEOUT") or timeout))
        while time.time() < deadline:
            for mid, _units, status in self.budget.resident():
                if mid == model_id and status == "running":
                    return True
            time.sleep(3)
        return False

    def stop_model(self, model_id):
        self.call("/api/v1/models/%s/stop" % model_id.replace("/", "%2F"), {})

    @contextlib.contextmanager
    def borrowed(self, model_id, units=None):
        """Load a model for a piece of work and give the units back afterwards.

        If it was already resident when you arrived it is somebody else's and we leave
        it alone. This is how a transient model (speech, OCR) coexists with somebody
        else's permanent set instead of quietly stealing their budget.
        """
        mine = False
        try:
            # Decide and load under the lock. Read residency, check the budget and start
            # the model as one indivisible step, or two processes both conclude the model
            # is absent, both start it, and the first one to finish stops it under the
            # other. `mine` is set BEFORE the call that can partially succeed, so a load
            # that half-happens still gets cleaned up — that was the leak this context
            # manager existed to prevent.
            with self._lock.held(timeout=None):
                resident = dict((mid, status) for mid, _u, status in self.budget.resident())
                if resident.get(model_id) != "running":
                    if units is not None and not self.budget.fits(units):
                        raise DeviceError("only %du free; %s needs %du"
                                          % (self.budget.free(), model_id, units))
                    mine = True
                    if not self.start_model(model_id):
                        raise DeviceError("%s did not report running within the timeout"
                                          % model_id)
            yield self
        finally:
            if mine:
                try:
                    self.stop_model(model_id)
                except Exception:
                    pass


if __name__ == "__main__":
    import sys
    t = OneLane(on_wait=lambda a, d, why: print("  waiting %.1fs (%s)" % (d, why)))
    print("device:", t.base)
    try:
        for mid, units, status in t.budget.resident():
            print("  %-42s %3du  %s" % (mid, units, status))
        print("  free: %du of %d" % (t.budget.free(), NPU_TOTAL))
    except Exception as exc:
        print("  could not reach the device:", str(exc)[:120])
        sys.exit(1)
    if "--chat" in sys.argv:
        models = [m for m, _u, s in t.budget.resident() if s == "running"]
        if models:
            print("  chat ->", t.chat(models[0], [{"role": "user", "content": "Say hello in five words."}],
                                      max_tokens=400)[:120])


# Back-compat: the library was called Turnstile before 1.0.
Turnstile = OneLane
