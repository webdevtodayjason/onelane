#!/usr/bin/env python3
"""Prove the onelane actually turns.

    python3 tests/fake_device_test.py

A fake Tiiny that behaves like the real one in the way that matters: it accepts one
inference at a time and answers 150004 to anything that overlaps. Then we point real
worker PROCESSES at it, because in-process queuing is the easy half of the problem and
testing only that would prove nothing.

The suite includes a CONTROL that hammers the fake device without a onelane. If the
control does not collide, the test is not measuring anything and says so.
"""

import json
import os
import subprocess
import sys
import tempfile
import stat as _stat2
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import onelane as turnstile_mod
from onelane import OneLane, DeviceBusy, DeviceError, _CrossProcessLock  # noqa: E402

HOST, PORT = "127.0.0.1", 8899
WORK_S = 0.12          # how long a fake inference "takes"
SLOW_S = 1.5           # a call that outlives the client's patience


class Device:
    """Counters shared by the handler threads."""
    def __init__(self):
        self.lock = threading.Lock()
        self.inflight = 0
        self.peak = 0
        self.collisions = 0
        self.served = 0
        self.fail_next = 0        # force N busy replies, to exercise the retry path
        self.decline_image = False
        self.start_never_runs = False
        self.stopped = []

    def enter(self):
        with self.lock:
            if self.fail_next > 0:
                self.fail_next -= 1
                return False
            self.inflight += 1
            self.peak = max(self.peak, self.inflight)
            if self.inflight > 1:
                self.collisions += 1
                self.inflight -= 1
                return False
            return True

    def leave(self):
        with self.lock:
            self.inflight -= 1
            self.served += 1


DEV = Device()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, payload):
        blob = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)

    def do_GET(self):
        if self.path == "/stats":
            return self._send(200, {"peak": DEV.peak, "collisions": DEV.collisions,
                                    "served": DEV.served})
        if self.path == "/api/v1/models/npu/status":
            tts = "stopped" if DEV.start_never_runs else "stopped"
            return self._send(200, {"models": [
                {"model_id": "fake/chat-35B", "npu_usage": 50, "status": "running"},
                {"model_id": "fake/tts", "npu_usage": 7, "status": tts}]})
        self._send(404, {"error": "nope"})

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(n)
        if self.path.startswith("/api/v1/models/"):
            if self.path.endswith("/stop"):
                DEV.stopped.append(self.path.split("/")[-2].replace("%2F", "/"))
            return self._send(200, {"ok": True})
        if self.path == "/v1/slow":
            # Occupies the device for longer than the client's own timeout, which is
            # the whole point: the caller stops waiting while the device keeps working.
            if not DEV.enter():
                return self._send(200, {"code": 150004, "message": "busy"})
            try:
                time.sleep(SLOW_S)
            finally:
                DEV.leave()
            return self._send(200, {"ok": True})
        if self.path == "/v1/image/generate":
            # The real device answers with raw PNG bytes, or a JSON body when it
            # declines. Both shapes have to reach the caller correctly.
            if DEV.decline_image:
                return self._send(200, {"code": 400, "message": "unsupported size"})
            png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(png)))
            self.end_headers()
            return self.wfile.write(png)
        if not DEV.enter():
            # exactly what a real Tiiny says when it is already thinking
            return self._send(200, {"code": 150004,
                                    "message": "The operation failed to complete."})
        try:
            time.sleep(WORK_S)
        finally:
            DEV.leave()
        self._send(200, {"choices": [{"message": {"content": "ok", "reasoning_content": ""}}]})


def stats():
    with urllib.request.urlopen("http://%s:%d/stats" % (HOST, PORT), timeout=5) as r:
        return json.load(r)


# --------------------------------------------------------------------------- #
# the worker, re-entered as a subprocess
# --------------------------------------------------------------------------- #

def _worker():
    mode, calls = sys.argv[2], int(sys.argv[3])
    ok = err = 0
    if mode == "timeout":
        # Give up on the socket well before the device finishes. The device is still
        # computing when we stop listening.
        t = OneLane(host=HOST, port=PORT, key="x", tries=1,
                      base_delay=0.1, max_delay=0.1, timeout=0.3, settle_s=1.9)
        try:
            t.call("/v1/slow", {})
        except Exception:
            pass
        print(json.dumps({"ok": 0, "err": 0}))
        return
    if mode == "follower":
        time.sleep(0.25)          # arrive while the first worker is timing out
        # tries=1 on purpose: no retry to paper over a collision. If this worker is let
        # in while the device is still busy, it fails, and that failure is the finding.
        t = OneLane(host=HOST, port=PORT, key="x", tries=1, base_delay=0.1)
        for _ in range(calls):
            try:
                t.chat("fake/chat-35B", [{"role": "user", "content": "hi"}])
                ok += 1
            except Exception:
                err += 1
        print(json.dumps({"ok": ok, "err": err}))
        return
    if mode == "onelane":
        t = OneLane(host=HOST, port=PORT, key="x", tries=8, base_delay=0.05, max_delay=0.4)
        for _ in range(calls):
            try:
                t.chat("fake/chat-35B", [{"role": "user", "content": "hi"}])
                ok += 1
            except DeviceBusy:
                err += 1
    else:                                   # control: no coordination at all
        for _ in range(calls):
            req = urllib.request.Request(
                "http://%s:%d/v1/chat/completions" % (HOST, PORT),
                data=b'{"model":"fake/chat-35B"}',
                headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    body = json.load(r)
                err += 1 if body.get("code") == 150004 else 0
                ok += 0 if body.get("code") == 150004 else 1
            except urllib.error.URLError:
                err += 1
    print(json.dumps({"ok": ok, "err": err}))


def spawn(mode, procs, calls):
    me = os.path.abspath(__file__)
    running = [subprocess.Popen([sys.executable, me, "--worker", mode, str(calls)],
                                stdout=subprocess.PIPE, text=True) for _ in range(procs)]
    out = []
    for p in running:
        stdout, _ = p.communicate(timeout=180)
        out.append(json.loads(stdout.strip().splitlines()[-1]))
    return {"ok": sum(o["ok"] for o in out), "err": sum(o["err"] for o in out)}


# --------------------------------------------------------------------------- #

def main():
    # Private lock dir. Check (g) asserts a file mode, and on a shared box a file left
    # by another user makes fchmod fail silently and the check flake for environmental
    # reasons. Also proves ONELANE_DIR is actually honoured.
    os.environ["ONELANE_DIR"] = tempfile.mkdtemp(prefix="onelane-suite-")
    os.environ["ONELANE_START_TIMEOUT"] = "2"   # keep the load-timeout check quick
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    time.sleep(0.2)
    failures = []

    def check(name, cond, detail=""):
        print("%-4s %s%s" % ("PASS" if cond else "FAIL", name, ("  " + detail) if detail else ""))
        if not cond:
            failures.append(name)

    # 1. CONTROL — no onelane. This must collide, or the harness proves nothing.
    print("\n-- control: 4 processes, no coordination --")
    DEV.peak = DEV.collisions = DEV.served = 0
    c = spawn("control", 4, 6)
    s = stats()
    check("control collides (harness is real)", s["collisions"] > 0,
          "%d collisions, %d rejected calls" % (s["collisions"], c["err"]))

    # 2. THE CLAIM — same load, through the onelane.
    print("\n-- onelane: 4 processes x 6 calls --")
    DEV.peak = DEV.collisions = DEV.served = 0
    t0 = time.time()
    r = spawn("onelane", 4, 6)
    el = time.time() - t0
    s = stats()
    check("no call was ever rejected", r["err"] == 0, "%d ok, %d failed" % (r["ok"], r["err"]))
    check("all 24 calls completed", r["ok"] == 24, "%d" % r["ok"])
    check("device never saw two at once", s["peak"] <= 1, "peak inflight %d" % s["peak"])
    check("zero collisions at the device", s["collisions"] == 0, "%d" % s["collisions"])
    check("throughput is serial, not stalled", el < 24 * WORK_S * 3.0,
          "%.1fs for 24 x %.2fs of work" % (el, WORK_S))

    # 3. Retry path: force the device to answer busy a few times.
    print("\n-- retry: device answers 150004 three times --")
    DEV.fail_next = 3
    waits = []
    t = OneLane(host=HOST, port=PORT, key="x", tries=8, base_delay=0.05, max_delay=0.4,
                  on_wait=lambda a, d, why: waits.append(why))
    got = t.chat("fake/chat-35B", [{"role": "user", "content": "hi"}])
    check("survived transient busy", got == "ok", "%d retries, reasons %s" % (len(waits), set(waits)))
    check("actually retried (on_wait fired)", len(waits) == 3, "%d waits" % len(waits))
    check("recognised it as device 150004",
          bool(waits) and all("150004" in w for w in waits), str(waits))

    # 4. Give up honestly rather than hanging forever.
    print("\n-- give up: device is busy for good --")
    DEV.fail_next = 999
    t2 = OneLane(host=HOST, port=PORT, key="x", tries=3, base_delay=0.02, max_delay=0.05)
    try:
        t2.chat("fake/chat-35B", [{"role": "user", "content": "hi"}])
        check("raises DeviceBusy instead of hanging", False, "returned normally")
    except DeviceBusy as exc:
        check("raises DeviceBusy instead of hanging", True, str(exc)[:60])
    DEV.fail_next = 0

    # 5. hold() keeps a sequence together.
    print("\n-- hold: a multi-call sequence is not interleaved --")
    DEV.peak = DEV.collisions = 0
    # The outsider must be a genuinely separate PROCESS. An in-process OneLane now
    # shares the same lock object by design, so probing with one would only re-test
    # threading.RLock -- it would pass with fcntl.flock deleted entirely, which is
    # exactly how this check silently stopped proving anything.
    lockpath = t._lock.path
    OUTSIDER = ("import fcntl,sys\n"
                "fh=open(sys.argv[1],'a+')\n"
                "try:\n"
                "  fcntl.flock(fh.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB); print('GOT')\n"
                "except OSError: print('BLOCKED')\n")
    with t.hold("a sequence"):
        during = subprocess.run([sys.executable, "-c", OUTSIDER, lockpath],
                                capture_output=True, text=True).stdout.strip()
        t.chat("fake/chat-35B", [{"role": "user", "content": "1"}])
        t.chat("fake/chat-35B", [{"role": "user", "content": "2"}])
    after = subprocess.run([sys.executable, "-c", OUTSIDER, lockpath],
                           capture_output=True, text=True).stdout.strip()
    check("another PROCESS is locked out during hold", during == "BLOCKED", during)
    check("and gets in once the hold ends", after == "GOT", after)

    # 6. A crashed holder must not wedge the device.
    print("\n-- crash: lock dies with the process that held it --")
    lockpath = t._lock.path
    kid = subprocess.Popen([sys.executable, "-c",
                            "import fcntl,sys,time\n"
                            "fh=open(%r,'a+')\n" % lockpath +
                            "fcntl.flock(fh.fileno(), fcntl.LOCK_EX)\n"
                            "print('held', flush=True)\n"
                            "time.sleep(30)\n"], stdout=subprocess.PIPE, text=True)
    kid.stdout.readline()
    blocked = _CrossProcessLock(lockpath).acquire(timeout=0.3)
    check("a live holder blocks others", blocked is False)
    kid.kill()
    kid.wait()
    freed = _CrossProcessLock(lockpath)
    check("killing the holder frees the device", freed.acquire(timeout=2.0) is True)
    freed.release()

    # 7. Budget reads residency off the device.
    print("\n-- budget --")
    check("counts only running models", t.budget.used() == 50, "used %du" % t.budget.used())
    check("reports free units", t.budget.free() == 50)
    check("fits() is honest", t.budget.fits(32) and not t.budget.fits(60))

    # 8. Regressions from review.
    print("\n-- regressions --")
    # (a) An unopenable lock file must not strand the in-process RLock. Before the fix
    #     the failed acquire kept the RLock, and every later call in this process hung.
    bad = _CrossProcessLock("/nonexistent-dir-%d/lock" % os.getpid())
    try:
        bad.acquire(timeout=0.2)
    except OSError:
        pass
    # Probe from ANOTHER thread: RLock is reentrant, so a same-thread acquire returns
    # True whether or not the lock leaked, and the check would pass with the fix removed.
    leaked = {"free": None}

    def probe():
        leaked["free"] = bad._local.acquire(timeout=0.5)
        if leaked["free"]:
            bad._local.release()

    pth = threading.Thread(target=probe); pth.start(); pth.join()
    check("failed acquire releases the in-process lock", leaked["free"] is True,
          "another thread would hang forever if it leaked")

    # (b) A hostname that cannot resolve is not 'busy'. It must fail now, not after the
    #     whole backoff ladder.
    t3 = OneLane(host="no-such-device.invalid", key="x", tries=6, base_delay=2.0)
    t0 = time.time()
    try:
        t3.chat("m", [{"role": "user", "content": "hi"}])
        check("unresolvable host fails fast", False, "returned normally")
    except DeviceBusy:
        check("unresolvable host fails fast", False, "retried a permanent failure")
    except Exception:
        el2 = time.time() - t0
        check("unresolvable host fails fast", el2 < 2.0, "%.2fs" % el2)

    # (c) Two OneLane objects in ONE process must not deadlock against each other.
    #     flock is per open file description, so before the fix each object held its own
    #     description and the second call blocked forever on the default timeout=None.
    print("")
    a = OneLane(host=HOST, port=PORT, key="x")
    b = OneLane(host=HOST, port=PORT, key="x")
    check("two instances share one lock", a._lock is b._lock)
    a._lock.acquire()
    try:
        check("second instance is reentrant, not deadlocked",
              b._lock.acquire(timeout=2.0) is True, "would hang forever if separate")
        b._lock.release()
    finally:
        a._lock.release()
    got = b.chat("fake/chat-35B", [{"role": "user", "content": "hi"}])
    check("device still usable afterwards", got == "ok")

    # (d) Different devices must not share a lock file. Stripping punctuation out of the
    #     host collided (192.0.2.177 and 192.0.21.77 both became 1920217), which
    #     silently serialised two unrelated Tiinys against each other.
    #
    #     These hosts do not exist, so the serial probe would spend its timeout on each
    #     one before falling back to the address. Stub it out: what the probe does with
    #     real hardware is checked separately, below and on a device.
    real_serial = turnstile_mod._device_serial
    turnstile_mod._device_serial = lambda addr, timeout=1.0, tries=2: None
    turnstile_mod._IDENTITY.clear()
    one = OneLane(host="192.0.2.177", key="x")
    two = OneLane(host="192.0.21.77", key="x")
    check("different hosts get different locks", one._lock.path != two._lock.path,
          os.path.basename(one._lock.path))
    check("same host still shares a lock",
          OneLane(host="192.0.2.177", key="x")._lock is one._lock)
    # Spelling must not key the lock: one app setting TIINY_HOST=localhost while the
    # other leaves it at the 127.0.0.1 default is the most likely real-world hit.
    check("localhost and 127.0.0.1 are the same device",
          OneLane(host="localhost", key="x")._lock is OneLane(host="127.0.0.1", key="x")._lock)
    check("case and a trailing dot do not split the lock",
          OneLane(host="LOCALHOST.", key="x")._lock is OneLane(host="localhost", key="x")._lock)
    check("lock_key overrides identity when you need it",
          OneLane(host="a.invalid", key="x", lock_key="dev1")._lock is
          OneLane(host="b.invalid", key="x", lock_key="dev1")._lock)
    check("different ports on one device share a lock",
          OneLane(host="192.0.2.177", port=9098, key="x")._lock is one._lock,
          "one NPU, so they must queue together")

    # (d2) The serial is the identity, not the address. A box's LAN address is a DHCP
    #      lease and it moves, and the same box answers on its USB /30 as well, so two
    #      apps reaching one device by two routes used to take out two lock files and
    #      coordinate with nobody. Firmware 1.0 made that likely rather than theoretical.
    print("")
    SERIALS = {"192.0.2.10": "TNY-SAME", "192.0.2.11": "TNY-SAME",
               "192.0.2.12": "TNY-OTHER"}
    turnstile_mod._device_serial = \
        lambda addr, timeout=1.0, tries=2: SERIALS.get(addr)
    turnstile_mod._IDENTITY.clear()
    lan = OneLane(host="192.0.2.10", key="x", port=80)     # the box on Wi-Fi
    usb = OneLane(host="192.0.2.11", key="x", port=80)     # the same box over USB
    other = OneLane(host="192.0.2.12", key="x", port=80)   # a different box
    check("two addresses for one serial share a lock", lan._lock is usb._lock,
          os.path.basename(lan._lock.path))
    check("a second serial still gets its own lock", other._lock is not lan._lock,
          os.path.basename(other._lock.path))
    check("the serial, not the address, names the lock file",
          "TNY-SAME" in os.path.basename(lan._lock.path))

    # A process that cannot reach :39218 must land on the same lock as one that can,
    # or the lock silently stops coordinating. That is what the shared serial file is
    # for, and it is the whole reason this change is safe to make.
    turnstile_mod._device_serial = lambda addr, timeout=1.0, tries=2: None
    turnstile_mod._IDENTITY.clear()
    blind = OneLane(host="192.0.2.10", key="x", port=80)
    check("a failed probe reuses the serial another process published",
          blind._lock is lan._lock, os.path.basename(blind._lock.path))

    turnstile_mod._device_serial = real_serial
    turnstile_mod._IDENTITY.clear()

    # (d3) The address comes from the farm before it comes from TIINY_HOST, so an app
    #      the farm planted needs no configuration.
    farm = os.path.join(os.environ["ONELANE_DIR"], "farm-device.json")
    with open(farm, "w") as fh:
        json.dump({"base": "http://192.0.2.44:8800/v1", "key": "farm-key"}, fh)
    saved_farm, saved_host, saved_key = (
        turnstile_mod.FARM_DEVICE, os.environ.pop("TIINY_HOST", None),
        os.environ.pop("TIINY_KEY", None))
    try:
        turnstile_mod.FARM_DEVICE = farm
        host, port, key = turnstile_mod.device_from_env()
        check("the farm's device file supplies host, port and key",
              (host, port, key) == ("192.0.2.44", 8800, "farm-key"),
              "%s:%s" % (host, port))
        os.environ["TIINY_BASE"] = "http://192.0.2.55"
        host, _p, _k = turnstile_mod.device_from_env()
        check("TIINY_BASE wins over the farm's file", host == "192.0.2.55", host)
        del os.environ["TIINY_BASE"]
        os.environ["TIINY_HOST"] = "192.0.2.66"
        host, _p, _k = turnstile_mod.device_from_env()
        check("the farm's file wins over TIINY_HOST", host == "192.0.2.44", host)
    finally:
        turnstile_mod.FARM_DEVICE = saved_farm
        os.environ.pop("TIINY_BASE", None)
        os.environ.pop("TIINY_HOST", None)
        if saved_host is not None:
            os.environ["TIINY_HOST"] = saved_host
        if saved_key is not None:
            os.environ["TIINY_KEY"] = saved_key

    # (e) Binary responses must survive as bytes, and a declined image must raise
    #     rather than handing the caller a dict where a PNG was promised.
    print("")
    png = t.image("fake/img", "a lighthouse", seed=7)
    check("image returns real bytes", isinstance(png, bytes) and png[:8] == b"\x89PNG\r\n\x1a\n",
          "%d bytes" % len(png))
    DEV.decline_image = True
    try:
        t.image("fake/img", "a lighthouse", seed=7)
        check("declined image raises instead of returning a dict", False, "returned normally")
    except DeviceError as exc:
        check("declined image raises instead of returning a dict", True, str(exc)[:50])
    except Exception as exc:
        check("declined image raises instead of returning a dict", False, type(exc).__name__)
    DEV.decline_image = False

    # (f) A thread releasing a lock it does not hold must be refused BEFORE the file is
    #     unlocked, or the device is handed away while the real owner is still using it.
    print("")
    guard = {"raised": None, "still_held": None}

    def thief():
        try:
            t._lock.release()
            guard["raised"] = False
        except RuntimeError:
            guard["raised"] = True

    t._lock.acquire()
    th2 = threading.Thread(target=thief)
    th2.start(); th2.join()
    # the owner must still hold it: a fresh process must still be locked out
    probe = subprocess.run([sys.executable, "-c",
                            "import fcntl,sys\n"
                            "fh=open(%r,'a+')\n" % t._lock.path +
                            "try:\n"
                            "  fcntl.flock(fh.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB); print('GOT')\n"
                            "except OSError: print('BLOCKED')\n"], capture_output=True, text=True)
    guard["still_held"] = "BLOCKED" in probe.stdout
    t._lock.release()
    check("non-owner release is refused", guard["raised"] is True)
    check("device stayed locked through the attempt", guard["still_held"] is True,
          "would have been stolen mid-inference")

    # (g) The lock file has to be usable by EVERY user sharing the device. A default
    #     umask creates it 0644, so a second user gets PermissionError instead of a lock,
    #     and the two applications never coordinate at all.
    print("")
    import stat as _stat
    lp = t._lock.path
    t._lock.acquire(); t._lock.release()
    mode = _stat.S_IMODE(os.stat(lp).st_mode)
    check("lock file is group/other writable", mode == 0o666, "mode %o" % mode)
    check("lock dir is shared between users", not turnstile_mod.LOCK_DIR.startswith("/var/folders"),
          turnstile_mod.LOCK_DIR)

    # (h) THE ONE THAT MATTERS MOST. A client-side timeout does not mean the device
    #     stopped working — our request is very likely still running on it. Releasing
    #     the lock there hands it to a cooperating peer who then collides with our own
    #     in-flight inference. Without the fix this reproduces reliably.
    print("")
    DEV.peak = DEV.collisions = DEV.served = 0
    me = os.path.abspath(__file__)
    procs = [subprocess.Popen([sys.executable, me, "--worker", "timeout", "1"],
                              stdout=subprocess.PIPE, text=True),
             subprocess.Popen([sys.executable, me, "--worker", "follower", "1"],
                              stdout=subprocess.PIPE, text=True)]
    outs = []
    for pr in procs:
        so, _ = pr.communicate(timeout=180)
        outs.append(json.loads(so.strip().splitlines()[-1]))
    follower = outs[1]
    check("a peer is not let in while our request is still running",
          follower["err"] == 0 and follower["ok"] == 1,
          "follower ok=%d err=%d" % (follower["ok"], follower["err"]))

    # (i) fork(): the child inherits the fd, the depth, the owner ident and the same
    #     open file description. Without a reset it believes it holds the device, and
    #     its release would drop the PARENT's flock mid-inference.
    print("")
    fl = _CrossProcessLock(os.path.join(os.path.dirname(t._lock.path), "onelane-forkcheck.lock"))
    fl.acquire()
    r, w = os.pipe()
    kid = os.fork()
    if kid == 0:
        os.close(r)
        try:
            msg = "%d|%s|%s" % (fl._depth, fl._owner, fl.acquire(timeout=0.3))
        except BaseException as exc:
            msg = "raised|%s|%s" % (type(exc).__name__, exc)
        os.write(w, msg.encode())
        os._exit(0)
    os.close(w)
    kid_msg = os.read(r, 200).decode()
    os.waitpid(kid, 0)
    depth_s, owner_s, got_s = kid_msg.split("|")
    check("forked child does not inherit the lock", depth_s == "0" and owner_s == "None", kid_msg)
    check("forked child cannot take the held device", got_s == "False", kid_msg)
    probe2 = subprocess.run([sys.executable, "-c", OUTSIDER, fl.path],
                            capture_output=True, text=True).stdout.strip()
    check("parent still holds it after the child exits", probe2 == "BLOCKED", probe2)
    fl.release()

    # (j) The lock path is predictable and lives in a world-writable directory, so it
    #     must never be followed as a symlink and then chmod'ed.
    link = os.path.join(os.path.dirname(t._lock.path), "onelane-symlinkcheck.lock")
    target = link + ".victim"
    for f in (link, target):
        try:
            os.remove(f)
        except OSError:
            pass
    open(target, "w").close()
    os.chmod(target, 0o600)
    os.symlink(target, link)
    try:
        _CrossProcessLock(link).acquire(timeout=0.5)
        check("refuses to follow a symlinked lock path", False, "followed it")
    except DeviceError:
        check("refuses to follow a symlinked lock path", True)
    except OSError:
        check("refuses to follow a symlinked lock path", True, "OSError")
    victim_mode = _stat2.S_IMODE(os.stat(target).st_mode)
    check("the symlink target was not made world-writable", victim_mode == 0o600,
          "mode %o" % victim_mode)
    for f in (link, target):
        try:
            os.remove(f)
        except OSError:
            pass

    # (k) ONELANE_DIR is honoured, and honoured at construction rather than import.
    check("lock lives under ONELANE_DIR",
          t._lock.path.startswith(os.environ["ONELANE_DIR"]), t._lock.path)

    # (l) borrowed() must hand the units back even when the load half-fails. start_model
    #     used to sit outside the try, so a model that came up but never reported running
    #     leaked exactly the units this context manager exists to return.
    print("")
    DEV.start_never_runs = True
    try:
        with t.borrowed("fake/tts", units=7):
            pass
        check("a model that never comes up is reported", False, "no error raised")
    except DeviceError:
        check("a model that never comes up is reported", True)
    except Exception as exc:
        check("a model that never comes up is reported", False, type(exc).__name__)
    check("and its units are still handed back", "fake/tts" in DEV.stopped,
          "stopped=%s" % DEV.stopped)
    DEV.start_never_runs = False
    DEV.stopped = []

    # (m) note() is a read-modify-write, so concurrent writers must not lose entries.
    print("")
    b = t.budget
    b.path = os.path.join(os.environ["ONELANE_DIR"], "ledger.json")
    errs = []

    def writer(i):
        try:
            b.note("model/%d" % i, i, "owner%d" % i)
        except Exception as exc:
            errs.append(repr(exc))

    ths = [threading.Thread(target=writer, args=(i,)) for i in range(6)]
    for th3 in ths:
        th3.start()
    for th3 in ths:
        th3.join()
    with open(b.path) as fh:
        book = json.load(fh)
    check("six concurrent ledger writes all survive", len(book) == 6 and not errs,
          "%d of 6 kept, %d errors" % (len(book), len(errs)))

    # (n) who() lets a dashboard see the holder and the queue WITHOUT taking the lock,
    #     which is the only way a monitor is allowed to work: polling it must never
    #     delay a real inference.
    print("")
    w0 = turnstile_mod.who(host=HOST)
    check("who() reports a free device", w0["held"] is False and w0["queue"] == 0)
    holder = OneLane(host=HOST, port=PORT, key="x", owner="daybreak")
    with holder.hold("enrich #1471"):
        w1 = turnstile_mod.who(host=HOST)
        # a second PROCESS queues behind it
        WAITER = ("import os,sys,time\n"
                  "sys.path.insert(0,%r)\n" % os.path.dirname(os.path.dirname(os.path.abspath(__file__))) +
                  "os.environ['ONELANE_DIR']=%r\n" % os.environ["ONELANE_DIR"] +
                  "import onelane as T\n"
                  "t=T.OneLane(host=%r,port=%d,key='x',owner='reverie')\n" % (HOST, PORT) +
                  "t._lock.why='painting'\n"
                  "t._lock.acquire(timeout=3.0) and t._lock.release()\n")
        kid2 = subprocess.Popen([sys.executable, "-c", WAITER])
        time.sleep(1.0)
        w2 = turnstile_mod.who(host=HOST)
        kid2.wait(timeout=30)
    w3 = turnstile_mod.who(host=HOST)
    check("who() names the holder", w1["held"] and w1["owner"] == "daybreak", str(w1["owner"]))
    check("who() carries what it is doing", w1["why"] == "enrich #1471", str(w1["why"]))
    check("who() shows the queue", w2["queue"] >= 1 and
          any(x["owner"] == "reverie" for x in w2["waiting"]), str(w2["waiting"]))
    check("who() clears once released", w3["held"] is False and w3["queue"] == 0)

    # A waiter killed with -9 never runs its finally, so without a sweep its file sits
    # in the queue directory forever and the dashboard shows a waiter that does not
    # exist. Readers bury the corpse.
    den = turnstile_mod._waiters_dir(holder._lock.path)
    os.makedirs(den, exist_ok=True)
    ghost = os.path.join(den, "999999-1.json")
    with open(ghost, "w") as fh:
        json.dump({"owner": "ghost", "pid": 999999, "since": time.time()}, fh)
    w4 = turnstile_mod.who(host=HOST)
    check("a dead waiter is not counted", w4["queue"] == 0,
          "queue=%d %s" % (w4["queue"], w4["waiting"]))
    check("and its file is swept", not os.path.exists(ghost))

    # A monitor that perturbs what it measures is worse than no monitor. who() used to
    # probe with flock(LOCK_EX|LOCK_NB), which briefly TAKES the lock whenever the device
    # is free: measured 24 of 400 uncontended acquires failing first try while a
    # dashboard polled. It must now be provably passive.
    lk = holder._lock
    stop_poll = threading.Event()

    def poller():
        while not stop_poll.is_set():
            turnstile_mod.who(path=lk.path)

    pth2 = threading.Thread(target=poller, daemon=True)
    pth2.start()
    missed = 0
    for _ in range(300):
        if lk.acquire(timeout=0):
            lk.release()
        else:
            missed += 1
    stop_poll.set()
    pth2.join(timeout=5)
    check("polling who() never steals the lock", missed == 0, "%d/300 first-try misses" % missed)

    # THE WORST BUG THIS LIBRARY HAS HAD. _stamp() runs after the flock is taken and
    # _depth is set, so anything it raises escapes acquire() still holding the lock,
    # with no context manager left to release it. A caller passing a Path as `why` hit
    # json.dumps -> TypeError -> the device was wedged for every process on the machine
    # until that process died.
    import pathlib as _pl
    weird = OneLane(host=HOST, port=PORT, key="x", owner="weird")
    with weird.hold(why=_pl.Path("/tmp/page-42")):
        pass
    check("a non-serialisable `why` cannot wedge the device",
          weird._lock._depth == 0 and weird._lock._fh is None,
          "depth=%s fh=%s" % (weird._lock._depth, weird._lock._fh))
    probe3 = subprocess.run([sys.executable, "-c", OUTSIDER, weird._lock.path],
                            capture_output=True, text=True).stdout.strip()
    check("and the device is still usable afterwards", probe3 == "GOT", probe3)

    # The waiter file lives in a world-writable directory under a guessable name, so a
    # planted symlink must not redirect the write into a file the user owns.
    den3 = turnstile_mod._waiters_dir(weird._lock.path)
    os.makedirs(den3, exist_ok=True)
    victim = os.path.join(os.environ["ONELANE_DIR"], "VICTIM.txt")
    with open(victim, "w") as fh:
        fh.write("PRECIOUS")
    link = os.path.join(den3, "%d-%d.json" % (os.getpid(), threading.get_ident()))
    try:
        os.unlink(link)
    except OSError:
        pass
    os.symlink(victim, link)
    blocker = turnstile_mod._lock_for(weird._lock.path)
    blocker.acquire()
    th4 = threading.Thread(target=lambda: weird._lock.acquire(timeout=1.0) and weird._lock.release())
    th4.start(); th4.join()
    blocker.release()
    with open(victim) as fh:
        still = fh.read()
    check("a symlinked waiter file cannot overwrite its target", still == "PRECIOUS",
          "victim now %r" % still[:40])
    for f in (victim, link):
        try:
            os.unlink(f)
        except OSError:
            pass

    # A hostile file in the world-writable queue directory must not break a poll.
    den2 = turnstile_mod._waiters_dir(lk.path)
    os.makedirs(den2, exist_ok=True)
    junk = os.path.join(den2, "99999998-1.json")
    with open(junk, "w") as fh:
        fh.write("x" * 200000)              # not JSON, and far over the read cap
    try:
        w5 = turnstile_mod.who(path=lk.path)
        check("garbage in the queue dir does not break who()", isinstance(w5, dict))
    except Exception as exc:
        check("garbage in the queue dir does not break who()", False, type(exc).__name__)
    try:
        os.unlink(junk)
    except OSError:
        pass

    # ---- a lock directory nobody else can see ------------------------------------
    # Both of these shipped silently in real deployments before anything caught them,
    # so they get permanent cases rather than a one-off script.
    import tempfile as _tf
    _plat, _readlink = sys.platform, os.readlink
    _orig_reason = turnstile_mod.unshared_reason

    def _ns(same):
        d = {"/proc/self/ns/mnt": "mnt:[1]", "/proc/1/ns/mnt": "mnt:[1]" if same else "mnt:[2]"}
        # Lazily: os.readlink on a real /proc path must not be evaluated on a Mac.
        os.readlink = lambda q: d[q] if q in d else _readlink(q)
    try:
        sys.platform = "darwin"
        check("macOS per-user temp dir is refused",
              "PER-USER" in (turnstile_mod.unshared_reason(_tf.gettempdir()) or ""))
        check("a shared /tmp is not flagged on macOS",
              turnstile_mod.unshared_reason("/tmp/onelane") is None)
        sys.platform = "linux"
        _ns(False)
        check("systemd PrivateTmp is refused",
              "PrivateTmp" in (turnstile_mod.unshared_reason("/tmp/onelane") or ""))
        check("a shared path escapes a private namespace",
              turnstile_mod.unshared_reason("/var/lib/x/locks") is None)
        _ns(True)
        check("same namespace raises no false alarm",
              turnstile_mod.unshared_reason("/tmp/onelane") is None)
        os.readlink = _readlink
        sys.platform = "darwin"
        try:
            turnstile_mod.check_shared(_tf.gettempdir())
            check("check_shared refuses to start", False, "did not raise")
        except RuntimeError as e:
            check("check_shared refuses to start", "coordinate nothing" in str(e))

        # ---- strict mode must fail CLOSED, every time -----------------------------
        # The first cut set the once-only warning flag before testing strict, so strict
        # raised once and then handed out a working-looking lock forever. Anything that
        # caught the error and retried got a silently unshared lock.
        import tempfile as _t2
        bad = os.path.join(_tf.mkdtemp(), "unshared")
        os.makedirs(bad, exist_ok=True)
        lk = _CrossProcessLock(os.path.join(bad, "x.lock"))
        _env = os.environ.get("ONELANE_STRICT")
        _dir = os.environ.pop("ONELANE_DIR", None)      # the gate: default dir only
        os.environ["ONELANE_STRICT"] = "1"
        turnstile_mod._auto_reason = "unset"
        turnstile_mod.unshared_reason = lambda *a, **k: "test: pretend it is unshared"
        try:
            raised = 0
            for _ in range(3):
                try:
                    lk.acquire(timeout=1)
                except RuntimeError:
                    raised += 1
                else:
                    lk.release()
            check("strict raises on EVERY acquire, not just the first", raised == 3,
                  "raised %d of 3" % raised)
            # ...and a raise there must not stand the RLock up on its own.
            free = lk._local.acquire(blocking=False)
            if free:
                lk._local.release()
            check("a refused acquire does not strand the RLock", free)
        finally:
            turnstile_mod.unshared_reason = _orig_reason
            turnstile_mod._auto_reason = "unset"
            os.environ.pop("ONELANE_STRICT", None)
            if _env is not None:
                os.environ["ONELANE_STRICT"] = _env
            if _dir is not None:
                os.environ["ONELANE_DIR"] = _dir

        # ---- an explicit ONELANE_DIR silences the automatic check ---------------
        # Someone who set it has thought about which directory their processes share.
        # This is also what stops a hardened unit bind-mounting a shared dir under /tmp
        # from being told it is wrong.
        turnstile_mod._auto_reason = "unset"
        os.environ["ONELANE_DIR"] = "/some/deliberate/path"
        try:
            check("an explicit ONELANE_DIR silences the auto-check",
                  turnstile_mod._auto_unshared(_tf.gettempdir()) is None)
        finally:
            os.environ.pop("ONELANE_DIR", None)
            turnstile_mod._auto_reason = "unset"
    finally:
        sys.platform, os.readlink = _plat, _readlink

    srv.shutdown()
    print("\n%s  (%d checks failed)" % ("ALL PASS" if not failures else "FAILURES: " + ", ".join(failures),
                                        len(failures)))
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        _worker()
    else:
        sys.exit(main())
