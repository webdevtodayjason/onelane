# OneLane

**Two apps, one Tiiny, no collisions.**

Your Tiiny Pocket thinks about one thing at a time. That is fine until you run a second
app against it, at which point both of them start failing with this:

```json
{"code": 150004, "message": "The operation failed to complete."}
```

OneLane fixes that. Whichever app asks second just waits its turn instead of erroring.

```python
from onelane import OneLane

lane = OneLane()                      # finds the device and the key by itself
with lane.hold(why="bedtime story"):
    ...                               # your inference call goes here
```

That is the whole thing. One file, no dependencies, Python standard library only.

## Why you cannot just fix this yourself

Inside one program it is easy. You queue your own calls and move on.

It stops being easy the moment a second program touches the same device, because your
queue and its queue have never heard of each other. Two apps that are each perfectly well
behaved on their own will still collide constantly, because neither one can see the other.

OneLane puts the queue somewhere both of them can see it: a lock file on disk, held by the
kernel. Every app that uses OneLane takes turns with every other app that uses OneLane,
even though they were written separately and know nothing about each other.

## Install

Copy `onelane.py` next to your code. That is it.

```bash
curl -O https://raw.githubusercontent.com/webdevtodayjason/onelane/main/onelane.py
```

## What you get

**It cannot deadlock your device.** The lock is held by the operating system, not by
OneLane. If your app crashes, gets killed, or loses power while holding it, the kernel
releases it immediately. There is no stuck state to clean up and no timeout to tune.

**It tells you who is using the device.** Handy when something is slow and you want to
know which app is responsible.

```python
from onelane import who
print(who())     # {'owner': 'newsboard', 'why': 'enrich', 'held_for': 3.2, ...}
```

**It warns you when it cannot actually protect you.** If your apps run in containers or
under systemd with `PrivateTmp=yes`, each one gets its own private view of `/tmp` and the
lock silently protects nothing. OneLane detects that and says so, rather than letting you
believe you are safe. Point `ONELANE_DIR` at a directory everything can see and it works.

**It nests.** A function that already holds the lane can call another one that also wants
it without deadlocking.

## Tested on real hardware

Not simulated. Two separate processes, six concurrent calls against one Tiiny Pocket, zero
refused, and the long-running app on the same device never noticed.

The test suite runs against a fake device so you can check it yourself in a second:

```bash
python3 tests/fake_device_test.py
```

There is also `verify-on-device.sh` if you want to prove it against your own Tiiny.

## Finding the device

`OneLane()` works out where the box is, so an app the farm planted needs no configuration:

| Step | What it is |
|---|---|
| `TIINY_BASE` | what the farm CLI exports. A full URL is fine; a port in it pins the gateway port |
| `~/.tiinyapps/device.json` | what `farm device` writes: `{"base": ..., "key": ...}` |
| `TIINY_HOST` | the name this library documented first. Still honoured |

Then it asks the box which port serves the gateway. Firmware 1.0 binds the gateway to the
container bridge only, so port 8800 is refused from another machine and everything arrives
on port 80. Old firmware still answers on 8800. Passing `port=` pins it and skips the probe.

## One lock per device, named by serial number

The lock file is named after the device's **serial number**, read from the unauthenticated
`http://<addr>:39218/device.json`. Not its address: a LAN address is a DHCP lease and it
moves, and the same box also answers on its USB `/30`, so two apps reaching one device by
two routes used to take out two lock files and coordinate with nobody. That is the worst way
for a lock to fail, and it is the reason this library exists.

If the box will not say who it is, the resolved address is the fallback, and a serial another
process already learned is published in `onelane-serials.json` beside the lock files so one
failed probe cannot split the lock either.

## Settings

| Variable | Default | What it does |
|---|---|---|
| `TIINY_BASE` | - | your Tiiny's address or base URL |
| `TIINY_HOST` | - | the same thing, older name |
| `TIINY_KEY` | - | the API key from the Tiiny's settings |
| `TIINY_PORT` | *probed* | pin the gateway port instead of asking the box |
| `ONELANE_DIR` | `/tmp` | where the lock file lives. Set this if your apps run in containers or under `PrivateTmp` |
| `ONELANE_STRICT` | unset | refuse to run at all if the lock cannot be shared, instead of warning |

## Upgrading from Turnstile

This library used to be called Turnstile. Nothing breaks: `turnstile.py` is still here as a
shim, the old `Turnstile` class name still works, and `TURNSTILE_*` environment variables
are still honoured. New code should use `onelane` and `ONELANE_*`.

## Licence

MIT. Take it and use it.
