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

lane = OneLane()                      # reads TIINY_HOST and TIINY_KEY
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

## Settings

| Variable | Default | What it does |
|---|---|---|
| `TIINY_HOST` | - | your Tiiny's address |
| `TIINY_KEY` | - | the API key from the Tiiny's settings |
| `ONELANE_DIR` | `/tmp` | where the lock file lives. Set this if your apps run in containers or under `PrivateTmp` |
| `ONELANE_STRICT` | unset | refuse to run at all if the lock cannot be shared, instead of warning |

## Upgrading from Turnstile

This library used to be called Turnstile. Nothing breaks: `turnstile.py` is still here as a
shim, the old `Turnstile` class name still works, and `TURNSTILE_*` environment variables
are still honoured. New code should use `onelane` and `ONELANE_*`.

## Licence

MIT. Take it and use it.
