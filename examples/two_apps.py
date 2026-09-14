#!/usr/bin/env python3
"""Two applications, one Tiiny, no collisions.

    python3 examples/two_apps.py            # against your real device
    python3 examples/two_apps.py --fake     # against the test fake, no hardware needed

App A is a ticker: short, frequent summaries, the way a dashboard behaves.
App B is a storyteller: long, occasional generations, the way a narrative app behaves.

They are separate PROCESSES that have never heard of each other. The only thing they
share is a OneLane pointed at the same device. Run it and watch the interleaving in
the log: nobody waits long, nobody is refused, and the device is never asked to do two
things at once.

Each one is a multiprocessing "spawn" child: a fresh interpreter, which is what they
need to be, and not a command run through subprocess. tiinyapp.farm refuses an archive
that contains subprocess at all, and nothing here ever wanted to run a command.

Run it once with `--chaos` to see what the same two programs look like without the
onelane. That is the version people write first.
"""

import multiprocessing
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from onelane import OneLane, DeviceBusy, device_from_env  # noqa: E402

FAKE_PORT = 8899

# A fresh interpreter per application, which is the honest shape of "two apps".
_SPAWN = multiprocessing.get_context("spawn")

TICKER_PROMPTS = [
    "In one sentence, why do distributed systems need backpressure?",
    "In one sentence, what is a thundering herd?",
    "In one sentence, what does an advisory lock guarantee?",
]
STORY_PROMPTS = [
    "Write two sentences of a bedtime story about a lighthouse keeper's cat.",
    "Write two sentences about what the cat found under the stairs.",
]


def log(app, msg):
    print("%7.2fs  %-9s %s" % (time.time() - START, app, msg), flush=True)


def pick_model(t):
    for mid, _u, status in t.budget.resident():
        if status == "running":
            return mid
    raise SystemExit("no model is running on the device; start one in TiinyOS first")


def run_app(name, prompts, gap, coordinated, host, port):
    t = OneLane(host=host, port=port,
                  key=os.environ.get("TIINY_KEY", "x"),
                  tries=10, base_delay=1.0, max_delay=8.0,
                  on_wait=lambda a, d, why: log(name, "device busy (%s), waiting %.1fs" % (why, d)))
    model = pick_model(t)
    for i, prompt in enumerate(prompts, 1):
        t0 = time.time()
        try:
            if coordinated:
                out = t.chat(model, [{"role": "user", "content": prompt}], max_tokens=600)
            else:
                # the naive version: straight at the device, no queue, no retry
                out = t._request("/v1/chat/completions",
                                 {"model": model, "messages": [{"role": "user", "content": prompt}],
                                  "max_tokens": 600})
                if isinstance(out, dict) and out.get("code") == 150004:
                    log(name, "REFUSED  call %d — device was busy (150004)" % i)
                    time.sleep(gap)
                    continue
                out = ((out.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
            log(name, "call %d ok in %5.1fs  %s" % (i, time.time() - t0, out.strip()[:64].replace("\n", " ")))
        except DeviceBusy as exc:
            log(name, "gave up on call %d: %s" % (i, exc))
        except Exception as exc:
            log(name, "call %d failed: %s" % (i, str(exc)[:80]))
        time.sleep(gap)


def main():
    fake = "--fake" in sys.argv
    coordinated = "--chaos" not in sys.argv
    # port None means "ask the device". Firmware 1.0 serves the gateway on 80
    # behind a Host-header router and refuses 8800 from another machine, older
    # firmware serves it on 8800, and OneLane works out which. Hardcoding 8800
    # here is how this example stopped working on 1.0.
    host, port = ("127.0.0.1", FAKE_PORT) if fake else (device_from_env()[0] or "127.0.0.1", None)

    srv = None
    if fake:
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"))
        import threading
        from fake_device_test import Handler, ThreadingHTTPServer
        srv = ThreadingHTTPServer((host, port), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        time.sleep(0.2)

    print("device %s%s   mode: %s\n" % (
        host, ":%d" % port if port else " (gateway port found on connect)",
        "TURNSTILE" if coordinated else "CHAOS (no coordination)"))
    kids = [
        _SPAWN.Process(target=run_app,
                       args=("ticker", TICKER_PROMPTS, 1.0, coordinated, host, port)),
        _SPAWN.Process(target=run_app,
                       args=("story", STORY_PROMPTS, 0.2, coordinated, host, port)),
    ]
    for kid in kids:
        kid.start()
    for kid in kids:
        kid.join()
    if srv:
        srv.shutdown()
    print("\nBoth applications finished." if coordinated else
          "\nNote the REFUSED lines. That is what one device and two hopeful programs looks like.")


START = time.time()

if __name__ == "__main__":
    main()
