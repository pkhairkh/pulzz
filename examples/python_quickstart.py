"""pulzZ Python quickstart.

Cross-language example demonstrating the minimal Python client lifecycle.
Build the wheel first: `cd bindings/python && maturin develop`.
Run: `python3 examples/python_quickstart.py`
"""

import pulzz


def main():
    print("pulzZ Python quickstart")
    print("  __version__    =", pulzz.__version__)
    print("  version_string =", pulzz.version_string)

    cfg = dict(
        carrier="websocket",
        security="pq_simple_v1",
        batch_size=50,
        zstd_level=3,
        timeout_ms=1000,
    )
    client = pulzz.PulzzClient(**cfg)
    print("  client constructed")

    try:
        client.connect("ws://127.0.0.1:1")
        print("  connect: succeeded (unexpected)")
    except RuntimeError as e:
        print("  connect (to closed port) failed as expected:", str(e)[:80])

    try:
        client.send_batch([(1, b'{"user":"alice"}'), (2, b'{"user":"bob"}')])
        print("  send_batch: succeeded (unexpected)")
    except RuntimeError as e:
        print("  send_batch (before connect) failed as expected:", str(e)[:80])

    client.close()
    print("pulzZ Python quickstart complete")


if __name__ == "__main__":
    main()
