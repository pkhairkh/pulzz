"""pulzZ Python binding example.

Demonstrates:
  - Importing the pulzz module
  - Constructing a PulzzClient with a config dict
  - Attempting to connect (and observing the failure path)
  - Sending a batch (lifecycle only — fails because not connected)
  - Closing the client

Build the module first:
    cd bindings/python
    maturin develop --release

Run:
    python3 bindings/python/examples/client_example.py
"""

import pulzz


def main():
    print("pulzZ Python binding example")
    print("  __version__    =", pulzz.__version__)
    print("  version_string =", pulzz.version_string)
    print("  pulzz_version() =", pulzz.pulzz_version())

    # Enum access
    print("  CarrierKind     =", pulzz.PyCarrierKind.WebSocket)
    print("  SecurityProfile =", pulzz.PySecurityProfile.PqSimpleV1)
    print("  SourceKind      =", pulzz.PySourceKind.Json)

    # Construct with kwargs
    client = pulzz.PulzzClient(
        carrier="websocket",
        security="pq_simple_v1",
        batch_size=50,
        zstd_level=0,
        timeout_ms=1000,
    )
    print("  client constructed")

    # Try to connect — expect failure (no server running on a closed port)
    try:
        client.connect("ws://127.0.0.1:1")
        print("  connect: succeeded (unexpected)")
    except RuntimeError as e:
        print("  connect: failed as expected:", str(e)[:80])

    # send_batch will fail because we're not connected
    try:
        client.send_batch([(1, b"hello"), (2, b"world")])
        print("  send_batch: succeeded (unexpected)")
    except RuntimeError as e:
        print("  send_batch: failed as expected:", str(e)[:80])

    # Close
    client.close()
    print("  closed")

    # Context manager protocol
    print("  context manager test:")
    with pulzz.PulzzClient() as ctx_client:
        try:
            ctx_client.connect("ws://127.0.0.1:1")
        except RuntimeError:
            print("    ctx client connect failed as expected")

    print("pulzZ Python binding example complete")


if __name__ == "__main__":
    main()
