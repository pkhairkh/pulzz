// Cross-language quickstart examples for the pulzZ SDK.
//
// Each example demonstrates the minimal client lifecycle in its respective
// language: construct a client, attempt to connect, observe the failure
// path (no server running), and clean up.
//
// Run order:
//   cargo run --example rust_quickstart -p pulzz-sdk
//   cc ffi/examples/c_example.c -I ffi/include -L target/debug -lpulzz -o target/debug/c_quickstart
//   node bindings/wasm/examples/node_example.mjs   # requires wasm-pack build first
//   python3 bindings/python/examples/client_example.py
//   cd bindings/go && go run examples/go_quickstart.go
package main

import (
	"fmt"

	pulzz "github.com/pkhairkh/pulzz/bindings/go"
)

func main() {
	fmt.Println("pulzZ Go quickstart")
	fmt.Println("  ABIVersion =", pulzz.ABIVersion())
	fmt.Println("  Version    =", pulzz.Version())

	cfg := pulzz.Config{
		Carrier:   pulzz.CarrierWebSocket,
		Security:  pulzz.SecurityPqSimpleV1,
		BatchSize: 50,
		ZstdLevel: 3,
		TimeoutMs: 1000,
	}

	client, err := pulzz.NewClient(cfg)
	if err != nil {
		fmt.Println("  NewClient failed:", err)
		return
	}
	defer client.Free()
	fmt.Println("  client constructed")

	err = client.Connect("ws://127.0.0.1:1")
	if err != nil {
		fmt.Println("  connect (to closed port) failed as expected:", err)
	}

	items := []pulzz.BatchItem{
		{ItemID: 1, SourceKind: pulzz.SourceJson, Payload: []byte(`{"user":"alice"}`)},
		{ItemID: 2, SourceKind: pulzz.SourceJson, Payload: []byte(`{"user":"bob"}`)},
	}
	err = client.SendBatch(items)
	if err != nil {
		fmt.Println("  sendBatch (before connect) failed as expected:", err)
	}

	fmt.Println("pulzZ Go quickstart complete")
}
