// Package pulzz provides a Go binding for the pulzZ SDK via cgo, wrapping
// the C ABI exposed by `libpulzz` (built from the `ffi/` Rust crate).
//
// Build:
//   cd bindings/go
//   CGO_LDFLAGS="-L../../target/debug" CGO_CFLAGS="-I../../ffi/include" go test ./...
//
// Usage:
//   client, err := pulzz.NewClient(pulzz.Config{...})
//   err = client.Connect("ws://localhost:8080")
//   err = client.Send(1, []byte("hello"))
//   err = client.Close()
package pulzz

/*
#cgo CFLAGS: -I${SRCDIR}/../../ffi/include
#include <pulzz.h>
#include <stdlib.h>
*/
import "C"

import (
        "errors"
        "fmt"
        "unsafe"
)

// CarrierKind enumerates the transport carriers supported by pulzZ.
type CarrierKind int

const (
        CarrierWebSocket    CarrierKind = C.WebSocket
        CarrierTcp          CarrierKind = C.Tcp
        CarrierQuicStream   CarrierKind = C.QuicStream
        CarrierQuicDatagram CarrierKind = C.QuicDatagram
        CarrierWebTransport CarrierKind = C.WebTransport
        CarrierUdpDatagram  CarrierKind = C.UdpDatagram
)

// SecurityProfile selects the PQC handshake profile.
type SecurityProfile int

const (
        SecurityPqMutualV1 SecurityProfile = C.PqMutualV1
        SecurityPqSimpleV1 SecurityProfile = C.PqSimpleV1
        SecurityClassicRef1 SecurityProfile = C.ClassicRef1
)

// SourceKind identifies the payload type for batch items.
type SourceKind int

const (
        SourceText   SourceKind = 0
        SourceJson   SourceKind = 1
        SourceBinary SourceKind = 2
        SourceImage  SourceKind = 3
)

// Config is the Go equivalent of the C PulzzConfig struct.
type Config struct {
        Carrier    CarrierKind
        Security   SecurityProfile
        BatchSize  uint32
        ZstdLevel  int32
        TimeoutMs  uint64
}

func (c Config) toC() C.PulzzConfig {
        return C.PulzzConfig{
                carrier:    C.PulzzCarrierKind(c.Carrier),
                security:   C.PulzzSecurityProfile(c.Security),
                batch_size: C.uint(c.BatchSize),
                zstd_level: C.int(c.ZstdLevel),
                timeout_ms: C.ulong(c.TimeoutMs),
        }
}

// Result is the C ABI result enum, mirrored in Go.
type Result int

const (
        ResultOk               Result = C.Ok
        ResultInvalidArg       Result = C.InvalidArg
        ResultInvalidState     Result = C.InvalidState
        ResultConnectionFailed Result = C.ConnectionFailed
        ResultHandshakeFailed  Result = C.HandshakeFailed
        ResultCompressionFailed Result = C.CompressionFailed
        ResultTimeout          Result = C.Timeout
        ResultBufferTooSmall    Result = C.BufferTooSmall
        ResultEndOfStream      Result = C.EndOfStream
        ResultInternal         Result = C.Internal
)

func (r Result) Error() error {
        switch r {
        case ResultOk:
                return nil
        case ResultInvalidArg:
                return errors.New("pulzz: invalid argument")
        case ResultInvalidState:
                return errors.New("pulzz: invalid state")
        case ResultConnectionFailed:
                return errors.New("pulzz: connection failed")
        case ResultHandshakeFailed:
                return errors.New("pulzz: handshake failed")
        case ResultCompressionFailed:
                return errors.New("pulzz: compression failed")
        case ResultTimeout:
                return errors.New("pulzz: operation timed out")
        case ResultBufferTooSmall:
                return errors.New("pulzz: buffer too small")
        case ResultEndOfStream:
                return errors.New("pulzz: end of stream")
        case ResultInternal:
                return errors.New("pulzz: internal error")
        default:
                return fmt.Errorf("pulzz: unknown result code %d", int(r))
        }
}

// lastError returns the thread-local last error string, or "" if none.
func lastError() string {
        cstr := C.pulzz_last_error()
        if cstr == nil {
                return ""
        }
        return C.GoString(cstr)
}

// wrapErr converts a C result code to a Go error, including the last
// error message if available.
func wrapErr(r C.PulzzResult) error {
        gr := Result(r)
        if gr == ResultOk {
                return nil
        }
        msg := lastError()
        if msg == "" {
                return gr.Error()
        }
        return fmt.Errorf("%w: %s", gr.Error(), msg)
}

// Client wraps a C pulzz_client_t handle.
type Client struct {
        handle C.PulzzClientHandle
}

// NewClient creates a new client from a Config.
func NewClient(cfg Config) (*Client, error) {
        cCfg := cfg.toC()
        var handle C.PulzzClientHandle
        r := C.pulzz_client_new(&cCfg, &handle)
        if err := wrapErr(r); err != nil {
                return nil, err
        }
        return &Client{handle: handle}, nil
}

// Connect to a server at url (e.g. "ws://localhost:8080").
func (c *Client) Connect(url string) error {
        cUrl := C.CString(url)
        defer C.free(unsafe.Pointer(cUrl))
        r := C.pulzz_client_connect(c.handle, cUrl)
        return wrapErr(r)
}

// Send a single item.
func (c *Client) Send(itemID uint64, payload []byte) error {
        if len(payload) == 0 {
                r := C.pulzz_client_send(c.handle, C.uint64_t(itemID), C.PulzzSlice{ptr: nil, len: 0})
                return wrapErr(r)
        }
        r := C.pulzz_client_send(c.handle, C.uint64_t(itemID), C.PulzzSlice{
                ptr: (*C.uint8_t)(unsafe.Pointer(&payload[0])),
                len: C.size_t(len(payload)),
        })
        return wrapErr(r)
}

// BatchItem is a single item in a batch.
type BatchItem struct {
        ItemID     uint64
        SourceKind SourceKind
        Payload    []byte
}

// SendBatch sends a batch of items. The batch is freed after the call.
func (c *Client) SendBatch(items []BatchItem) error {
        var batch C.PulzzBatchHandle
        if r := C.pulzz_batch_new(&batch); r != C.Ok {
                return wrapErr(r)
        }
        defer C.pulzz_batch_free(batch)

        for _, it := range items {
                var slice C.PulzzSlice
                if len(it.Payload) > 0 {
                        slice.ptr = (*C.uint8_t)(unsafe.Pointer(&it.Payload[0]))
                        slice.len = C.size_t(len(it.Payload))
                }
                if r := C.pulzz_batch_add(batch, C.uint64_t(it.ItemID), C.uint8_t(it.SourceKind), slice); r != C.Ok {
                        return wrapErr(r)
                }
        }
        r := C.pulzz_client_send_batch(c.handle, batch)
        // batch is consumed by send_batch; do not free it again.
        // Mark as freed to skip the defer above.
        batch = nil
        return wrapErr(r)
}

// Record is a received record.
type Record struct {
        ItemID     uint64
        Payload    []byte
        RecordType uint8
}

// Recv receives the next record. Returns nil, nil on end-of-stream.
func (c *Client) Recv(timeoutMs uint32) (*Record, error) {
        var itemID C.uint64_t
        var payloadPtr *C.uint8_t
        var payloadLen C.size_t
        var recordType C.uint8_t
        r := C.pulzz_client_recv(
                c.handle,
                &itemID,
                (**C.uint8_t)(unsafe.Pointer(&payloadPtr)),
                &payloadLen,
                &recordType,
                C.uint(timeoutMs),
        )
        if Result(r) == ResultEndOfStream {
                return nil, nil
        }
        if err := wrapErr(r); err != nil {
                return nil, err
        }
        defer C.pulzz_record_free_payload(payloadPtr, payloadLen)
        rec := &Record{
                ItemID:     uint64(itemID),
                RecordType: uint8(recordType),
        }
        if payloadLen > 0 {
                rec.Payload = C.GoBytes(unsafe.Pointer(payloadPtr), C.int(payloadLen))
        }
        return rec, nil
}

// Close the client connection.
func (c *Client) Close() error {
        if c.handle == nil {
                return nil
        }
        r := C.pulzz_client_close(c.handle)
        c.handle = nil
        return wrapErr(r)
}

// Free releases the client handle without graceful close.
func (c *Client) Free() {
        if c.handle != nil {
                C.pulzz_client_free(c.handle)
                c.handle = nil
        }
}

// Version returns the pulzZ version string.
func Version() string {
        return C.GoString(C.pulzz_version_string())
}

// ABIVersion returns the packed ABI version (major<<16 | minor<<8 | patch).
func ABIVersion() uint32 {
        return uint32(C.pulzz_abi_version())
}
