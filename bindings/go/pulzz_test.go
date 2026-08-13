package pulzz

import (
	"testing"
)

func TestABIVersion(t *testing.T) {
	v := ABIVersion()
	// 0.4.0 → (0 << 16) | (4 << 8) | 0 = 0x400
	if v != 0x400 {
		t.Errorf("ABIVersion() = 0x%x, want 0x400", v)
	}
}

func TestVersionString(t *testing.T) {
	s := Version()
	if s == "" {
		t.Errorf("Version() returned empty string")
	}
	t.Logf("Version() = %s", s)
}

func TestNewClientWithDefaultConfig(t *testing.T) {
	cfg := Config{
		Carrier:   CarrierWebSocket,
		Security:  SecurityPqSimpleV1,
		BatchSize: 0,
		ZstdLevel: 3,
		TimeoutMs: 1000,
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Free()
}

func TestSendBeforeConnectReturnsInvalidState(t *testing.T) {
	cfg := Config{Carrier: CarrierWebSocket, Security: SecurityPqSimpleV1, TimeoutMs: 1000}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Free()

	err = client.Send(1, []byte("hello"))
	if err == nil {
		t.Errorf("Send before Connect should fail")
	}
	t.Logf("Send (before connect) error = %v (expected)", err)
}

func TestConnectToClosedPortFails(t *testing.T) {
	cfg := Config{Carrier: CarrierWebSocket, Security: SecurityPqSimpleV1, TimeoutMs: 500}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Free()

	err = client.Connect("ws://127.0.0.1:1")
	if err == nil {
		t.Errorf("Connect to closed port should fail")
	}
	t.Logf("Connect (to closed port) error = %v (expected)", err)
}

func TestResultErrorMapping(t *testing.T) {
	cases := []struct {
		r   Result
		msg string
	}{
		{ResultOk, ""},
		{ResultInvalidArg, "invalid argument"},
		{ResultInvalidState, "invalid state"},
		{ResultTimeout, "timed out"},
	}
	for _, c := range cases {
		err := c.r.Error()
		if c.r == ResultOk && err != nil {
			t.Errorf("ResultOk should not produce an error, got %v", err)
		}
		if c.msg != "" && err == nil {
			t.Errorf("Result %d should produce an error", c.r)
		}
	}
}
