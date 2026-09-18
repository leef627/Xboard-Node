package xray

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/xtls/xray-core/features/stats"
)

func TestInboundTagIncludesNodeAndListener(t *testing.T) {
	tests := []struct {
		name, protocol, listen string
		nodeID, port           int
		want                   string
	}{
		{"ipv4", "vless", "0.0.0.0", 7, 443, "vless_7_0.0.0.0_443"},
		{"default-listen", "vless", "", 7, 443, "vless_7_::_443"},
		{"explicit-ipv6-any", "vless", "::", 7, 443, "vless_7_::_443"},
		{"ipv6", "vmess", "2001:db8::1", 8, 8443, "vmess_8_2001:db8::1_8443"},
		{"standalone", "vless", "127.0.0.1", 0, 10000, "vless_0_127.0.0.1_10000"},
		{"different-port", "vless", "0.0.0.0", 7, 8443, "vless_7_0.0.0.0_8443"},
		{"different-node", "vless", "0.0.0.0", 9, 443, "vless_9_0.0.0.0_443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.KernelConfig{Type: "xray", NodeID: tt.nodeID}
			node := &model.NodeSpec{Protocol: tt.protocol, ListenIP: tt.listen, ServerPort: tt.port}
			generated := buildConfig(cfg, node, testUsers, kernel.TLSCert{})["inbounds"].([]M)[0]
			if got := generated["tag"]; got != tt.want {
				t.Fatalf("generated tag = %v, want %s", got, tt.want)
			}
			if got := inboundTag(cfg, node); got != tt.want {
				t.Fatalf("runtime tag = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestXrayRuntimeTagSelectsInboundWithoutRestart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	x := New(config.KernelConfig{Type: "xray", NodeID: 7, LogLevel: "error", ConfigDir: t.TempDir()})
	t.Cleanup(x.Stop)
	node := &model.NodeSpec{Protocol: "vless", ListenIP: "127.0.0.1", ServerPort: port, Network: "tcp"}
	users := []model.UserSpec{{ID: 42, UUID: "11111111-1111-4111-8111-111111111111"}}
	if err := x.Start(node, users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("vless_7_127.0.0.1_%d", port)
	if x.inboundTag != want {
		t.Fatalf("runtime tag = %s, want %s", x.inboundTag, want)
	}
	manager, err := x.getUserManager()
	if err != nil {
		t.Fatalf("cannot find user manager under new tag: %v", err)
	}
	if manager.GetUser(context.Background(), "user@42") == nil {
		t.Fatal("initial user missing")
	}
	statsManager := x.instance.GetFeature(stats.ManagerType()).(stats.Manager)
	if statsManager.GetCounter("inbound>>>"+want+">>>traffic>>>uplink") == nil {
		t.Fatal("inbound counter did not use new tag")
	}
	instance := x.instance
	next := []model.UserSpec{{ID: 42, UUID: "22222222-2222-4222-8222-222222222222"}}
	added, removed, err := x.UpdateUsers(next)
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 || removed != 1 {
		t.Fatalf("expected credential replacement, got added=%d removed=%d", added, removed)
	}
	if x.instance != instance {
		t.Fatal("user update restarted the kernel instead of finding the renamed inbound")
	}
	wantUser, err := toMemoryUser("vless", node, next[0])
	if err != nil {
		t.Fatal(err)
	}
	gotUser := manager.GetUser(context.Background(), "user@42")
	if gotUser == nil || !gotUser.Account.Equals(wantUser.Account) {
		t.Fatal("renamed inbound did not receive the replacement credential")
	}
}
