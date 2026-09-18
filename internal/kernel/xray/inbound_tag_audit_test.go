package xray

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/xtls/xray-core/features/bandwidth"
	"github.com/xtls/xray-core/features/stats"
	"gopkg.in/yaml.v3"
)

const tagAuditUUID = "11111111-1111-4111-8111-111111111111"
const tagAuditNextUUID = "22222222-2222-4222-8222-222222222222"

func tagAuditPort(t *testing.T, host string) int {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func tagAuditStart(t *testing.T, nodeID, port int, host string, routes []map[string]any, users []model.UserSpec) (*Xray, *model.NodeSpec) {
	t.Helper()
	x := New(config.KernelConfig{Type: "xray", NodeID: nodeID, LogLevel: "error", ConfigDir: t.TempDir(), CustomRoute: routes})
	node := &model.NodeSpec{Protocol: "vless", Network: "tcp", ListenIP: host, ServerPort: port}
	if err := x.Start(node, users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.Stop)
	return x, node
}

func TestInboundTagAuditReloadAndFailure(t *testing.T) {
	echoPort, _ := tagAuditEcho(t)
	routes := []map[string]any{{"type": "field", "ip": []string{"127.0.0.1/32"}, "outboundTag": "direct"}}
	users := []model.UserSpec{{ID: 42, UUID: tagAuditUUID, SpeedLimit: 8, DeviceLimit: 2}}
	x, node := tagAuditStart(t, 7, tagAuditPort(t, "127.0.0.1"), "127.0.0.1", routes, users)
	firstInstance, firstTag := x.instance, x.inboundTag
	if err := x.Reload(node, users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	if x.instance != firstInstance || x.inboundTag != firstTag {
		t.Fatal("unchanged config unexpectedly changed instance or tag")
	}

	next := *node
	next.ServerPort = tagAuditPort(t, "127.0.0.1")
	if err := x.Reload(&next, users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("vless_7_127.0.0.1_%d", next.ServerPort)
	if x.inboundTag != want || x.instance == firstInstance {
		t.Fatalf("port reload did not update tag/instance: %s", x.inboundTag)
	}
	manager, err := x.getUserManager()
	if err != nil || manager.GetUser(context.Background(), "user@42") == nil {
		t.Fatalf("reloaded inbound lookup failed: %v", err)
	}
	sm := x.instance.GetFeature(stats.ManagerType()).(stats.Manager)
	if sm.GetCounter("inbound>>>"+want+">>>traffic>>>uplink") == nil || sm.GetCounter("inbound>>>"+firstTag+">>>traffic>>>uplink") != nil {
		t.Fatal("new instance retained stale inbound counter names")
	}

	updated := []model.UserSpec{{ID: 42, UUID: tagAuditNextUUID, SpeedLimit: 16, DeviceLimit: 3}}
	if _, _, err := x.UpdateUsers(updated); err != nil {
		t.Fatal(err)
	}
	bm := x.instance.GetFeature(bandwidth.ManagerType()).(bandwidth.Manager)
	limiter := bm.GetUserLimiter("user@42")
	if limiter == nil || float64(limiter.Limit()) != 2_000_000 {
		t.Fatal("user speed limit changed identity after tag update")
	}
	x.limitDispatcher.mu.Lock()
	deviceLimit := x.limitDispatcher.deviceLimits["user@42"]
	x.limitDispatcher.mu.Unlock()
	if deviceLimit != 3 {
		t.Fatalf("device limit = %d, want 3", deviceLimit)
	}

	instance, tag := x.instance, x.inboundTag
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	rejected := next
	rejected.ServerPort = busy.Addr().(*net.TCPAddr).Port
	if err := x.Reload(&rejected, updated, kernel.TLSCert{}); err == nil {
		t.Fatal("expected occupied port to reject reload")
	}
	if x.instance != instance || x.inboundTag != tag || !x.IsRunning() {
		t.Fatal("failed reload changed the live inbound identity")
	}
	if _, err := x.getUserManager(); err != nil {
		t.Fatalf("old inbound no longer accessible after failed reload: %v", err)
	}
	if added, err := x.AddUsers([]model.UserSpec{{ID: 43, UUID: tagAuditUUID}}); err != nil || added != 1 {
		t.Fatalf("add after failed reload: added=%d error=%v", added, err)
	}
	if removed, err := x.RemoveUsers([]model.UserSpec{{ID: 43, UUID: tagAuditUUID}}); err != nil || removed != 1 {
		t.Fatalf("remove after failed reload: removed=%d error=%v", removed, err)
	}
	if x.instance != instance {
		t.Fatal("user operations unexpectedly restarted the retained core")
	}
	if err := tagAuditProbe("127.0.0.1", next.ServerPort, echoPort, tagAuditNextUUID); err != nil {
		t.Fatalf("retained inbound stopped serving traffic after failed reload: %v", err)
	}
	t.Logf("port reload: %s -> %s; rejected reload kept %s; speed/device limits and add/remove users preserved", firstTag, tag, tag)
}

func TestInboundTagAuditListenAddressReload(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Skipf("secondary loopback address unavailable: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()
	echoPort, _ := tagAuditEcho(t)
	routes := []map[string]any{{"type": "field", "ip": []string{"127.0.0.1/32"}, "outboundTag": "direct"}}
	users := []model.UserSpec{{ID: 42, UUID: tagAuditUUID}}
	x, node := tagAuditStart(t, 7, tagAuditPort(t, "127.0.0.1"), "127.0.0.1", routes, users)
	oldTag := x.inboundTag
	next := *node
	next.ListenIP, next.ServerPort = "127.0.0.2", port
	if err := x.Reload(&next, users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("vless_7_127.0.0.2_%d", port)
	if x.inboundTag != want {
		t.Fatalf("listener change left tag %s, want %s", x.inboundTag, want)
	}
	if _, err := x.getUserManager(); err != nil {
		t.Fatal(err)
	}
	if err := tagAuditProbe("127.0.0.2", port, echoPort, tagAuditUUID); err != nil {
		t.Fatal(err)
	}
	t.Logf("listen address reload: %s -> %s; live connection succeeded", oldTag, want)
}

func TestInboundTagAuditNodeIDIsRuntimeOnly(t *testing.T) {
	cfg := config.KernelConfig{Type: "xray", NodeID: 7}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := yaml.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["node_id"]; exists {
		t.Fatal("runtime node ID leaked into YAML config")
	}
	if _, exists := fields["nodeid"]; exists {
		t.Fatal("runtime node ID leaked into YAML config")
	}
	var decoded config.KernelConfig
	if err := yaml.Unmarshal([]byte("type: xray\nnode_id: 999\n"), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.NodeID != 0 {
		t.Fatalf("kernel YAML unexpectedly overrides runtime node ID: %d", decoded.NodeID)
	}
}

func tagAuditEcho(t *testing.T) (int, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := new(atomic.Int64)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, accepted
}

func tagAuditProbe(host string, nodePort, targetPort int, uuid string) error {
	id, err := hex.DecodeString(strings.ReplaceAll(uuid, "-", ""))
	if err != nil || len(id) != 16 {
		return fmt.Errorf("invalid fixture UUID")
	}
	c, err := net.DialTimeout("tcp", net.JoinHostPort(host, fmt.Sprint(nodePort)), time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	header := append([]byte{0}, id...)
	header = append(header, 0, 1, byte(targetPort>>8), byte(targetPort), 1, 127, 0, 0, 1)
	payload := []byte("inbound-tag-audit")
	if _, err := c.Write(append(header, payload...)); err != nil {
		return err
	}
	response := make([]byte, 2)
	if _, err := io.ReadFull(c, response); err != nil {
		return err
	}
	if response[0] != 0 {
		return fmt.Errorf("unexpected VLESS response")
	}
	if _, err := io.CopyN(io.Discard, c, int64(response[1])); err != nil {
		return err
	}
	read := make([]byte, len(payload))
	if _, err := io.ReadFull(c, read); err != nil {
		return err
	}
	if string(read) != string(payload) {
		return fmt.Errorf("echo mismatch")
	}
	return nil
}

func TestInboundTagAuditTrafficIsolation(t *testing.T) {
	echoPort, _ := tagAuditEcho(t)
	routes := []map[string]any{{"type": "field", "ip": []string{"127.0.0.1/32"}, "outboundTag": "direct"}}
	users := []model.UserSpec{{ID: 42, UUID: tagAuditUUID}}
	a, an := tagAuditStart(t, 7, tagAuditPort(t, "127.0.0.1"), "127.0.0.1", routes, users)
	b, bn := tagAuditStart(t, 8, tagAuditPort(t, "127.0.0.1"), "127.0.0.1", routes, users)
	if err := tagAuditProbe("127.0.0.1", an.ServerPort, echoPort, tagAuditUUID); err != nil {
		t.Fatal(err)
	}
	at, _, _, err := a.GetUserTraffic(context.Background())
	if err != nil || at[42][0] <= 0 || at[42][1] <= 0 {
		t.Fatalf("node 7 missing user 42 traffic: %v %v", at, err)
	}
	bt, _, _, err := b.GetUserTraffic(context.Background())
	if err != nil || len(bt) != 0 {
		t.Fatalf("node 7 traffic leaked into node 8: %v %v", bt, err)
	}
	if err := tagAuditProbe("127.0.0.1", bn.ServerPort, echoPort, tagAuditUUID); err != nil {
		t.Fatal(err)
	}
	bt, _, _, err = b.GetUserTraffic(context.Background())
	if err != nil || bt[42][0] <= 0 || bt[42][1] <= 0 {
		t.Fatalf("node 8 missing user 42 traffic: %v %v", bt, err)
	}
	unchanged, _, _, err := a.GetUserTraffic(context.Background())
	if err != nil || unchanged[42] != at[42] {
		t.Fatalf("node 8 changed node 7 accounting: %v %v", unchanged, err)
	}
	if _, _, err := a.UpdateUsers([]model.UserSpec{{ID: 42, UUID: tagAuditNextUUID}}); err != nil {
		t.Fatal(err)
	}
	if err := tagAuditProbe("127.0.0.1", an.ServerPort, echoPort, tagAuditNextUUID); err != nil {
		t.Fatal(err)
	}
	after, _, _, err := a.GetUserTraffic(context.Background())
	if err != nil || len(after) != 1 || after[42][0] <= at[42][0] || after[42][1] <= at[42][1] {
		t.Fatalf("UUID replacement changed user accounting identity: %v %v", after, err)
	}
	t.Logf("node tags %s / %s: isolated counters, user@42 preserved across UUID replacement", a.inboundTag, b.inboundTag)
}

func TestInboundTagAuditRoutingCompatibility(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy_rule_%v", legacy), func(t *testing.T) {
			echoPort, accepted := tagAuditEcho(t)
			port := tagAuditPort(t, "127.0.0.1")
			tag := fmt.Sprintf("vless_7_127.0.0.1_%d", port)
			if legacy {
				tag = "vless-in"
			}
			routes := []map[string]any{{"type": "field", "inboundTag": []string{tag}, "ip": []string{"127.0.0.1/32"}, "outboundTag": "direct"}}
			_, _ = tagAuditStart(t, 7, port, "127.0.0.1", routes, []model.UserSpec{{ID: 42, UUID: tagAuditUUID}})
			err := tagAuditProbe("127.0.0.1", port, echoPort, tagAuditUUID)
			if legacy {
				if err == nil || accepted.Load() != 0 {
					t.Fatal("old tag unexpectedly matched the new inbound")
				}
			} else if err != nil || accepted.Load() != 1 {
				t.Fatalf("new tag rule did not route to echo: %v", err)
			}
			t.Logf("legacy=%v backend_connections=%d probe_error=%v", legacy, accepted.Load(), err)
		})
	}
}

func TestInboundTagAuditIPv6Listener(t *testing.T) {
	ln, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	echoPort, _ := tagAuditEcho(t)
	tag := fmt.Sprintf("vless_7_::1_%d", port)
	routes := []map[string]any{{"type": "field", "inboundTag": []string{tag}, "ip": []string{"127.0.0.1/32"}, "outboundTag": "direct"}}
	x, _ := tagAuditStart(t, 7, port, "::1", routes, []model.UserSpec{{ID: 42, UUID: tagAuditUUID}})
	if x.inboundTag != tag {
		t.Fatalf("unexpected IPv6 tag: %s", x.inboundTag)
	}
	if err := tagAuditProbe("::1", port, echoPort, tagAuditUUID); err != nil {
		t.Fatal(err)
	}
	t.Logf("IPv6 listener accepted connection under tag %s", tag)
}
