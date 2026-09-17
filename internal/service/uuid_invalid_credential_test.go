package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestUUIDRotationInvalidCredentialKeepsWorkingUser(t *testing.T) {
	echoPort, nodePort := rotationEcho(t), rotationFreePort(t)
	s := New(&config.Config{Kernel: config.KernelConfig{Type: "xray", LogLevel: "error", ConfigDir: t.TempDir()}})
	k := &rotationObservedKernel{Kernel: s.kernel}
	s.kernel = k
	t.Cleanup(k.Stop)
	s.lastConfig = &model.NodeSpec{
		Protocol: "vless", Network: "tcp", ListenIP: "127.0.0.1", ServerPort: nodePort,
		CustomRouteRules: []model.CustomRouteRule{{
			Name:   "local-test-echo-only",
			Match:  model.RouteMatch{IPCIDRs: []string{"127.0.0.1/32"}, Ports: []string{fmt.Sprint(echoPort)}},
			Action: model.RouteAction{Type: "direct"},
		}},
	}
	old := []model.UserSpec{{ID: 42, UUID: rotationOldUUID}}
	s.updateUserState(old)
	oldHash := s.lastUserHash
	if !s.startKernel(s.lastConfig, old) {
		t.Fatal("baseline kernel did not start")
	}
	if err := rotationProbe(nodePort, echoPort, rotationOldUUID); err != nil {
		t.Fatal(err)
	}

	invalid := []model.UserSpec{{ID: 42, UUID: strings.Repeat("z", 36)}}
	s.handleWSEvent(context.Background(), controlplane.Event{Type: controlplane.EventSyncUsers, Users: invalid})
	if k.updateErr == nil {
		t.Fatal("invalid credential error was swallowed")
	}
	if !k.IsRunning() || s.lastUserHash != oldHash || len(s.lastUsers) != 1 || s.lastUsers[0] != old[0] {
		t.Fatalf("failed update committed state: running=%v users=%v hash=%s", k.IsRunning(), s.lastUsers, s.lastUserHash)
	}
	if err := rotationProbe(nodePort, echoPort, rotationOldUUID); err != nil {
		t.Fatalf("invalid replacement revoked working credential: %v", err)
	}

	next := []model.UserSpec{{ID: 42, UUID: rotationNewUUID}}
	s.handleWSEvent(context.Background(), controlplane.Event{Type: controlplane.EventSyncUsers, Users: next})
	if err := rotationProbe(nodePort, echoPort, rotationNewUUID); err != nil {
		t.Fatalf("valid retry failed: %v", err)
	}
	if err := rotationProbe(nodePort, echoPort, rotationOldUUID); err == nil {
		t.Fatal("old UUID remains valid after successful retry")
	}
}
