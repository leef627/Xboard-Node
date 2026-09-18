package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestXrayInboundTagUsesNodeIDAndPreservesUserUpdates(t *testing.T) {
	base := &config.Config{Panel: config.PanelConfig{NodeID: 99}, Kernel: config.KernelConfig{Type: "xray", LogLevel: "error"}}
	multi := *base
	multi.Nodes = []config.NodeEntry{{NodeID: 7}, {NodeID: 8}}
	machine := *base
	machine.Machine = &config.MachineConfig{MachineID: 120, Token: "fixture"}
	standalone := *base
	standalone.Panel.NodeID = 0
	standalone.Standalone = &config.StandaloneConfig{}
	configs := append([]*config.Config{base}, multi.ExpandNodes()...)
	configs = append(configs, machine.ExpandMachineNode(81, "vless"), &standalone)
	for _, cfg := range configs {
		t.Run(fmt.Sprintf("node_%d", cfg.Panel.NodeID), func(t *testing.T) {
			echoPort, nodePort := rotationEcho(t), rotationFreePort(t)
			tag := fmt.Sprintf("vless_%d_127.0.0.1_%d", cfg.Panel.NodeID, nodePort)
			cfg.Kernel.ConfigDir = t.TempDir()
			cfg.Kernel.CustomRoute = []map[string]any{{
				"type": "field", "inboundTag": []string{tag}, "ip": []string{"127.0.0.1/32"},
				"port": fmt.Sprint(echoPort), "outboundTag": "direct",
			}}
			s := New(cfg)
			k := &rotationObservedKernel{Kernel: s.kernel}
			s.kernel = k
			t.Cleanup(k.Stop)
			s.lastConfig = &model.NodeSpec{Protocol: "vless", Network: "tcp", ListenIP: "127.0.0.1", ServerPort: nodePort}
			old := []model.UserSpec{{ID: 42, UUID: rotationOldUUID}}
			s.updateUserState(old)
			if !s.startKernel(s.lastConfig, old) {
				t.Fatal("kernel did not start")
			}
			// This allow rule requires the exact new inbound tag. If node identity
			// was not propagated, the default loopback block rejects the request.
			if err := rotationProbe(nodePort, echoPort, rotationOldUUID); err != nil {
				t.Fatalf("tag-based routing failed for %s: %v", tag, err)
			}
			next := []model.UserSpec{{ID: 42, UUID: rotationNewUUID}}
			s.applyUserUpdate(context.Background(), next, computeUserHash(next))
			if k.updateCalls != 1 || k.updateErr != nil {
				t.Fatalf("user update could not find inbound %s: calls=%d error=%v", tag, k.updateCalls, k.updateErr)
			}
			if err := rotationProbe(nodePort, echoPort, rotationNewUUID); err != nil {
				t.Fatalf("new UUID failed after tag change: %v", err)
			}
			if err := rotationProbe(nodePort, echoPort, rotationOldUUID); err == nil {
				t.Fatal("old UUID still authenticates")
			}
			t.Logf("validated tag=%s; user ID remains 42", tag)
		})
	}
}
