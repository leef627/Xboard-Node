package service

// Recovery must work for full snapshots, deltas, and the service event paths.
// No proxy listener or panel connection is created in these unit tests.

import (
	"context"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestZeroUserRecoveryVerification(t *testing.T) {
	tests := []struct {
		name    string
		running bool
		apply   func(context.Context, *Service, []model.UserSpec)
	}{
		{"full_update_stopped", false, func(ctx context.Context, s *Service, users []model.UserSpec) {
			s.applyUserUpdate(ctx, users, computeUserHash(users))
		}},
		{"delta_add_stopped", false, func(ctx context.Context, s *Service, users []model.UserSpec) {
			s.applyUserDelta(ctx, "add", users)
		}},
		{"ws_full_stopped", false, func(ctx context.Context, s *Service, users []model.UserSpec) {
			s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncUsers, Users: users})
		}},
		{"ws_delta_stopped", false, func(ctx context.Context, s *Service, users []model.UserSpec) {
			s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncUserDelta, DeltaAction: "add", DeltaUsers: users})
		}},
		{"rest_users_only_stopped", false, func(ctx context.Context, s *Service, users []model.UserSpec) {
			s.applyPullResult(ctx, pullResult{users: users, userHash: computeUserHash(users)})
		}},
		{"repeated_full_update_stopped", false, func(ctx context.Context, s *Service, users []model.UserSpec) {
			for i := 0; i < 3; i++ {
				s.applyUserUpdate(ctx, users, computeUserHash(users))
			}
		}},
		{"control_full_update_running", true, func(ctx context.Context, s *Service, users []model.UserSpec) {
			s.applyUserUpdate(ctx, users, computeUserHash(users))
		}},
		{"control_delta_add_running", true, func(ctx context.Context, s *Service, users []model.UserSpec) {
			s.applyUserDelta(ctx, "add", users)
		}},
		{"control_rest_config_and_users_stopped", false, func(ctx context.Context, s *Service, users []model.UserSpec) {
			next := *s.lastConfig
			next.ServerPort++
			s.applyPullResult(ctx, pullResult{
				config: &next, configHash: computeConfigHash(&next),
				users: users, userHash: computeUserHash(users),
			})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &fakeKernel{running: tt.running}
			s := newTestService(k)
			s.cfg = &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
			s.lastConfig = &model.NodeSpec{Protocol: "vless", ServerPort: 23456}
			s.lastConfigHash = computeConfigHash(s.lastConfig)
			s.updateUserState(nil)
			users := []model.UserSpec{{ID: 1, UUID: "11111111-1111-4111-8111-111111111111"}}

			tt.apply(context.Background(), s, users)

			t.Logf("running=%v lastUsers=%d startCalls=%d updateCalls=%d addCalls=%d",
				k.running, len(s.lastUsers), k.startCalls, k.updateCalls, k.addCalls)
			if !k.running {
				t.Error("kernel did not start after receiving the first user")
			}
			if len(s.lastUsers) != 1 || s.lastUsers[0].ID != 1 {
				t.Errorf("new user was not retained: lastUsers=%v", s.lastUsers)
			}
			if !tt.running && k.startCalls != 1 {
				t.Errorf("Start calls = %d, want 1", k.startCalls)
			}
		})
	}
}
