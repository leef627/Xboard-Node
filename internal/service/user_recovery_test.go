package service

import (
	"context"
	"errors"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestFirstUserWaitsForConfig(t *testing.T) {
	k := &fakeKernel{}
	s := newTestService(k)
	users := []model.UserSpec{{ID: 1, UUID: "first-user"}}
	s.applyUserUpdate(context.Background(), users, computeUserHash(users))
	if k.startCalls != 0 || len(s.lastUsers) != 1 {
		t.Fatalf("users before config: startCalls=%d users=%v", k.startCalls, s.lastUsers)
	}
	s.handleWSEvent(context.Background(), controlplane.Event{
		Type:   controlplane.EventSyncConfig,
		Config: &model.NodeSpec{Protocol: "vless", ServerPort: 23456},
	})
	if !k.running || k.startCalls != 1 {
		t.Fatalf("config should start pending users: running=%v starts=%d", k.running, k.startCalls)
	}
}

func TestFirstUserStartFailureCanRetry(t *testing.T) {
	k := &fakeKernel{startErr: errors.New("temporary bind failure")}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState(nil)
	oldHash := s.lastUserHash
	users := []model.UserSpec{{ID: 1, UUID: "first-user", SpeedLimit: 8}}
	event := controlplane.Event{Type: controlplane.EventSyncUsers, Users: users}
	s.handleWSEvent(context.Background(), event)
	if len(s.lastUsers) != 0 || s.lastUserHash != oldHash || s.speedTracker.GetLimiter("first-user") != nil {
		t.Fatal("failed start must restore user state and limiters")
	}
	k.startErr = nil
	s.handleWSEvent(context.Background(), event)
	if !k.running || k.startCalls != 2 || len(s.lastUsers) != 1 {
		t.Fatalf("same snapshot should retry: running=%v starts=%d users=%v", k.running, k.startCalls, s.lastUsers)
	}
}

func TestFirstUserAppliesDeferredCertificate(t *testing.T) {
	k := &fakeKernel{}
	s := newTestService(k)
	s.cfg.Cert.CertDir = t.TempDir()
	s.lastConfig = &model.NodeSpec{Protocol: "vless", CertConfig: &config.CertConfig{
		CertMode: "self", Domain: "localhost",
	}}
	k.onStart = func(_ *model.NodeSpec, users []model.UserSpec, cert kernel.TLSCert) {
		if !cert.HasCert() || len(users) != 1 || s.speedTracker.GetLimiter(users[0].UUID) == nil {
			t.Fatal("first start must receive current users, limits and deferred certificate")
		}
	}
	s.applyUserDelta(context.Background(), "add", []model.UserSpec{{ID: 1, UUID: "first-user", SpeedLimit: 8}})
	if k.startCalls != 1 || !k.running {
		t.Fatal("first user did not start the kernel")
	}
	defer s.cert.Stop()
}

func TestRemoveLastUserThenAddStartsWithNewSnapshot(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	old := []model.UserSpec{{ID: 1, UUID: "old-user"}}
	s.updateUserState(old)
	s.applyUserDelta(context.Background(), "remove", old)
	if k.running || len(s.lastUsers) != 0 {
		t.Fatal("last user removal must clear users and stop")
	}
	k.onStart = func(_ *model.NodeSpec, users []model.UserSpec, _ kernel.TLSCert) {
		if len(users) != 1 || users[0].UUID != "new-user" {
			t.Fatalf("stale users on restart: %v", users)
		}
	}
	s.applyUserDelta(context.Background(), "add", []model.UserSpec{{ID: 2, UUID: "new-user"}})
	if !k.running || k.startCalls != 1 {
		t.Fatal("new snapshot did not restart kernel")
	}
}
