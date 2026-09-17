package kernel

// Regressions for credential replacement under a stable panel user ID.

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestUserDiffUUIDRotationRemovesOldCredential(t *testing.T) {
	old := model.UserSpec{ID: 42, UUID: "11111111-1111-4111-8111-111111111111"}
	next := model.UserSpec{ID: 42, UUID: "22222222-2222-4222-8222-222222222222"}
	added, removed := UserDiff([]model.UserSpec{old}, []model.UserSpec{next})
	if len(added) != 1 || added[0] != next {
		t.Errorf("toAdd = %v, want [%v]", added, next)
	}
	if len(removed) != 1 || removed[0] != old {
		t.Errorf("toRemove = %v, want [%v]: the old credential must be removed before adding the same ID", removed, old)
	}
}

func TestUserDiffLimitChangeDoesNotReplaceCredential(t *testing.T) {
	old := model.UserSpec{ID: 42, UUID: "11111111-1111-4111-8111-111111111111", SpeedLimit: 8}
	next := old
	next.SpeedLimit = 16
	next.DeviceLimit = 3
	added, removed := UserDiff([]model.UserSpec{old}, []model.UserSpec{next})
	if len(added) != 0 || len(removed) != 0 {
		t.Errorf("limit-only change should not replace authentication: added=%v removed=%v", added, removed)
	}
}
