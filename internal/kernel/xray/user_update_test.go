package xray

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/xtls/xray-core/common/protocol"
)

type failingUserManager struct {
	users    map[string]*protocol.MemoryUser
	onAdd    func(*protocol.MemoryUser) error
	onRemove func(string) error
}

func (m *failingUserManager) AddUser(_ context.Context, u *protocol.MemoryUser) error {
	if m.onAdd != nil {
		if err := m.onAdd(u); err != nil {
			return err
		}
	}
	if m.users[u.Email] != nil {
		return fmt.Errorf("duplicate email %s", u.Email)
	}
	m.users[u.Email] = u
	return nil
}
func (m *failingUserManager) RemoveUser(_ context.Context, email string) error {
	if m.onRemove != nil {
		if err := m.onRemove(email); err != nil {
			return err
		}
	}
	if m.users[email] == nil {
		return fmt.Errorf("missing email %s", email)
	}
	delete(m.users, email)
	return nil
}
func (m *failingUserManager) GetUser(_ context.Context, email string) *protocol.MemoryUser {
	return m.users[email]
}
func (m *failingUserManager) GetUsers(context.Context) []*protocol.MemoryUser {
	var users []*protocol.MemoryUser
	for _, u := range m.users {
		users = append(users, u)
	}
	return users
}
func (m *failingUserManager) GetUsersCount(context.Context) int64 { return int64(len(m.users)) }

func TestManagedUserReplacementRollsBackFailures(t *testing.T) {
	for _, operation := range []string{"add", "remove"} {
		t.Run(operation, func(t *testing.T) {
			old1 := &protocol.MemoryUser{Email: userEmail(1)}
			old2 := &protocol.MemoryUser{Email: userEmail(2)}
			new1 := &protocol.MemoryUser{Email: userEmail(1), Level: 1}
			new2 := &protocol.MemoryUser{Email: userEmail(2), Level: 1}
			failure := errors.New("injected update failure")
			m := &failingUserManager{users: map[string]*protocol.MemoryUser{old1.Email: old1, old2.Email: old2}}
			if operation == "add" {
				m.onAdd = func(u *protocol.MemoryUser) error {
					if u == new2 {
						return failure
					}
					return nil
				}
			} else {
				m.onRemove = func(email string) error {
					if email == old2.Email {
						return failure
					}
					return nil
				}
			}
			err := updateManagedUsers(context.Background(), m, []*protocol.MemoryUser{new1, new2}, []model.UserSpec{{ID: 1}, {ID: 2}})
			if !errors.Is(err, failure) {
				t.Fatalf("failure not returned: %v", err)
			}
			if len(m.users) != 2 || m.users[old1.Email] != old1 || m.users[old2.Email] != old2 {
				t.Fatalf("original accounts were not restored: %v", m.users)
			}
		})
	}
}

func TestManagedUserReplacementReportsRollbackFailure(t *testing.T) {
	old := &protocol.MemoryUser{Email: userEmail(1)}
	next := &protocol.MemoryUser{Email: userEmail(1), Level: 1}
	updateFailure := errors.New("cannot add new account")
	rollbackFailure := errors.New("cannot restore old account")
	m := &failingUserManager{users: map[string]*protocol.MemoryUser{old.Email: old}}
	m.onAdd = func(u *protocol.MemoryUser) error {
		if u == next {
			return updateFailure
		}
		return rollbackFailure
	}
	err := updateManagedUsers(context.Background(), m, []*protocol.MemoryUser{next}, []model.UserSpec{{ID: 1}})
	if !errors.Is(err, updateFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("lost failure information: %v", err)
	}
}
