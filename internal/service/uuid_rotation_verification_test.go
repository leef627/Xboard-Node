package service

// Starts the real embedded Xray kernel and a local echo server on 127.0.0.1.
// Every credential probe creates a fresh TCP connection and completes a
// VLESS handshake plus an echoed payload. No external panel is used.

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

const (
	rotationOldUUID   = "11111111-1111-4111-8111-111111111111"
	rotationNewUUID   = "22222222-2222-4222-8222-222222222222"
	rotationOtherUUID = "33333333-3333-4333-8333-333333333333"
)

// The wrapper records results and delegates all operations to the real kernel.
type rotationObservedKernel struct {
	kernel.Kernel
	updateCalls int
	addCalls    int
	removeCalls int
	updateErr   error
	addErr      error
	removeErr   error
}

func (k *rotationObservedKernel) UpdateUsers(users []model.UserSpec) (int, int, error) {
	k.updateCalls++
	a, r, err := k.Kernel.UpdateUsers(users)
	k.updateErr = err
	return a, r, err
}

func (k *rotationObservedKernel) AddUsers(users []model.UserSpec) (int, error) {
	k.addCalls++
	n, err := k.Kernel.AddUsers(users)
	k.addErr = err
	return n, err
}

func (k *rotationObservedKernel) RemoveUsers(users []model.UserSpec) (int, error) {
	k.removeCalls++
	n, err := k.Kernel.RemoveUsers(users)
	k.removeErr = err
	return n, err
}

func rotationEcho(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func rotationFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func rotationProbe(nodePort, targetPort int, credential string) error {
	id, err := hex.DecodeString(strings.ReplaceAll(credential, "-", ""))
	if err != nil || len(id) != 16 {
		return fmt.Errorf("invalid fixture UUID")
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", nodePort), time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	header := append([]byte{0}, id...)
	header = append(header, 0, 1, byte(targetPort>>8), byte(targetPort), 1, 127, 0, 0, 1)
	payload := []byte("uuid-rotation-check")
	if _, err = conn.Write(append(header, payload...)); err != nil {
		return err
	}
	response := make([]byte, 2)
	if _, err = io.ReadFull(conn, response); err != nil {
		return err
	}
	if response[0] != 0 {
		return fmt.Errorf("unexpected VLESS response: %v", response)
	}
	if _, err = io.CopyN(io.Discard, conn, int64(response[1])); err != nil {
		return err
	}
	reply := make([]byte, len(payload))
	if _, err = io.ReadFull(conn, reply); err != nil {
		return err
	}
	if string(reply) != string(payload) {
		return fmt.Errorf("echo mismatch")
	}
	return nil
}

func TestUUIDRotationLive(t *testing.T) {
	for _, kind := range []string{"xray"} {
		for _, mode := range []string{"full", "delta"} {
			for _, population := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/%s/users_%d", kind, mode, population), func(t *testing.T) {
					echoPort := rotationEcho(t)
					nodePort := rotationFreePort(t)
					cfg := &config.Config{Kernel: config.KernelConfig{Type: kind, LogLevel: "error", ConfigDir: t.TempDir()}}
					s := New(cfg)
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
					s.lastConfigHash = computeConfigHash(s.lastConfig)
					oldUsers := []model.UserSpec{{ID: 42, UUID: rotationOldUUID}}
					if population == 2 {
						oldUsers = append(oldUsers, model.UserSpec{ID: 43, UUID: rotationOtherUUID})
					}
					s.updateUserState(oldUsers)
					if !s.startKernel(s.lastConfig, oldUsers) {
						t.Fatal("real kernel did not start")
					}
					if err := rotationProbe(nodePort, echoPort, rotationOldUUID); err != nil {
						t.Fatalf("baseline old UUID failed: %v", err)
					}
					if err := rotationProbe(nodePort, echoPort, rotationNewUUID); err == nil {
						t.Fatal("baseline unexpectedly accepts new UUID")
					}
					t.Log("baseline: old UUID accepted, new UUID rejected (fresh connections)")

					nextUsers := append([]model.UserSpec(nil), oldUsers...)
					nextUsers[0].UUID = rotationNewUUID
					if mode == "full" {
						s.handleWSEvent(context.Background(), controlplane.Event{Type: controlplane.EventSyncUsers, Users: nextUsers})
					} else {
						s.handleWSEvent(context.Background(), controlplane.Event{Type: controlplane.EventSyncUserDelta, DeltaAction: "add", DeltaUsers: nextUsers[:1]})
					}
					newErr := rotationProbe(nodePort, echoPort, rotationNewUUID)
					oldErr := rotationProbe(nodePort, echoPort, rotationOldUUID)
					storedUUID := ""
					for _, u := range s.lastUsers {
						if u.ID == 42 {
							storedUUID = u.UUID
						}
					}
					t.Logf("after: running=%v oldAccepted=%v newAccepted=%v storedUUID=%s hashMatchesNew=%v", k.IsRunning(), oldErr == nil, newErr == nil, storedUUID, s.lastUserHash == computeUserHash(nextUsers))
					t.Logf("kernel calls: update=%d add=%d remove=%d; errors: update=%v add=%v remove=%v", k.updateCalls, k.addCalls, k.removeCalls, k.updateErr, k.addErr, k.removeErr)
					if newErr != nil {
						t.Errorf("new UUID should authenticate: %v", newErr)
					}
					if oldErr == nil {
						t.Error("old UUID still authenticates on a NEW connection")
					}
					if !k.IsRunning() {
						t.Error("kernel stopped during UUID replacement")
					}
					if storedUUID != rotationNewUUID {
						t.Errorf("stored user UUID did not advance: %s", storedUUID)
					}
					if population == 2 {
						if err := rotationProbe(nodePort, echoPort, rotationOtherUUID); err != nil {
							t.Errorf("unaffected user's credential failed: %v", err)
						}
					}
					if mode == "full" {
						before := k.updateCalls
						s.handleWSEvent(context.Background(), controlplane.Event{Type: controlplane.EventSyncUsers, Users: nextUsers})
						retryErr := rotationProbe(nodePort, echoPort, rotationNewUUID)
						t.Logf("repeat full sync: kernelUpdatesBefore=%d after=%d newAccepted=%v", before, k.updateCalls, retryErr == nil)
					}
				})
			}
		}
	}
}
