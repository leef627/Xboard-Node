package service

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/kernel/xray"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestXrayDefaultSniffingRewritesHTTPDestination(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Received-Host", r.Host)
		_, _ = io.WriteString(w, "sniffing-aligned")
	}))
	defer backend.Close()
	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port
	nodePort := rotationFreePort(t)
	k := xray.New(config.KernelConfig{Type: "xray", LogLevel: "error", ConfigDir: t.TempDir()})
	defer k.Stop()
	node := &model.NodeSpec{
		Protocol: "vless", Network: "tcp", ListenIP: "127.0.0.1", ServerPort: nodePort,
		CustomRouteRules: []model.CustomRouteRule{{
			Match:  model.RouteMatch{Domains: []string{"full:localhost"}},
			Action: model.RouteAction{Type: "direct"},
		}},
	}
	if err := k.Start(node, []model.UserSpec{{ID: 42, UUID: rotationOldUUID}}, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", nodePort), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	id, err := hex.DecodeString(strings.ReplaceAll(rotationOldUUID, "-", ""))
	if err != nil {
		t.Fatal(err)
	}
	header := append([]byte{0}, id...)
	// The requested IP is not the HTTP listener's 127.0.0.1. Success requires
	// sniffing Host: localhost and changing the actual target, not just routing.
	header = append(header, 0, 1, byte(backendPort>>8), byte(backendPort), 1, 127, 0, 0, 2)
	payload := []byte("GET /probe HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if _, err := conn.Write(append(header, payload...)); err != nil {
		t.Fatal(err)
	}
	responseHeader := make([]byte, 2)
	if _, err := io.ReadFull(conn, responseHeader); err != nil {
		t.Fatalf("VLESS response: %v", err)
	}
	if responseHeader[0] != 0 {
		t.Fatalf("unexpected VLESS version: %d", responseHeader[0])
	}
	if _, err := io.CopyN(io.Discard, conn, int64(responseHeader[1])); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "sniffing-aligned" || response.Header.Get("X-Received-Host") != "localhost" {
		t.Fatalf("request did not reach sniffed destination: body=%q headers=%v", body, response.Header)
	}
}
