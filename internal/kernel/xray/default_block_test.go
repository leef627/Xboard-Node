package xray

import (
	"context"
	"encoding/json"
	"testing"

	coreRouter "github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/bittorrent"
	"github.com/xtls/xray-core/common/session"
	routingSession "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/infra/conf"
)

func newDefaultBlockRouter(t *testing.T) *coreRouter.Router {
	t.Helper()
	data, err := json.Marshal(buildRouting(nil, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	var config conf.RouterConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	pb, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	router := new(coreRouter.Router)
	if err := router.Init(context.Background(), pb, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })
	return router
}

func assertDefaultBlock(t *testing.T, router *coreRouter.Router, address, protocol string, blocked bool) {
	t.Helper()
	ctx := &routingSession.Context{
		Outbound: &session.Outbound{Target: xnet.TCPDestination(xnet.ParseAddress(address), 443)},
		Content:  &session.Content{Protocol: protocol},
	}
	route, err := router.PickRoute(ctx)
	if blocked {
		if err != nil || route.GetOutboundTag() != "block" {
			t.Fatalf("%s (%s): expected block, route=%v err=%v", address, protocol, route, err)
		}
	} else if err != common.ErrNoClue {
		t.Fatalf("%s (%s): expected no default block, route=%v err=%v", address, protocol, route, err)
	}
}

func TestDefaultBlockRangesMatchCoreRouter(t *testing.T) {
	router := newDefaultBlockRouter(t)
	blocked := []string{
		"0.1.2.3", "10.1.2.3", "100.64.0.1", "127.0.0.1", "169.254.1.1",
		"172.16.0.1", "192.0.0.1", "192.0.2.1", "192.168.1.1", "192.88.99.1",
		"198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "255.255.255.255",
		"::", "::1", "fc00::1", "fdff:ffff::1", "fe80::1", "ff02::1",
	}
	for _, address := range blocked {
		t.Run("block_"+address, func(t *testing.T) { assertDefaultBlock(t, router, address, "", true) })
	}
	allowed := []string{
		"1.1.1.1", "8.8.8.8", "100.63.255.255", "100.128.0.1", "172.15.255.255",
		"172.32.0.1", "192.0.3.1", "192.88.98.255", "192.88.100.0", "198.51.99.255",
		"198.51.101.0", "203.0.114.1", "223.255.255.254", "::2", "2001:4860:4860::8888",
	}
	for _, address := range allowed {
		t.Run("allow_"+address, func(t *testing.T) { assertDefaultBlock(t, router, address, "", false) })
	}
}

func TestDefaultBlockRecognizesBitTorrent(t *testing.T) {
	// A synthetic TCP handshake; no torrent download or external connection.
	header := append([]byte{19}, []byte("BitTorrent protocol")...)
	header = append(header, make([]byte, 48)...)
	result, err := bittorrent.SniffBittorrent(header)
	if err != nil {
		t.Fatalf("core did not recognize the BT handshake: %v", err)
	}
	if result.Protocol() != "bittorrent" {
		t.Fatalf("unexpected sniffed protocol: %s", result.Protocol())
	}
	router := newDefaultBlockRouter(t)
	// A public destination proves that the protocol rule is not tied to IP blocks.
	assertDefaultBlock(t, router, "8.8.8.8", result.Protocol(), true)
	for _, protocol := range []string{"", "http", "tls"} {
		assertDefaultBlock(t, router, "8.8.8.8", protocol, false)
	}
}
