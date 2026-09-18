package xray

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/xtls/xray-core/infra/conf"
)

func TestInboundSniffingDefaultsMatchXrayR(t *testing.T) {
	for _, protocol := range []string{"vmess", "vless", "trojan", "shadowsocks", "socks", "http", "hysteria"} {
		t.Run(protocol, func(t *testing.T) {
			node := &model.NodeSpec{Protocol: protocol, ServerPort: 8443, Network: "tcp", Cipher: "aes-128-gcm", Version: 2}
			// Only sniffing is built into a core config here; TLS fixture strings
			// prevent unrelated missing-certificate warnings in the generator.
			inbound := buildInbound(testKernelCfg, node, testUsers, kernel.TLSCert{CertPEM: []byte("TEST_CERT"), KeyPEM: []byte("TEST_KEY")})
			data, err := json.Marshal(inbound)
			if err != nil {
				t.Fatal(err)
			}
			var decoded conf.InboundDetourConfig
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.SniffingConfig == nil {
				t.Fatal("generated inbound has no sniffing configuration")
			}
			effective, err := decoded.SniffingConfig.Build()
			if err != nil {
				t.Fatal(err)
			}
			if !effective.Enabled || !slices.Equal(effective.DestinationOverride, []string{"http", "tls"}) {
				t.Fatalf("unexpected effective sniffing configuration: %v", effective)
			}
			if effective.RouteOnly || effective.MetadataOnly || len(effective.DomainsExcluded) != 0 {
				t.Fatalf("default target rewriting must match XrayR: %v", effective)
			}
		})
	}
}
