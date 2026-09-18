package xray

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	coreRouter "github.com/xtls/xray-core/app/router"
	appstats "github.com/xtls/xray-core/app/stats"
	xrayCore "github.com/xtls/xray-core/core"
	featurebandwidth "github.com/xtls/xray-core/features/bandwidth"
	xraystats "github.com/xtls/xray-core/features/stats"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

func newStatsManager(t *testing.T) xraystats.Manager {
	t.Helper()
	mgr, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		t.Fatalf("stats.NewManager() error = %v", err)
	}
	return mgr
}

func mustRegisterCounter(t *testing.T, mgr xraystats.Manager, name string) xraystats.Counter {
	t.Helper()
	counter, err := mgr.RegisterCounter(name)
	if err != nil {
		t.Fatalf("RegisterCounter(%q) error = %v", name, err)
	}
	return counter
}

func counterName(userID int, direction string) string {
	return fmt.Sprintf("user>>>%s>>>traffic>>>%s", userEmail(userID), direction)
}

func newStatsBackedXray(t *testing.T, users []model.UserSpec, ld *LimitDispatcher) (*Xray, xraystats.Manager) {
	t.Helper()
	mgr := newStatsManager(t)
	inst := new(xrayCore.Instance)
	if err := inst.AddFeature(mgr); err != nil {
		t.Fatalf("Instance.AddFeature(stats) error = %v", err)
	}
	x := New(config.KernelConfig{Type: "xray"})
	x.instance = inst
	x.limitDispatcher = ld
	x.users = users
	x.cumTraffic = make(map[int][2]int64)
	x.running.Store(true)
	return x, mgr
}

func TestXrayGetUserTrafficUsesBuiltInStatsAndDispatcherState(t *testing.T) {
	users := []model.UserSpec{
		{ID: 1, UUID: "uuid-1"},
		{ID: 2, UUID: "uuid-2"},
	}

	ld := newTestDispatcher()
	email1 := userEmail(1)
	email2 := userEmail(2)
	ld.UpdateLimits(map[string]int{email1: 1, email2: 2}, nil, nil)
	ld.mu.Lock()
	ld.limitedIPs[email1] = map[string]int{"1.1.1.1": 1}
	ld.mu.Unlock()
	ref := &atomic.Int64{}
	ref.Store(1)
	ic := &ipCounter{}
	ic.ips.Store("2.2.2.2", ref)
	ld.unlimitedIPs.Store(email2, ic)
	ld.connCount.Store(7)

	x, mgr := newStatsBackedXray(t, users, ld)
	up1 := mustRegisterCounter(t, mgr, counterName(1, "uplink"))
	down1 := mustRegisterCounter(t, mgr, counterName(1, "downlink"))
	up2 := mustRegisterCounter(t, mgr, counterName(2, "uplink"))
	down2 := mustRegisterCounter(t, mgr, counterName(2, "downlink"))
	up1.Add(100)
	down1.Add(200)
	up2.Add(50)
	down2.Add(80)

	traffic, aliveIPs, connCount, err := x.GetUserTraffic(context.Background())
	if err != nil {
		t.Fatalf("GetUserTraffic() error = %v", err)
	}
	if connCount != 7 {
		t.Fatalf("connCount = %d, want 7", connCount)
	}
	if got := traffic[1]; got != [2]int64{100, 200} {
		t.Fatalf("traffic[1] = %v, want [100 200]", got)
	}
	if got := traffic[2]; got != [2]int64{50, 80} {
		t.Fatalf("traffic[2] = %v, want [50 80]", got)
	}
	if !aliveIPs[1]["1.1.1.1"] {
		t.Fatal("expected limited-user IP to be reported")
	}
	if !aliveIPs[2]["2.2.2.2"] {
		t.Fatal("expected unlimited-user IP to be reported")
	}

	up1.Add(30)
	down1.Add(40)
	traffic, _, _, err = x.GetUserTraffic(context.Background())
	if err != nil {
		t.Fatalf("second GetUserTraffic() error = %v", err)
	}
	if got := traffic[1]; got != [2]int64{130, 240} {
		t.Fatalf("traffic[1] after second poll = %v, want [130 240]", got)
	}
	if got := traffic[2]; got != [2]int64{50, 80} {
		t.Fatalf("traffic[2] after second poll = %v, want [50 80]", got)
	}
}

func TestXrayUpdateDispatcherLimitsPropagatesDeviceMetadata(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray"})
	ld := newTestDispatcher()
	x.limitDispatcher = ld

	users := []model.UserSpec{
		{ID: 7, UUID: "uuid-7", DeviceLimit: 2, SpeedLimit: 12},
		{ID: 9, UUID: "uuid-9", DeviceLimit: 0, SpeedLimit: 0},
	}

	x.updateDispatcherLimits(users)

	if got := ld.deviceLimits[userEmail(7)]; got != 2 {
		t.Fatalf("deviceLimits[email] = %d, want 2", got)
	}
	if got := ld.deviceLimits["uuid-7"]; got != 2 {
		t.Fatalf("deviceLimits[uuid] = %d, want 2", got)
	}
	if _, ok := ld.deviceLimits[userEmail(9)]; ok {
		t.Fatal("unexpected device limit entry for unlimited user")
	}
}

func TestXraySetSpeedLimitFuncUsesPatchedCorePath(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray"})
	called := false

	x.SetSpeedLimitFunc(func(string) *rate.Limiter {
		called = true
		return rate.NewLimiter(rate.Limit(1), 1)
	})

	if called {
		t.Fatal("patched xray path should not consult external speed-limit callbacks")
	}

	mu, err := toMemoryUser("vless", &model.NodeSpec{Protocol: "vless"}, model.UserSpec{ID: 1, UUID: "11111111-1111-1111-1111-111111111111", SpeedLimit: 8})
	if err != nil {
		t.Fatalf("toMemoryUser() error = %v", err)
	}
	if mu.Level != 0 {
		t.Fatalf("MemoryUser.Level = %d, want 0", mu.Level)
	}
}


func TestXrayCapabilities(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray"})
	caps := x.Capabilities()
	if !caps.PerUserSpeedLimit || !caps.DeviceLimit || !caps.BuiltInTrafficStats || !caps.AliveIPTracking {
		t.Fatalf("unexpected positive xray capabilities: %+v", caps)
	}
	if caps.ForceCloseConnection || caps.ForceCloseUser {
		t.Fatalf("unexpected force-close xray capabilities: %+v", caps)
	}
}


func TestXrayUpdateBandwidthLimitsWritesPatchedCoreFeature(t *testing.T) {
	inst := new(xrayCore.Instance)
	bm := featurebandwidth.New()
	if err := inst.AddFeature(bm); err != nil {
		t.Fatalf("Instance.AddFeature(bandwidth) error = %v", err)
	}
	x := New(config.KernelConfig{Type: "xray"})
	x.instance = inst
	x.updateBandwidthLimits([]model.UserSpec{{ID: 1, UUID: "uuid-1", SpeedLimit: 8}})
	lim := bm.GetUserLimiter(userEmail(1))
	if lim == nil {
		t.Fatal("expected patched bandwidth feature to receive user limiter")
	}
}


func TestXrayUpdateBandwidthLimitsUsesSpeedLimitFunc(t *testing.T) {
	inst := new(xrayCore.Instance)
	bm := featurebandwidth.New()
	if err := inst.AddFeature(bm); err != nil {
		t.Fatalf("Instance.AddFeature(bandwidth) error = %v", err)
	}
	x := New(config.KernelConfig{Type: "xray"})
	x.instance = inst
	x.users = []model.UserSpec{{ID: 1, UUID: "uuid-1", SpeedLimit: 8}}
	shared := rate.NewLimiter(7, 7)
	x.SetSpeedLimitFunc(func(uuid string) *rate.Limiter {
		if uuid != "uuid-1" {
			t.Fatalf("unexpected uuid: %s", uuid)
		}
		return shared
	})
	lim := bm.GetUserLimiter(userEmail(1))
	if lim != shared {
		t.Fatal("expected patched bandwidth feature to consume shared limiter from callback")
	}
}


func TestXrayUpdateBandwidthLimitsFallsBackToUserSpeed(t *testing.T) {
	inst := new(xrayCore.Instance)
	bm := featurebandwidth.New()
	if err := inst.AddFeature(bm); err != nil {
		t.Fatalf("Instance.AddFeature(bandwidth) error = %v", err)
	}
	x := New(config.KernelConfig{Type: "xray"})
	x.instance = inst
	x.users = []model.UserSpec{{ID: 2, UUID: "uuid-2", SpeedLimit: 16}}
	x.updateBandwidthLimits(nil)
	if bm.GetUserLimiter(userEmail(2)) == nil {
		t.Fatal("expected fallback limiter derived from user speed")
	}
}


func TestXrayUpdateUsersLimitOnlyRefreshesDispatcherAndBandwidth(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray"})
	ld := newTestDispatcher()
	inst := new(xrayCore.Instance)
	bm := featurebandwidth.New()
	if err := inst.AddFeature(bm); err != nil {
		t.Fatalf("Instance.AddFeature(bandwidth) error = %v", err)
	}
	x.instance = inst
	x.limitDispatcher = ld
	x.users = []model.UserSpec{{ID: 1, UUID: "uuid-1", DeviceLimit: 1, SpeedLimit: 8}}

	added, removed, err := x.UpdateUsers([]model.UserSpec{{ID: 1, UUID: "uuid-1", DeviceLimit: 3, SpeedLimit: 16}})
	if err != nil {
		t.Fatalf("UpdateUsers() error = %v", err)
	}
	if added != 0 || removed != 0 {
		t.Fatalf("UpdateUsers() counts = (%d, %d), want (0, 0)", added, removed)
	}
	if got := ld.deviceLimits[userEmail(1)]; got != 3 {
		t.Fatalf("device limit after refresh = %d, want 3", got)
	}
	if bm.GetUserLimiter(userEmail(1)) == nil {
		t.Fatal("expected bandwidth limiter to be refreshed for unchanged user set")
	}
}

func TestXrayRemoveUsersStopsKernelWhenLastUserRemoved(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray"})
	x.instance = new(xrayCore.Instance)
	ld := newTestDispatcher()
	x.limitDispatcher = ld
	x.users = []model.UserSpec{{ID: 1, UUID: "uuid-1"}}
	x.running.Store(true)

	removed, err := x.RemoveUsers([]model.UserSpec{{ID: 1, UUID: "uuid-1"}})
	if err != nil {
		t.Fatalf("RemoveUsers() error = %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if x.IsRunning() {
		t.Fatal("expected xray to stop when last user is removed")
	}
}

type geoTestTransport struct {
	mu       sync.Mutex
	data     map[string][]byte
	requests []string
	fail     bool
}

func (f *geoTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != "github.com" || !strings.HasPrefix(r.URL.Path, "/Loyalsoldier/v2ray-rules-dat/") {
		return nil, fmt.Errorf("unexpected test request: %s", r.URL)
	}
	name := filepath.Base(r.URL.Path)
	f.mu.Lock()
	f.requests = append(f.requests, name)
	data := f.data[name]
	f.mu.Unlock()
	if data == nil {
		return nil, fmt.Errorf("unexpected geo file: %s", name)
	}
	code, status := 200, "200 OK"
	if f.fail {
		code, status = 503, "503 Service Unavailable"
	}
	// Keep simultaneous starts overlapping long enough to exercise the cache lock.
	time.Sleep(10 * time.Millisecond)
	return &http.Response{StatusCode: code, Status: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
}

func newGeoTestTransport(t *testing.T) *geoTestTransport {
	t.Helper()
	ipData, err := proto.Marshal(&coreRouter.GeoIPList{Entry: []*coreRouter.GeoIP{{CountryCode: "PRIVATE", Cidr: []*coreRouter.CIDR{{Ip: []byte{127, 0, 0, 0}, Prefix: 8}}}}})
	if err != nil {
		t.Fatal(err)
	}
	siteData, err := proto.Marshal(&coreRouter.GeoSiteList{Entry: []*coreRouter.GeoSite{{CountryCode: "TEST", Domain: []*coreRouter.Domain{{Type: coreRouter.Domain_Full, Value: "example.com"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	f := &geoTestTransport{data: map[string][]byte{"geoip.dat": ipData, "geosite.dat": siteData}}
	previous := http.DefaultTransport
	http.DefaultTransport = f
	t.Cleanup(func() { http.DefaultTransport = previous })
	return f
}

func geoTestNode(t *testing.T) *model.NodeSpec {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return &model.NodeSpec{Protocol: "vless", ListenIP: "127.0.0.1", ServerPort: port, Network: "tcp"}
}

func TestGeoDataRoutesStart(t *testing.T) {
	cases := []struct{ source, kind string }{{"default", "ip"}, {"disabled", "ip"}, {"mixed", "site"}, {"preseed_env", "ip"}}
	for _, source := range []string{"panel", "structured", "panel_raw", "local", "file", "yaml_file", "preseed_config"} {
		cases = append(cases, struct{ source, kind string }{source, "ip"}, struct{ source, kind string }{source, "site"})
	}
	for _, tc := range cases {
		t.Run(tc.source+"/"+tc.kind, func(t *testing.T) {
			transport := newGeoTestTransport(t)
			geoDir, configDir := t.TempDir(), t.TempDir()
			// A stale global resource directory must not override this node's config.
			previousCanonical, previousAlias := t.TempDir(), t.TempDir()
			t.Setenv("xray.location.asset", previousCanonical)
			t.Setenv("XRAY_LOCATION_ASSET", previousAlias)
			node := geoTestNode(t)
			cfg := config.KernelConfig{Type: "xray", ConfigDir: configDir, GeoDataDir: geoDir, LogLevel: "error"}
			match, field, file := "geoip:private", "ip", "geoip.dat"
			if tc.kind == "site" {
				match, field, file = "geosite:test", "domain", "geosite.dat"
			}
			raw := map[string]any{"type": "field", field: []string{match}, "outboundTag": "block"}
			wantDownloads := []string{file}
			switch tc.source {
			case "default":
				wantDownloads = nil
			case "panel":
				node.Routes = []model.RouteRule{{ID: 1, Match: []string{match}, Action: "block"}}
			case "structured", "disabled":
				rule := model.CustomRouteRule{Name: "geo-regression", Action: model.RouteAction{Type: "block"}}
				if tc.kind == "site" {
					rule.Match.Domains = []string{match}
				} else {
					rule.Match.IPCIDRs = []string{match}
				}
				rule.Disabled = tc.source == "disabled"
				if rule.Disabled {
					wantDownloads = nil
				}
				node.CustomRouteRules = []model.CustomRouteRule{rule}
			case "panel_raw":
				node.CustomRoutes = []map[string]any{raw}
			case "local":
				cfg.CustomRoute = []map[string]any{raw}
			case "file", "yaml_file":
				data, err := json.Marshal(map[string]any{"routing": map[string]any{"rules": []map[string]any{raw}}})
				if err != nil {
					t.Fatal(err)
				}
				if tc.source == "yaml_file" {
					data = []byte(fmt.Sprintf("routing:\n  rules:\n    - type: field\n      %s: ['%s']\n      outboundTag: block\n", field, match))
				}
				cfg.CustomConfig = filepath.Join(configDir, "custom.conf")
				if err := os.WriteFile(cfg.CustomConfig, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "preseed_config", "preseed_env":
				cfg.CustomRoute = []map[string]any{raw}
				for name, data := range transport.data {
					if err := os.WriteFile(filepath.Join(geoDir, name), data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				wantDownloads = nil
				if tc.source == "preseed_env" {
					previousAlias = geoDir
					t.Setenv("XRAY_LOCATION_ASSET", previousAlias)
				}
			case "mixed":
				node.Routes = []model.RouteRule{{ID: 1, Match: []string{"geoip:private"}, Action: "block"}}
				node.CustomRoutes = []map[string]any{raw}
				wantDownloads = []string{"geoip.dat", "geosite.dat"}
			}
			x := New(cfg)
			defer x.Stop()
			if err := x.Start(node, testUsers, kernel.TLSCert{}); err != nil {
				t.Fatalf("start with %s geo rule: %v", tc.source, err)
			}
			if !slices.Equal(transport.requests, wantDownloads) {
				t.Fatalf("downloaded %v, want %v", transport.requests, wantDownloads)
			}
			for _, name := range wantDownloads {
				if data, err := os.ReadFile(filepath.Join(geoDir, name)); err != nil || !bytes.Equal(data, transport.data[name]) {
					t.Fatalf("invalid downloaded %s: %v", name, err)
				}
			}
			if os.Getenv("xray.location.asset") != previousCanonical || os.Getenv("XRAY_LOCATION_ASSET") != previousAlias {
				t.Fatal("starting the node changed process-wide resource settings")
			}
			t.Logf("started successfully; downloaded=%v", transport.requests)
		})
	}
}

func TestGeoDataNativeRoutingFields(t *testing.T) {
	for _, tc := range []struct {
		name, rule string
		ip, site   bool
	}{
		{"source", `{"source":["geoip:private"]}`, true, false},
		{"sourceIP_string", `{"sourceIP":"1.1.1.1,geoip:!private"}`, true, false},
		{"sourceIP_overrides_source", `{"sourceIP":[],"source":["geoip:private"]}`, false, false},
		{"localIP", `{"localIP":["geoip:private"]}`, true, false},
		{"domains_alias", `{"domains":"geosite:test"}`, false, true},
		{"external_default_files", `{"ip":["ext:geoip.dat:private","ext-ip:geoip.dat:private"],"domain":["ext:geosite.dat:test","ext-domain:geosite.dat:test"]}`, true, true},
		{"external_other_file", `{"ip":["ext:custom.dat:private"]}`, false, false},
		{"unrelated_strings", `{"ruleTag":"geoip:private","inboundTag":["geosite:test"],"domain":["full:geosite:test"],"ip":["127.0.0.0/8"]}`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ip, site, err := routingGeoDataNeeds([]byte(`{"routing":{"rules":[` + tc.rule + `]}}`))
			if err != nil || ip != tc.ip || site != tc.site {
				t.Fatalf("needs=(%v,%v), error=%v", ip, site, err)
			}
		})
	}
}

func TestGeoDataConcurrentNodes(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared_directory_%v", shared), func(t *testing.T) {
			transport := newGeoTestTransport(t)
			t.Setenv("xray.location.asset", t.TempDir())
			previous := os.Getenv("xray.location.asset")
			dirs := []string{t.TempDir(), t.TempDir()}
			if shared {
				dirs[1] = dirs[0]
			}
			nodes := []*model.NodeSpec{geoTestNode(t), geoTestNode(t)}
			start := make(chan struct{})
			results := make(chan error, len(nodes))
			for i, node := range nodes {
				ipMatch := "geoip:private"
				if !shared {
					// Each node must load its own category; choosing the other node's
					// directory fails even though both directories contain geoip.dat.
					category := fmt.Sprintf("NODE%d", i)
					ipMatch = "geoip:" + category
					data, err := proto.Marshal(&coreRouter.GeoIPList{Entry: []*coreRouter.GeoIP{{CountryCode: category, Cidr: []*coreRouter.CIDR{{Ip: []byte{127, 0, 0, 0}, Prefix: 8}}}}})
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dirs[i], "geoip.dat"), data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				x := New(config.KernelConfig{Type: "xray", ConfigDir: t.TempDir(), GeoDataDir: dirs[i], LogLevel: "error", CustomRoute: []map[string]any{{"type": "field", "ip": []string{ipMatch}, "domain": []string{"geosite:test"}, "outboundTag": "block"}}})
				t.Cleanup(x.Stop)
				go func() { <-start; results <- x.Start(node, testUsers, kernel.TLSCert{}) }()
			}
			close(start)
			for range nodes {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
			want := []string{"geosite.dat", "geosite.dat"}
			if shared {
				want = []string{"geoip.dat", "geosite.dat"}
			}
			slices.Sort(transport.requests)
			if !slices.Equal(transport.requests, want) {
				t.Fatalf("downloaded=%v, want=%v", transport.requests, want)
			}
			if os.Getenv("xray.location.asset") != previous {
				t.Fatal("concurrent nodes leaked resource settings")
			}
		})
	}
}

func TestGeoDataDownloadFailurePreservesRunningKernel(t *testing.T) {
	transport := newGeoTestTransport(t)
	transport.fail = true
	x := New(config.KernelConfig{Type: "xray", ConfigDir: t.TempDir(), LogLevel: "error"})
	node := geoTestNode(t)
	if err := x.Start(node, testUsers, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	defer x.Stop()
	instance := x.instance
	next := *node
	next.CustomRoutes = []map[string]any{{"type": "field", "ip": []string{"geoip:private"}, "outboundTag": "block"}}
	err := x.Reload(&next, testUsers, kernel.TLSCert{})
	if err == nil || !strings.Contains(err.Error(), "prepare xray geo data") || !strings.Contains(err.Error(), "503") {
		t.Fatalf("missing actionable download error: %v", err)
	}
	if !x.IsRunning() || x.instance != instance {
		t.Fatal("failed geo download replaced the running kernel")
	}
	if _, err := x.getUserManager(); err != nil {
		t.Fatal(err)
	}
}

func TestGeoDataParseFailureRestoresEnvironment(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			t.Setenv("xray.location.asset", "previous-asset-path")
			if !existing {
				if err := os.Unsetenv("xray.location.asset"); err != nil {
					t.Fatal(err)
				}
			}
			data := []byte(`{"routing":{"rules":[{"type":"field","outboundTag":"block","ip":["invalid-ip"]}]}}`)
			if _, _, err := createXrayInstance(data, t.TempDir()); err == nil {
				t.Fatal("invalid routing unexpectedly parsed")
			}
			value, found := os.LookupEnv("xray.location.asset")
			if found != existing || (existing && value != "previous-asset-path") {
				t.Fatal("failed parsing did not restore previous asset environment")
			}
		})
	}
}
