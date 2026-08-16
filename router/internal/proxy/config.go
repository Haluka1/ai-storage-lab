package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"ai-inference-storage-showcase/router/internal/common"
)

type Config struct {
	Server  ServerConfig         `json:"server"`
	Router  RouterConfig         `json:"router"`
	Workers []common.WorkerState `json:"workers"`
}

type ServerConfig struct {
	ListenAddr string `json:"listen_addr"`
	AdminAddr  string `json:"admin_addr"`
}

type RouterConfig struct {
	RunID           string          `json:"run_id"`
	Strategy        string          `json:"strategy"`
	BlockSizeTokens int             `json:"block_size_tokens"`
	CacheTTLMS      int             `json:"cache_ttl_ms"`
	DecisionLogPath string          `json:"decision_log_path"`
	TraceLogPath    string          `json:"trace_log_path"`
	EntryTopology   common.Topology `json:"entry_topology"`
}

func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, err
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (cfg *Config) ApplyDefaults() {
	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = "127.0.0.1:8080"
	}
	if cfg.Server.AdminAddr == "" {
		cfg.Server.AdminAddr = "127.0.0.1:9090"
	}
	if cfg.Router.RunID == "" {
		cfg.Router.RunID = "router_proxy_local"
	}
	if cfg.Router.Strategy == "" {
		cfg.Router.Strategy = "cost_aware"
	}
	if cfg.Router.BlockSizeTokens <= 0 {
		cfg.Router.BlockSizeTokens = 16
	}
	if cfg.Router.CacheTTLMS <= 0 {
		cfg.Router.CacheTTLMS = int((30 * time.Second).Milliseconds())
	}
	// Persistent decision and trace logs are opt-in. The public local path
	// stays hermetic unless a caller explicitly supplies a temporary file.
	if cfg.Router.EntryTopology.Unknown() {
		cfg.Router.EntryTopology = common.Topology{
			Cloud:     "local",
			Region:    "local",
			Zone:      "zone-a",
			ClusterID: "cluster-a",
			NodeID:    "router-local",
		}
	}
	for i := range cfg.Workers {
		normalizeWorkerDefaults(&cfg.Workers[i], cfg.Router.EntryTopology)
		if cfg.Workers[i].LastUpdated.IsZero() {
			cfg.Workers[i].LastUpdated = time.Now().UTC()
		}
	}
}

func (cfg Config) Validate() error {
	if cfg.Server.ListenAddr == cfg.Server.AdminAddr {
		return errors.New("router public and admin listeners must be different")
	}
	adminHost, _, err := net.SplitHostPort(cfg.Server.AdminAddr)
	if err != nil {
		return fmt.Errorf("invalid router admin_addr: %w", err)
	}
	if !isLoopbackHost(adminHost) {
		return errors.New("router admin_addr must bind to loopback")
	}
	if len(cfg.Workers) == 0 {
		return errors.New("router config must contain at least one worker")
	}
	workerIDs := make(map[common.WorkerID]struct{}, len(cfg.Workers))
	for _, worker := range cfg.Workers {
		if worker.ID == "" {
			return errors.New("worker id must not be empty")
		}
		if worker.URL == "" {
			return errors.New("worker url must not be empty")
		}
		if _, duplicate := workerIDs[worker.ID]; duplicate {
			return fmt.Errorf("duplicate worker id %q", worker.ID)
		}
		workerIDs[worker.ID] = struct{}{}
		if worker.RouterInflightRequests != 0 {
			return fmt.Errorf("worker %q router_inflight_requests is router-managed and must not be configured", worker.ID)
		}
		if worker.QueueDepth < 0 || worker.ActiveDecodeBlocks < 0 || worker.InflightRequests < 0 {
			return fmt.Errorf("worker %q queue, decode, and inflight counts must be non-negative", worker.ID)
		}
	}
	return nil
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func normalizeWorkerDefaults(worker *common.WorkerState, fallbackTopology common.Topology) {
	if worker.Health == "" {
		worker.Health = common.WorkerReady
	}
	if worker.ReadinessState == "" {
		switch worker.Health {
		case common.WorkerReady:
			worker.ReadinessState = common.ReadinessReady
		case common.WorkerDraining:
			worker.ReadinessState = common.ReadinessDraining
			worker.Draining = true
		case common.WorkerUnhealthy:
			worker.ReadinessState = common.ReadinessUnhealthy
		default:
			worker.ReadinessState = common.ReadinessNotReady
		}
	}
	if worker.Draining || worker.Health == common.WorkerDraining || worker.ReadinessState == common.ReadinessDraining {
		worker.Draining = true
		worker.Health = common.WorkerDraining
		worker.ReadinessState = common.ReadinessDraining
	}
	if worker.MemoryPressure == 0 && (worker.GPUMemoryPressure > 0 || worker.KVCachePressure > 0) {
		worker.MemoryPressure = worker.ResourcePressure()
	}
	if worker.Topology.Unknown() {
		worker.Topology = fallbackTopology
	}
}
