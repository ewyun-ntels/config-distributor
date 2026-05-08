package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultConfigPath = "config.yaml"
)

type Config struct {
	Log        LogConfig        `yaml:"log"`
	Server     ServerConfig     `yaml:"server"`
	NATS       NATSConfig       `yaml:"nats"`
	Bootstrap  BootstrapConfig  `yaml:"bootstrap"`
	Reconciler ReconcilerConfig `yaml:"reconciler"`
	Filter     FilterConfig     `yaml:"filter"`
}

type LogConfig struct {
	Level string `yaml:"level"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type NATSConfig struct {
	URL    string `yaml:"url"`
	Bucket string `yaml:"bucket"`
}

type BootstrapConfig struct {
	Namespaces []string `yaml:"namespaces"`
}

type ReconcilerConfig struct {
	Enabled         bool `yaml:"enabled"`
	IntervalSeconds int  `yaml:"intervalSeconds"`
}

type FilterConfig struct {
	ManagedLabel ManagedLabelConfig `yaml:"managedLabel"`
}

type ManagedLabelConfig struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

// LoadConfig loads configuration from file.
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := Config{Reconciler: ReconcilerConfig{Enabled: true}}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	setDefaults(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func (c Config) SlogLevel() (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(c.Log.Level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unsupported log level %q", c.Log.Level)
	}
}

// setDefaults sets default values for configuration.
func setDefaults(cfg *Config) {
	if strings.TrimSpace(cfg.Log.Level) == "" {
		cfg.Log.Level = "info"
	}
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8080
	}
	if strings.TrimSpace(cfg.NATS.URL) == "" {
		cfg.NATS.URL = "nats://localhost:4222"
	}
	if strings.TrimSpace(cfg.NATS.Bucket) == "" {
		cfg.NATS.Bucket = "UPM_CONFIG"
	}
	if len(cfg.Bootstrap.Namespaces) == 0 {
		cfg.Bootstrap.Namespaces = []string{"default"}
	}
	if cfg.Reconciler.IntervalSeconds == 0 {
		cfg.Reconciler.IntervalSeconds = 300
	}
	if strings.TrimSpace(cfg.Filter.ManagedLabel.Key) == "" {
		cfg.Filter.ManagedLabel.Key = "config.upm.io/managed"
	}
	if strings.TrimSpace(cfg.Filter.ManagedLabel.Value) == "" {
		cfg.Filter.ManagedLabel.Value = "true"
	}
}

// Validate checks the configuration for invalid values.
// Called after setDefaults, so string fields are already non-empty.
// Only port range requires explicit validation since it can be set to an invalid value.
func (cfg *Config) Validate() error {
	if cfg.Server.Port <= 0 || cfg.Server.Port > 65535 {
		return fmt.Errorf("server.port: invalid port %d (must be 1-65535)", cfg.Server.Port)
	}
	if cfg.Reconciler.IntervalSeconds < 0 {
		return fmt.Errorf("reconciler.intervalSeconds: invalid value %d (must be >= 0)", cfg.Reconciler.IntervalSeconds)
	}
	return nil
}
