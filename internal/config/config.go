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
	Log    LogConfig    `yaml:"log"`
	Server ServerConfig `yaml:"server"`
	NATS   NATSConfig   `yaml:"nats"`
	Watch  WatchConfig  `yaml:"watch"`
	Filter FilterConfig `yaml:"filter"`
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

type WatchConfig struct {
	Namespaces []string `yaml:"namespaces"`
	Resources  []string `yaml:"resources"`
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

	var cfg Config
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
	if len(cfg.Watch.Namespaces) == 0 {
		cfg.Watch.Namespaces = []string{"default"}
	}
	if len(cfg.Watch.Resources) == 0 {
		cfg.Watch.Resources = []string{"configmaps", "secrets"}
	}
	if strings.TrimSpace(cfg.Filter.ManagedLabel.Key) == "" {
		cfg.Filter.ManagedLabel.Key = "config.upm.io/managed"
	}
	if strings.TrimSpace(cfg.Filter.ManagedLabel.Value) == "" {
		cfg.Filter.ManagedLabel.Value = "true"
	}
}

// Validate checks the configuration for invalid or missing values.
// Called after setDefaults, so only checks values that must be explicitly provided
// or values that could be set to invalid ranges.
func (cfg *Config) Validate() error {
	var errs []string

	if cfg.Server.Port <= 0 || cfg.Server.Port > 65535 {
		errs = append(errs, fmt.Sprintf("server.port: invalid port %d (must be 1-65535)", cfg.Server.Port))
	}
	if strings.TrimSpace(cfg.NATS.URL) == "" {
		errs = append(errs, "nats.url is required")
	}
	if strings.TrimSpace(cfg.NATS.Bucket) == "" {
		errs = append(errs, "nats.bucket is required")
	}
	if strings.TrimSpace(cfg.Filter.ManagedLabel.Key) == "" {
		errs = append(errs, "filter.managedLabel.key is required")
	}
	if strings.TrimSpace(cfg.Filter.ManagedLabel.Value) == "" {
		errs = append(errs, "filter.managedLabel.value is required")
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}
