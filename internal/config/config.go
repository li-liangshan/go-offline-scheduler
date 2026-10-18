package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

const envPrefix = "SCHEDULER"

// AppConfig 调度器运行配置。所有字段均可通过 SCHEDULER_ 前缀环境变量覆盖，
// 例如 workerImage 可设置为 SCHEDULER_WORKERIMAGE。
type AppConfig struct {
	Namespace            string `mapstructure:"namespace"`
	GlobalMaxConcurrency int    `mapstructure:"globalMaxConcurrency"`
	HttpListenAddr       string `mapstructure:"httpListenAddr"`
	HttpReadHeaderSec    int    `mapstructure:"httpReadHeaderTimeoutSec"`
	HttpReadSec          int    `mapstructure:"httpReadTimeoutSec"`
	HttpWriteSec         int    `mapstructure:"httpWriteTimeoutSec"`
	HttpIdleSec          int    `mapstructure:"httpIdleTimeoutSec"`
	HttpMaxBodyBytes     int64  `mapstructure:"httpMaxBodyBytes"`
	ApiAuthToken         string `mapstructure:"apiAuthToken"`

	NatsUrl             string `mapstructure:"natsUrl"`
	NatsMaxDeliver      int    `mapstructure:"natsMaxDeliver"`
	NatsStreamMaxAgeSec int    `mapstructure:"natsStreamMaxAgeSec"`

	PebbleDataPath string `mapstructure:"pebbleDataPath"`
	PebbleCacheMB  int    `mapstructure:"pebbleCacheMB"`
	BackupDir      string `mapstructure:"backupDir"`
	BackupInterval int    `mapstructure:"backupIntervalSec"`
	BackupTimeout  int    `mapstructure:"backupTimeoutSec"`
	BackupRetain   int    `mapstructure:"backupRetainCount"`

	TaskRetentionSec   int `mapstructure:"taskRetentionSec"`
	CleanupIntervalSec int `mapstructure:"cleanupIntervalSec"`

	LeaseName    string `mapstructure:"leaseName"`
	OtelEndpoint string `mapstructure:"otelEndpoint"`
	LogLevel     string `mapstructure:"logLevel"`

	WorkerImage            string `mapstructure:"workerImage"`
	WorkerImagePullPolicy  string `mapstructure:"workerImagePullPolicy"`
	WorkerCPURequest       string `mapstructure:"workerCpuRequest"`
	WorkerCPULimit         string `mapstructure:"workerCpuLimit"`
	WorkerMemoryRequest    string `mapstructure:"workerMemoryRequest"`
	WorkerMemoryLimit      string `mapstructure:"workerMemoryLimit"`
	WorkerServiceAccount   string `mapstructure:"workerServiceAccount"`
	JobTTLAfterFinishedSec int    `mapstructure:"jobTtlSecondsAfterFinished"`
	DeleteFinishedJobs     bool   `mapstructure:"deleteFinishedJobs"`

	ResourceCheckEnabled bool `mapstructure:"resourceCheckEnabled"`
}

func LoadConfig(path string) (*AppConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg AppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.applyAliases()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("namespace", "offline-job")
	v.SetDefault("globalMaxConcurrency", 50)
	v.SetDefault("httpListenAddr", ":8080")
	v.SetDefault("httpReadHeaderTimeoutSec", 5)
	v.SetDefault("httpReadTimeoutSec", 30)
	v.SetDefault("httpWriteTimeoutSec", 30)
	v.SetDefault("httpIdleTimeoutSec", 60)
	v.SetDefault("httpMaxBodyBytes", int64(1<<20))
	v.SetDefault("natsUrl", "nats://127.0.0.1:4222")
	v.SetDefault("natsMaxDeliver", 5)
	v.SetDefault("natsStreamMaxAgeSec", 24*3600)
	v.SetDefault("pebbleDataPath", "./pebble-data")
	v.SetDefault("pebbleCacheMB", 64)
	v.SetDefault("backupDir", "./pebble-backup")
	v.SetDefault("backupIntervalSec", 3600)
	v.SetDefault("backupTimeoutSec", 600)
	v.SetDefault("backupRetainCount", 24)
	// 默认保留 30 天终态任务；设置为 0 可关闭自动清理。
	v.SetDefault("taskRetentionSec", 30*24*3600)
	v.SetDefault("cleanupIntervalSec", 600)
	v.SetDefault("leaseName", "offline-scheduler-leader")
	v.SetDefault("otelEndpoint", "otel-collector:4318")
	v.SetDefault("logLevel", "info")
	v.SetDefault("workerImagePullPolicy", "IfNotPresent")
	v.SetDefault("workerCpuRequest", "500m")
	v.SetDefault("workerCpuLimit", "2")
	v.SetDefault("workerMemoryRequest", "1Gi")
	v.SetDefault("workerMemoryLimit", "4Gi")
	v.SetDefault("jobTtlSecondsAfterFinished", 3600)
	v.SetDefault("deleteFinishedJobs", false)
	v.SetDefault("resourceCheckEnabled", true)
}

// applyAliases 兼容旧字段名，保证已有部署配置可以平滑升级。
func (c *AppConfig) applyAliases() {
	if c.BackupInterval <= 0 {
		c.BackupInterval = 3600
	}
	if c.BackupTimeout <= 0 {
		c.BackupTimeout = 600
	}
	if c.BackupRetain <= 0 {
		c.BackupRetain = 24
	}
	if c.PebbleCacheMB <= 0 {
		c.PebbleCacheMB = 64
	}
	if c.NatsMaxDeliver <= 0 {
		c.NatsMaxDeliver = 5
	}
	if c.NatsStreamMaxAgeSec <= 0 {
		c.NatsStreamMaxAgeSec = 24 * 3600
	}
	if c.CleanupIntervalSec <= 0 {
		c.CleanupIntervalSec = 600
	}
	if c.HttpReadHeaderSec <= 0 {
		c.HttpReadHeaderSec = 5
	}
	if c.HttpReadSec <= 0 {
		c.HttpReadSec = 30
	}
	if c.HttpWriteSec <= 0 {
		c.HttpWriteSec = 30
	}
	if c.HttpIdleSec <= 0 {
		c.HttpIdleSec = 60
	}
	if c.HttpMaxBodyBytes <= 0 {
		c.HttpMaxBodyBytes = 1 << 20
	}
	if c.WorkerImagePullPolicy == "" {
		c.WorkerImagePullPolicy = "IfNotPresent"
	}
}

func (c *AppConfig) Validate() error {
	required := map[string]string{
		"namespace":           c.Namespace,
		"httpListenAddr":      c.HttpListenAddr,
		"natsUrl":             c.NatsUrl,
		"pebbleDataPath":      c.PebbleDataPath,
		"backupDir":           c.BackupDir,
		"leaseName":           c.LeaseName,
		"workerImage":         c.WorkerImage,
		"workerCpuRequest":    c.WorkerCPURequest,
		"workerCpuLimit":      c.WorkerCPULimit,
		"workerMemoryRequest": c.WorkerMemoryRequest,
		"workerMemoryLimit":   c.WorkerMemoryLimit,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("config %s is required", name)
		}
	}
	if c.GlobalMaxConcurrency <= 0 {
		return fmt.Errorf("globalMaxConcurrency must be greater than 0")
	}
	if c.JobTTLAfterFinishedSec < 0 {
		return fmt.Errorf("jobTtlSecondsAfterFinished cannot be negative")
	}
	if c.TaskRetentionSec < 0 {
		return fmt.Errorf("taskRetentionSec cannot be negative")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("unsupported logLevel %q", c.LogLevel)
	}
	switch c.WorkerImagePullPolicy {
	case "Always", "IfNotPresent", "Never":
	default:
		return fmt.Errorf("unsupported workerImagePullPolicy %q", c.WorkerImagePullPolicy)
	}
	return nil
}

func (c *AppConfig) HTTPReadHeaderTimeout() time.Duration {
	return time.Duration(c.HttpReadHeaderSec) * time.Second
}

func (c *AppConfig) HTTPReadTimeout() time.Duration {
	return time.Duration(c.HttpReadSec) * time.Second
}

func (c *AppConfig) HTTPWriteTimeout() time.Duration {
	return time.Duration(c.HttpWriteSec) * time.Second
}

func (c *AppConfig) HTTPIdleTimeout() time.Duration {
	return time.Duration(c.HttpIdleSec) * time.Second
}

func (c *AppConfig) BackupIntervalDuration() time.Duration {
	return time.Duration(c.BackupInterval) * time.Second
}

func (c *AppConfig) BackupTimeoutDuration() time.Duration {
	return time.Duration(c.BackupTimeout) * time.Second
}
