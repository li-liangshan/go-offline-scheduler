package config

import (
	"github.com/spf13/viper"
)

type AppConfig struct {
	Namespace            string `mapstructure:"namespace"`
	GlobalMaxConcurrency int    `mapstructure:"globalMaxConcurrency"`
	HttpListenAddr       string `mapstructure:"httpListenAddr"`
	NatsUrl              string `mapstructure:"natsUrl"`
	PebbleDataPath       string `mapstructure:"pebbleDataPath"`
	BackupDir            string `mapstructure:"backupDir"`
	LeaseName            string `mapstructure:"leaseName"`
	OtelEndpoint         string `mapstructure:"otelEndpoint"`
	LogLevel             string `mapstructure:"logLevel"`
}

func LoadConfig(path string) (*AppConfig, error) {
	viper.SetConfigFile(path)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	var cfg AppConfig
	err = viper.Unmarshal(&cfg)
	return &cfg, err
}
