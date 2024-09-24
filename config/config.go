package config

import (
	"log"

	"github.com/kelseyhightower/envconfig"
)

type AppConfig struct {
	AWSProfile        string `split_words:"true" required:"false"`
	AWSRegion         string `split_words:"true" required:"true"`
	LoggerFile        string `split_words:"true" required:"false"`
	S3BucketName      string `split_words:"true" required:"true"`
	SqsQueueUrl       string `split_words:"true" required:"true"`
	ObjectName        string `split_words:"true" required:"false"`
	GcsBucketName     string `split_words:"true" required:"false"`
	SqsBatchSize      int    `split_words:"true" required:"false" default:"10"`
	WorkerQueueLength int64  `split_words:"true" required:"false" default:"100"`
	MessageLimit      int64  `split_words:"true" required:"false"`
	Mode              string `split_words:"true" required:"true"`
	MemoryLimit       string `split_words:"true" required:"false"`
}

func LoadConfig() *AppConfig {
	var appCfg AppConfig
	err := envconfig.Process("migration_bucket", &appCfg)
	if err != nil {
		log.Fatalf("Failed to process environment variables: %v", err)
	}
	return &appCfg
}
