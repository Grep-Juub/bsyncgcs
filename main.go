package main

import (
	"context"
	"log"

	"bsyncgcs/awsutils"
	"bsyncgcs/config"
	"bsyncgcs/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
)

func main() {
	appCfg := config.LoadConfig()

	// Setup Logger
	err := utils.SetupLogger(appCfg.LoggerFile)
	if err != nil {
		log.Fatalf("Failed to set up logger: %v", err)
	}

	// Load AWS Config
	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.TODO(),
		awsconfig.WithRegion(appCfg.AWSRegion),
		awsconfig.WithAssumeRoleCredentialOptions(func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = "session-name"
		}),
	)
	if err != nil {
		log.Fatalf("Unable to load SDK config: %v", err)
	}

	// Initialize SQS Client
	awsutils.InitSQSClient(awsCfg)

	// Run based on the Mode
	switch appCfg.Mode {
	case "populator":
		runPopulator(appCfg, awsCfg)
	case "worker":
		runWorker(appCfg, awsCfg)
	default:
		log.Fatalf("Invalid mode specified: %s", appCfg.Mode)
	}
}

func runPopulator(appCfg *config.AppConfig, awsCfg aws.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := make(chan []string, appCfg.WorkerQueueLength)
	var bytesRead int64

	// Get total size of the S3 object
	totalSize, err := awsutils.GetS3ObjectSize(
		ctx, awsCfg, appCfg.S3BucketName, appCfg.ObjectName,
	)
	if err != nil {
		log.Fatalf("Failed to get S3 object size: %v", err)
	}

	// Start streaming CSV from S3
	go func() {
		err := awsutils.StreamCSVFromS3(
			ctx, awsCfg, appCfg.S3BucketName, appCfg.ObjectName,
			10, stream, &bytesRead,
		)
		if err != nil {
			log.Printf("Error in StreamCSVFromS3: %v", err)
		}
		cancel()
	}()

	done := make(chan struct{})
	go utils.DisplayProgress(done, totalSize, &bytesRead)

	// Process the stream and send messages to SQS
	awsutils.Process(ctx, appCfg.SqsQueueUrl, stream, awsCfg, appCfg)

	close(done)
}

func runWorker(appCfg *config.AppConfig, awsCfg aws.Config) {
	awsutils.RunWorker(appCfg, awsCfg)
}
