package awsutils

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"bsyncgcs/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

var (
	sqsClient     *sqs.Client
	sqsClientLock sync.RWMutex
)

func InitSQSClient(cfg aws.Config) {
	sqsClientLock.Lock()
	defer sqsClientLock.Unlock()
	if sqsClient == nil {
		sqsClient = sqs.NewFromConfig(cfg)
	}
}

func getSQSClient() *sqs.Client {
	sqsClientLock.RLock()
	defer sqsClientLock.RUnlock()
	return sqsClient
}

func recreateSQSClient(cfg aws.Config) {
	sqsClientLock.Lock()
	defer sqsClientLock.Unlock()
	sqsClient = sqs.NewFromConfig(cfg)
}

func SendMessageToSQS(queueURL string, batch []types.SendMessageBatchRequestEntry, awsCfg aws.Config) error {
	maxRetries := 5
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Get the current SQS client
		client := getSQSClient()

		// Use context.Background() to ensure the send operation isn't canceled prematurely
		_, err = client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  batch,
		})
		if err == nil {
			return nil
		}

		log.Printf("Failed to send message batch to SQS (attempt %d/%d): %v", attempt, maxRetries, err)

		// Recreate client if needed
		recreateSQSClient(awsCfg)

		// Wait before retrying (exponential backoff)
		backoff := time.Duration(1<<uint(attempt-1)) * time.Second
		time.Sleep(backoff)
	}

	return err
}

func Process(ctx context.Context, queueURL string, stream <-chan []string, awsCfg aws.Config, appCfg *config.AppConfig) {
	numWorkers := 100
	var wg sync.WaitGroup

	var totalMessagesSent int64

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case batch, ok := <-stream:
					if !ok {
						return
					}

					var messagesToSend int64 = int64(len(batch))
					batchToSend := batch

					// Check if we've reached the message limit
					currentTotal := atomic.LoadInt64(&totalMessagesSent)
					if appCfg.MessageLimit > 0 && currentTotal >= appCfg.MessageLimit {
						// Limit reached, stop processing
						return
					}

					// Adjust batch size if necessary
					if appCfg.MessageLimit > 0 {
						remaining := appCfg.MessageLimit - currentTotal
						if messagesToSend > remaining {
							messagesToSend = remaining
							batchToSend = batch[:messagesToSend]
						}
					}

					// Atomically increment totalMessagesSent
					newTotal := atomic.AddInt64(&totalMessagesSent, messagesToSend)

					var entries []types.SendMessageBatchRequestEntry
					for _, msg := range batchToSend {
						id := uuid.New().String()
						entries = append(entries, types.SendMessageBatchRequestEntry{
							Id:          aws.String(id),
							MessageBody: aws.String(msg),
						})
					}

					// Send the batch to SQS
					err := SendMessageToSQS(queueURL, entries, awsCfg)
					if err != nil {
						log.Printf("Failed to send message to SQS: %v", err)
						// Adjust totalMessagesSent if send failed
						atomic.AddInt64(&totalMessagesSent, -messagesToSend)
					}

					// If we've reached the message limit, exit
					if appCfg.MessageLimit > 0 && newTotal >= appCfg.MessageLimit {
						return
					}
				}
			}
		}()
	}

	wg.Wait()
}
