package awsutils

import (
	"bsyncgcs/config"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sourcegraph/conc/pool"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func RunWorker(appCfg *config.AppConfig, awsCfg aws.Config) {
	// Initialize clients
	sqsClient := sqs.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg)
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.Concurrency = 5
	})

	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		log.Fatalf("Failed to create GCS client: %v", err)
	}
	defer gcsClient.Close()

	gcsBucket := gcsClient.Bucket(appCfg.GcsBucketName)

	// Configure concurrency
	taskPool := pool.New().WithMaxGoroutines(20)
	receivePool := pool.New().WithMaxGoroutines(10)

	// Start processing SQS messages
	err = processSQSMessages(
		appCfg, sqsClient, uploader, gcsBucket, taskPool, receivePool,
	)
	if err != nil {
		log.Fatalf("Failed to process SQS messages: %v", err)
	}

	log.Println("Worker completed successfully.")
}

func processSQSMessages(
	appCfg *config.AppConfig,
	sqsClient *sqs.Client,
	uploader *manager.Uploader,
	bucket *storage.BucketHandle,
	taskPool *pool.Pool,
	receivePool *pool.Pool,
) error {
	var mu sync.Mutex
	var receiptHandles []types.DeleteMessageBatchRequestEntry

	for i := 0; i < receivePool.MaxGoroutines(); i++ {
		receivePool.Go(func() {
			for {
				output, err := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(appCfg.SqsQueueUrl),
					MaxNumberOfMessages: int32(appCfg.SqsBatchSize),
					WaitTimeSeconds:     10,
				})
				if err != nil {
					log.Printf("Failed to receive messages: %v", err)
					continue
				}

				if len(output.Messages) == 0 {
					log.Println("No more messages to process.")
					return
				}

				for _, message := range output.Messages {
					message := message // Capture range variable
					taskPool.Go(func() {
						fileName := strings.Trim(*message.Body, `"`)
						err := transferFileGCS2S3WithRetry(
							bucket, uploader, appCfg, fileName,
						)
						if err != nil {
							if strings.Contains(err.Error(), "object not found") {
								log.Printf("Object not found: %v", err)
							} else {
								log.Printf(
									"Failed to transfer file %s: %v",
									fileName, err,
								)
							}
							// Do not delete the message
							return
						}

						// Add receipt handle for deletion
						mu.Lock()
						receiptHandles = append(
							receiptHandles,
							types.DeleteMessageBatchRequestEntry{
								Id:            aws.String(*message.MessageId),
								ReceiptHandle: message.ReceiptHandle,
							},
						)
						mu.Unlock()

						// Delete messages in batches of 10
						mu.Lock()
						if len(receiptHandles) >= 10 {
							batch := receiptHandles[:10]
							receiptHandles = receiptHandles[10:]
							mu.Unlock()
							deleteMessages(sqsClient, appCfg, batch)
						} else {
							mu.Unlock()
						}
					})
				}
			}
		})
	}

	receivePool.Wait()
	taskPool.Wait()

	// Delete remaining messages
	mu.Lock()
	if len(receiptHandles) > 0 {
		batch := receiptHandles
		mu.Unlock()
		deleteMessages(sqsClient, appCfg, batch)
	} else {
		mu.Unlock()
	}

	return nil
}

func deleteMessages(
	sqsClient *sqs.Client,
	appCfg *config.AppConfig,
	receiptHandles []types.DeleteMessageBatchRequestEntry,
) {
	batchSize := 10

	for i := 0; i < len(receiptHandles); i += batchSize {
		end := i + batchSize
		if end > len(receiptHandles) {
			end = len(receiptHandles)
		}
		batch := receiptHandles[i:end]

		_, err := sqsClient.DeleteMessageBatch(context.TODO(), &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(appCfg.SqsQueueUrl),
			Entries:  batch,
		})
		if err != nil {
			log.Printf("Failed to delete messages: %v", err)
		} else {
			log.Printf("Deleted a batch of %d messages", len(batch))
		}
	}
}

func transferFileGCS2S3WithRetry(
	bucket *storage.BucketHandle,
	uploader *manager.Uploader,
	appCfg *config.AppConfig,
	fileName string,
) error {
	var err error
	maxRetries := 5
	baseDelay := 1 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = transferFileGCS2S3(bucket, uploader, appCfg, fileName)
		if !isRateLimitError(err) {
			return err
		}

		// Exponential backoff with jitter
		waitTime := baseDelay * time.Duration(math.Pow(2, float64(attempt))) *
			time.Duration(rand.Float64()+0.5)
		if waitTime > 30*time.Second {
			waitTime = 30 * time.Second
		}

		log.Printf(
			"Rate limit error for file %s. Retrying in %v...",
			fileName, waitTime,
		)
		time.Sleep(waitTime)
	}

	return fmt.Errorf(
		"Failed to transfer file %s after %d attempts: %v",
		fileName, maxRetries, err,
	)
}

func isRateLimitError(err error) bool {
	if gErr, ok := err.(*googleapi.Error); ok {
		switch gErr.Code {
		case 408, 429, 500, 502, 503, 504:
			return true
		}
	}
	return false
}

func transferFileGCS2S3(
	bucket *storage.BucketHandle,
	uploader *manager.Uploader,
	appCfg *config.AppConfig,
	fileName string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gcsReader, err := bucket.Object(fileName).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			log.Printf("GCS object %s not found: %v", fileName, err)
			return fmt.Errorf("object not found: %s", fileName)
		}
		log.Printf("Failed to create GCS reader for %s: %v", fileName, err)
		return fmt.Errorf("failed to create GCS reader: %v", err)
	}
	defer gcsReader.Close()

	keyPrefix := generateS3Key(fileName)
	s3Key := fmt.Sprintf("%s/%s", keyPrefix, fileName)

	object := bucket.Object(fileName)
	attrs, err := object.Attrs(ctx)
	if err != nil {
		log.Printf("Failed to get GCS attributes for %s: %v", fileName, err)
		return fmt.Errorf("failed to get GCS attributes: %v", err)
	}

	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(appCfg.S3BucketName),
		Key:         aws.String(s3Key),
		Body:        gcsReader,
		ContentType: &attrs.ContentType,
	})
	if err != nil {
		log.Printf("Failed to upload to S3: %v", err)
		return fmt.Errorf("failed to upload to S3: %v", err)
	}

	log.Printf(
		"Transferred file %s from GCS to S3 at %s",
		fileName, s3Key,
	)
	return nil
}

func generateS3Key(fileName string) string {
	if len(fileName) < 8 {
		log.Printf(
			"Filename %s is too short, using default path",
			fileName,
		)
		return "00/00/00/00"
	}

	octets := []string{
		fileName[0:2],
		fileName[2:4],
		fileName[4:6],
		fileName[6:8],
	}

	return strings.Join(octets, "/")
}
