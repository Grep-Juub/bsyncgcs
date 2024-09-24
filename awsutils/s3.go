package awsutils

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"bsyncgcs/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func GetS3ObjectSize(ctx context.Context, cfg aws.Config, bucketName, objectName string) (int64, error) {
	s3Client := s3.NewFromConfig(cfg)

	// Get the object head from S3
	headOutput, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get object head from S3: %w", err)
	}

	return *headOutput.ContentLength, nil
}

func StreamCSVFromS3(ctx context.Context, cfg aws.Config, bucketName, objectName string, batchSize int, batchChan chan<- []string, bytesRead *int64) error {
	s3Client := s3.NewFromConfig(cfg)

	// Get the object from S3
	output, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer output.Body.Close()

	// Wrap the S3 response body with CountingReader to count compressed bytes read
	countingReader := &utils.CountingReader{Reader: output.Body}

	// Create a gzip reader
	gzReader, err := gzip.NewReader(countingReader)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	reader := bufio.NewReaderSize(gzReader, 128*1024)

	var batch []string

	for {
		if ctx.Err() != nil {
			// Context canceled, stop processing
			return ctx.Err()
		}

		line, err := reader.ReadString('\n')
		if err != nil && err.Error() != "EOF" {
			return fmt.Errorf("error reading from S3 stream: %w", err)
		}

		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			batch = append(batch, line)
		}

		if len(batch) == batchSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case batchChan <- batch:
				batch = nil
			}
		}

		// Update bytesRead
		atomic.StoreInt64(bytesRead, countingReader.BytesRead())

		if err != nil && err.Error() == "EOF" {
			break
		}
	}

	// Send any remaining batch
	if len(batch) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batchChan <- batch:
		}
	}

	close(batchChan)
	return nil
}
