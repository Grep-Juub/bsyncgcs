
# BsyncGcs (Billion Sync GCS)

BsyncGcs is a tool designed to synchronize a vast number of small files from Google Cloud Storage (GCS) to Amazon S3. By leveraging a distributed architecture and the power of AWS SQS queues, BsyncGcs efficiently handles the migration of billions of files, making it ideal for large-scale data transfer tasks.

## Table of Contents

- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the Populator](#running-the-populator)
  - [Running the Worker](#running-the-worker)
- [Example Scripts](#example-scripts)
  - [Populator Mode Script](#populator-mode-script)
  - [Worker Mode Script](#worker-mode-script)
- [Context and Motivation](#context-and-motivation)

## How It Works

BsyncGcs operates in two modes:

1. **Populator Mode**: Reads a CSV file containing a list of file names stored in GCS and populates an AWS SQS queue with messages representing each file to be transferred.
2. **Worker Mode**: Consumes messages from the SQS queue and transfers the corresponding files from GCS to S3.

This separation allows for scalable and efficient processing, enabling multiple workers to run in parallel and handle large volumes of data.

## Prerequisites

- **Go**: Version 1.16 or higher.
- **AWS Account**: With access to S3 and SQS services.
- **Google Cloud Account**: With access to the GCS bucket containing the files.
- **AWS Credentials**: Configured via AWS CLI or environment variables.
- **Google Cloud Credentials**: If required, for accessing non-public GCS buckets.

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/grep-juub/bsyngcs.git
   cd bsyngcs
   ```

2. **Install Dependencies**

   ```bash
   go mod download
   ```

## Configuration

BsyncGcs uses environment variables for configuration. These variables can be set directly in your shell or via a `.env` file.

### Common Environment Variables

- `MIGRATION_BUCKET_AWS_REGION`: AWS region where your resources are located.
- `MIGRATION_BUCKET_SQS_QUEUE_URL`: URL of the SQS queue.
- `MIGRATION_BUCKET_S3_BUCKET_NAME`: Name of the S3 bucket.

### Populator-Specific Variables

- `MIGRATION_BUCKET_MODE`: Set to `populator`.
- `MIGRATION_BUCKET_OBJECT_NAME`: S3 object key of the compressed CSV file containing the list of files.
- `MIGRATION_BUCKET_WORKER_QUEUE_LENGTH`: (Optional) Size of the worker queue.
- `MIGRATION_BUCKET_MESSAGE_LIMIT`: (Optional) Limit the number of messages to process.

### Worker-Specific Variables

- `MIGRATION_BUCKET_MODE`: Set to `worker`.
- `MIGRATION_BUCKET_GCS_BUCKET_NAME`: Name of the GCS bucket containing the files.
- `MIGRATION_BUCKET_SQS_BATCH_SIZE`: Number of SQS messages to process per batch.
- `MIGRATION_BUCKET_WORKER_QUEUE_LENGTH`: (Optional) Size of the worker queue.

## Usage

### Running the Populator

The populator reads a CSV file from S3, which contains a list of file names, and populates an SQS queue with messages for each file.

1. **Set Environment Variables**

   Configure the environment variables as per your setup. See the [Example Scripts](#example-scripts) section for guidance.

2. **Run the Populator**

   ```bash
   go run main.go
   ```

### Running the Worker

The worker consumes messages from the SQS queue and transfers the specified files from GCS to S3.

1. **Set Environment Variables**

   Configure the environment variables as per your setup.

2. **Run the Worker**

   ```bash
   go run main.go
   ```

## Example Scripts

Below are example scripts to run BsyncGcs in both populator and worker modes.

### Populator Mode Script

Create a file named `run_populator.sh`:

```bash
#!/bin/bash

# Set environment variables for Populator mode
export MIGRATION_BUCKET_AWS_REGION="us-west-2"
export MIGRATION_BUCKET_SQS_QUEUE_URL="https://sqs.us-west-2.amazonaws.com/123456789012/your-queue"
export MIGRATION_BUCKET_S3_BUCKET_NAME="your-s3-bucket-name"
export MIGRATION_BUCKET_OBJECT_NAME="path/to/yourfile.csv.gz"
export MIGRATION_BUCKET_WORKER_QUEUE_LENGTH="100"
export MIGRATION_BUCKET_MESSAGE_LIMIT="0"  # Set to 0 for no limit
export MIGRATION_BUCKET_MODE="populator"

# Run the application
go run main.go
```

### Worker Mode Script

Create a file named `run_worker.sh`:

```bash
#!/bin/bash

# Set environment variables for Worker mode
export MIGRATION_BUCKET_AWS_REGION="us-west-2"
export MIGRATION_BUCKET_SQS_QUEUE_URL="https://sqs.us-west-2.amazonaws.com/123456789012/your-queue"
export MIGRATION_BUCKET_S3_BUCKET_NAME="your-s3-bucket-name"
export MIGRATION_BUCKET_GCS_BUCKET_NAME="your-gcs-bucket-name"
export MIGRATION_BUCKET_SQS_BATCH_SIZE="10"
export MIGRATION_BUCKET_WORKER_QUEUE_LENGTH="100"
export MIGRATION_BUCKET_MODE="worker"

# Run the application
go run main.go
```

**Make the Scripts Executable:**

```bash
chmod +x run_populator.sh
chmod +x run_worker.sh
```

## Context and Motivation

In scenarios where a massive amount of small files need to be migrated between cloud storage providers, traditional tools and methods can be inefficient and time-consuming. This was the case when we needed to transfer approximately **300 TB** of data consisting of nearly **1 billion files** from GCS to S3.

Due to the small size of each file (~200 KB), the overhead of transferring files individually became a significant bottleneck. To overcome this, we designed BsyncGcs to:

- **Parallelize Processing**: Utilize multiple workers running concurrently to handle file transfers.
- **Efficient Queue Management**: Use AWS SQS to distribute tasks effectively among workers.
- **Scalable Architecture**: Deploy the solution on an AWS EKS cluster, allowing for horizontal scaling based on workload.

By implementing BsyncGcs, we achieved a high-throughput migration process, significantly reducing the total time required for the data transfer.
