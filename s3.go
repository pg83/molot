package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func newS3Client(cfg *Config) *s3.Client {
	awsCfg := aws.Config{
		Region:      cfg.AWSRegion,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AWSKey, cfg.AWSSecret, ""),
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpt != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpt)
		}

		o.UsePathStyle = true
	})
}

func s3StatExists(ctx context.Context, cli *s3.Client, bucket, key string) bool {
	_, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err == nil {
		return true
	}

	if isNotFound(err) {
		return false
	}

	Throw(err)

	return false
}

// isNotFound covers both modeled NotFound (HEAD) and NoSuchKey (GET) —
// HeadObject in v2 returns *types.NotFound, GetObject returns
// *types.NoSuchKey, generic 404 is reported as smithy.GenericAPIError.
func isNotFound(err error) bool {
	var nf *types.NotFound

	if errors.As(err, &nf) {
		return true
	}

	var nsk *types.NoSuchKey

	if errors.As(err, &nsk) {
		return true
	}

	var ae smithy.APIError

	if errors.As(err, &ae) {
		code := ae.ErrorCode()

		return code == "NotFound" || code == "NoSuchKey"
	}

	return false
}

func s3GetJSON(ctx context.Context, cli *s3.Client, bucket, key string, out any) {
	resp := Throw2(cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}))

	defer resp.Body.Close()

	data := Throw2(io.ReadAll(resp.Body))
	Throw(json.Unmarshal(data, out))
}

func s3PutJSON(ctx context.Context, cli *s3.Client, bucket, key string, body any) {
	data := Throw2(json.MarshalIndent(body, "", "  "))

	Throw2(cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}))
}

type s3Entry struct {
	Key          string
	LastModified time.Time
}

// s3List returns objects under prefix, lex-sorted (S3 default). Pages
// through ContinuationToken for buckets with >1000 objects. Returns
// (Key, LastModified) so callers can use LM as a liveness signal —
// the UI uses it to distinguish live running markers from stuck ones.
func s3List(ctx context.Context, cli *s3.Client, bucket, prefix string) []s3Entry {
	var out []s3Entry
	var token *string

	for {
		resp := Throw2(cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		}))

		for _, obj := range resp.Contents {
			if obj.Key == nil || obj.LastModified == nil {
				continue
			}

			out = append(out, s3Entry{Key: *obj.Key, LastModified: *obj.LastModified})
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}

		token = resp.NextContinuationToken
	}

	return out
}
