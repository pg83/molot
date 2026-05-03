package main

import (
	"context"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fetchMain implements `molot fetch <uid> <out-path>`. Downloads the
// node's result.zstd from S3 to a local file. Used by wrap.sh.tmpl
// instead of `minio-client cp` so the wrap doesn't need an mc alias /
// MC_HOST env / per-task ~/.mc config dir.
func fetchMain(args []string) {
	if len(args) != 2 {
		ThrowFmt("usage: molot fetch <uid> <out-path>")
	}

	uid, outPath := args[0], args[1]
	cfg := loadS3Config()
	key := cfg.ResultObjectKey(uid)

	f := Throw2(os.Create(outPath))
	defer f.Close()

	resp := Throw2(cfg.S3Cli.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(key),
	}))

	defer resp.Body.Close()

	Throw2(io.Copy(f, resp.Body))
}

// pushMain implements `molot push <uid> <in-path>`. Uploads the node's
// result tarball from a local file to S3. Counterpart to fetchMain.
func pushMain(args []string) {
	if len(args) != 2 {
		ThrowFmt("usage: molot push <uid> <in-path>")
	}

	uid, inPath := args[0], args[1]
	cfg := loadS3Config()
	key := cfg.ResultObjectKey(uid)

	f := Throw2(os.Open(inPath))
	defer f.Close()

	Throw2(cfg.S3Cli.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(key),
		Body:   f,
	}))
}

// loadS3Config reads S3-only env vars, validates them, returns a
// Config with S3Cli ready. Skips the GORN_API check — fetch/push don't
// dispatch to gorn, just S3 I/O.
func loadS3Config() *Config {
	c := &Config{
		S3Bucket:  os.Getenv("S3_BUCKET"),
		S3Endpt:   os.Getenv("S3_ENDPOINT"),
		AWSKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecret: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWSRegion: os.Getenv("AWS_REGION"),
		S3Root:    os.Getenv("MOLOT_S3_ROOT"),
	}

	if c.AWSRegion == "" {
		c.AWSRegion = "us-east-1"
	}

	if c.S3Root == "" {
		c.S3Root = "molot"
	}

	if c.S3Bucket == "" {
		ThrowFmt("S3_BUCKET is required")
	}

	if c.S3Endpt == "" {
		ThrowFmt("S3_ENDPOINT is required")
	}

	if c.AWSKey == "" || c.AWSSecret == "" {
		ThrowFmt("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")
	}

	c.S3Cli = newS3Client(c)

	return c
}
