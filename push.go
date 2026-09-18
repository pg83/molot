package main

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// pushMain implements `molot push <uid> <in-path>`. Uploads the node's
// result tarball from a local file to S3.
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
