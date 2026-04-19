package main

import (
	"os"
	"strings"
)

type Config struct {
	GornBin   string
	GornAPI   string
	S3Bucket  string
	S3Endpt   string
	AWSKey    string
	AWSSecret string
	AWSRegion string
}

func loadConfig() *Config {
	c := &Config{
		GornBin:   getenvOr("MOLOT_GORN", "gorn"),
		GornAPI:   os.Getenv("GORN_API"),
		S3Bucket:  os.Getenv("S3_BUCKET"),
		S3Endpt:   os.Getenv("S3_ENDPOINT"),
		AWSKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecret: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWSRegion: getenvOr("AWS_REGION", "us-east-1"),
	}

	if c.GornAPI == "" {
		ThrowFmt("molot: GORN_API is required")
	}

	if !strings.HasPrefix(c.GornAPI, "http://") && !strings.HasPrefix(c.GornAPI, "https://") {
		ThrowFmt("molot: GORN_API must start with http:// or https:// (got %q)", c.GornAPI)
	}

	if c.S3Bucket == "" {
		ThrowFmt("molot: S3_BUCKET is required")
	}

	if c.S3Endpt == "" {
		ThrowFmt("molot: S3_ENDPOINT is required")
	}

	if c.AWSKey == "" || c.AWSSecret == "" {
		ThrowFmt("molot: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")
	}

	return c
}

func getenvOr(k, def string) string {
	v := os.Getenv(k)

	if v == "" {
		return def
	}

	return v
}
