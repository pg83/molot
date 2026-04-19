package main

import (
	"encoding/json"
	"flag"
	"os"
	"strings"
)

type Config struct {
	GornBin   string `json:"gorn_bin,omitempty"`
	GornAPI   string `json:"gorn_api,omitempty"`
	S3Bucket  string `json:"s3_bucket,omitempty"`
	S3Endpt   string `json:"s3_endpoint,omitempty"`
	AWSKey    string `json:"aws_access_key_id,omitempty"`
	AWSSecret string `json:"aws_secret_access_key,omitempty"`
	AWSRegion string `json:"aws_region,omitempty"`
	Dump      bool   `json:"dump,omitempty"`
	Quiet     bool   `json:"quiet,omitempty"`
	UID       string `json:"-"` // not meaningful in a config file; runtime-only
}

type cliOpts struct {
	cfgFile   string
	gornBin   string
	gornAPI   string
	s3Bucket  string
	s3Endpt   string
	awsKey    string
	awsSecret string
	awsRegion string
	dump      bool
	quiet     bool
	uid       string
}

func parseCLI(args []string) (*cliOpts, *flag.FlagSet) {
	fs := flag.NewFlagSet("molot", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	o := &cliOpts{}

	fs.StringVar(&o.cfgFile, "config", "", "path to JSON config file")
	fs.StringVar(&o.gornAPI, "api", "", "gorn control API URL (env GORN_API)")
	fs.StringVar(&o.s3Bucket, "bucket", "", "S3 bucket name (env S3_BUCKET)")
	fs.StringVar(&o.s3Endpt, "endpoint", "", "S3 endpoint URL (env S3_ENDPOINT)")
	fs.StringVar(&o.awsKey, "aws-key", "", "S3 access key (env AWS_ACCESS_KEY_ID)")
	fs.StringVar(&o.awsSecret, "aws-secret", "", "S3 secret key (env AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&o.awsRegion, "aws-region", "", "S3 region (env AWS_REGION; default us-east-1)")
	fs.StringVar(&o.gornBin, "gorn", "", "path to gorn binary (env MOLOT_GORN; default \"gorn\")")
	fs.BoolVar(&o.dump, "dump", false, "dump each generated wrap script to stderr (env MOLOT_DUMP)")
	fs.BoolVar(&o.quiet, "quiet", false, "suppress per-task stream, print only on failure (env MOLOT_QUIET)")
	fs.StringVar(&o.uid, "uid", "", "run only the node with this uid, skipping dep traversal (for debugging)")

	Throw(fs.Parse(args))

	if fs.NArg() > 0 {
		ThrowFmt("molot: unexpected positional args: %v", fs.Args())
	}

	return o, fs
}

// setIfExplicit returns b if flag `name` was explicitly set on fs, else a.
func setFromFlagStr(fs *flag.FlagSet, name string, a, b string) string {
	if flagWasSet(fs, name) {
		return b
	}

	return a
}

func setFromFlagBool(fs *flag.FlagSet, name string, a, b bool) bool {
	if flagWasSet(fs, name) {
		return b
	}

	return a
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	found := false

	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})

	return found
}

func loadConfig(args []string) *Config {
	o, fs := parseCLI(args)

	c := &Config{}

	// Layer 1: JSON config file.
	if o.cfgFile != "" {
		data := Throw2(os.ReadFile(o.cfgFile))
		Throw(json.Unmarshal(data, c))
	}

	// Layer 2: env vars (override file if set).
	overlayFromEnv(c)

	// Layer 3: explicit CLI flags (override env + file).
	c.GornAPI = setFromFlagStr(fs, "api", c.GornAPI, o.gornAPI)
	c.S3Bucket = setFromFlagStr(fs, "bucket", c.S3Bucket, o.s3Bucket)
	c.S3Endpt = setFromFlagStr(fs, "endpoint", c.S3Endpt, o.s3Endpt)
	c.AWSKey = setFromFlagStr(fs, "aws-key", c.AWSKey, o.awsKey)
	c.AWSSecret = setFromFlagStr(fs, "aws-secret", c.AWSSecret, o.awsSecret)
	c.AWSRegion = setFromFlagStr(fs, "aws-region", c.AWSRegion, o.awsRegion)
	c.GornBin = setFromFlagStr(fs, "gorn", c.GornBin, o.gornBin)
	c.Dump = setFromFlagBool(fs, "dump", c.Dump, o.dump)
	c.Quiet = setFromFlagBool(fs, "quiet", c.Quiet, o.quiet)
	c.UID = o.uid // CLI-only

	// Defaults.
	if c.AWSRegion == "" {
		c.AWSRegion = "us-east-1"
	}

	if c.GornBin == "" {
		c.GornBin = "gorn"
	}

	validate(c)

	return c
}

func overlayFromEnv(c *Config) {
	if v := os.Getenv("GORN_API"); v != "" {
		c.GornAPI = v
	}

	if v := os.Getenv("S3_BUCKET"); v != "" {
		c.S3Bucket = v
	}

	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		c.S3Endpt = v
	}

	if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
		c.AWSKey = v
	}

	if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
		c.AWSSecret = v
	}

	if v := os.Getenv("AWS_REGION"); v != "" {
		c.AWSRegion = v
	}

	if v := os.Getenv("MOLOT_GORN"); v != "" {
		c.GornBin = v
	}

	if os.Getenv("MOLOT_DUMP") != "" {
		c.Dump = true
	}

	if os.Getenv("MOLOT_QUIET") != "" {
		c.Quiet = true
	}
}

func validate(c *Config) {
	if c.GornAPI == "" {
		ThrowFmt("molot: GORN_API / --api is required")
	}

	if !strings.HasPrefix(c.GornAPI, "http://") && !strings.HasPrefix(c.GornAPI, "https://") {
		ThrowFmt("molot: GORN_API must start with http:// or https:// (got %q)", c.GornAPI)
	}

	if c.S3Bucket == "" {
		ThrowFmt("molot: S3_BUCKET / --bucket is required")
	}

	if c.S3Endpt == "" {
		ThrowFmt("molot: S3_ENDPOINT / --endpoint is required")
	}

	if c.AWSKey == "" || c.AWSSecret == "" {
		ThrowFmt("molot: AWS_ACCESS_KEY_ID / --aws-key and AWS_SECRET_ACCESS_KEY / --aws-secret are required")
	}
}
