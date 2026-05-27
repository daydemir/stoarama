package r2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Client struct {
	bucket    string
	s3        *s3.Client
	presigner *s3.PresignClient
}

type Config struct {
	AccountID string
	AccessKey string
	SecretKey string
	Region    string
	Bucket    string
	Endpoint  string
}

type ObjectHead struct {
	ETag      string
	SizeBytes int64
}

type ObjectInfo struct {
	Key          string
	ETag         string
	SizeBytes    int64
	LastModified time.Time
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Region == "" {
		cfg.Region = "auto"
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(cfg.Endpoint)
	})

	return &Client{
		bucket:    cfg.Bucket,
		s3:        client,
		presigner: s3.NewPresignClient(client),
	}, nil
}

func (c *Client) Bucket() string { return c.bucket }

func (c *Client) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	out, err := c.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign get %s: %w", key, err)
	}
	return out.URL, nil
}

func (c *Client) PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (string, error) {
	in := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	out, err := c.presigner.PresignPutObject(ctx, in, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign put %s: %w", key, err)
	}
	return out.URL, nil
}

func (c *Client) PutBytes(ctx context.Context, key, contentType string, body []byte) (string, error) {
	in := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	out, err := c.s3.PutObject(ctx, in)
	if err != nil {
		return "", fmt.Errorf("put object %s: %w", key, err)
	}
	return cleanETag(aws.ToString(out.ETag)), nil
}

func (c *Client) PutReader(ctx context.Context, key, contentType string, body io.Reader) (string, error) {
	in := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	out, err := c.s3.PutObject(ctx, in)
	if err != nil {
		return "", fmt.Errorf("put object %s: %w", key, err)
	}
	return cleanETag(aws.ToString(out.ETag)), nil
}

func (c *Client) Head(ctx context.Context, key string) (ObjectHead, error) {
	out, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return ObjectHead{}, fmt.Errorf("head object %s: %w", key, err)
	}
	return ObjectHead{ETag: cleanETag(aws.ToString(out.ETag)), SizeBytes: aws.ToInt64(out.ContentLength)}, nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", key, err)
	}
	return b, nil
}

func (c *Client) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("open object %s: %w", key, err)
	}
	return out.Body, nil
}

func (c *Client) ListPrefix(ctx context.Context, prefix string, limit int, fn func(ObjectInfo) error) error {
	var token *string
	seen := 0
	for {
		out, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return fmt.Errorf("list prefix %s: %w", prefix, err)
		}
		for _, obj := range out.Contents {
			info := ObjectInfo{
				Key:          aws.ToString(obj.Key),
				ETag:         cleanETag(aws.ToString(obj.ETag)),
				SizeBytes:    aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
			}
			if err := fn(info); err != nil {
				return err
			}
			seen++
			if limit > 0 && seen >= limit {
				return nil
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			return nil
		}
		token = out.NextContinuationToken
	}
}

func (c *Client) DeleteObjects(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	objects := make([]types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return fmt.Errorf("delete object key is empty")
		}
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(trimmed)})
	}
	out, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(c.bucket),
		Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return fmt.Errorf("delete objects: %w", err)
	}
	if len(out.Errors) > 0 {
		first := out.Errors[0]
		return fmt.Errorf("delete object %s: %s %s", aws.ToString(first.Key), aws.ToString(first.Code), aws.ToString(first.Message))
	}
	return nil
}

func cleanETag(v string) string {
	return strings.Trim(v, "\"")
}
