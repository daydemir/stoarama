package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

// object is one listed key. The snapshot is TSV: key, size, etag, mtime (unix).
type object struct {
	Key   string
	Size  int64
	ETag  string
	MTime int64
}

func runList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	out := fs.String("out", "-", "snapshot TSV path, - for stdout")
	workers := fs.Int("workers", 32, "parallel ListObjectsV2 requests")
	depth := fs.Int("split-depth", 5, "delimiter levels crawled before flat listing")
	var prefixes []string
	fs.Func("prefix", "list only keys under this prefix (repeatable; default the whole bucket)", func(v string) error {
		if v == "" {
			return fmt.Errorf("empty --prefix")
		}
		prefixes = append(prefixes, v)
		return nil
	})
	fs.Parse(args)
	if len(prefixes) == 0 {
		prefixes = []string{""}
	}
	for i, a := range prefixes {
		for j, b := range prefixes {
			if i != j && strings.HasPrefix(a, b) {
				return fmt.Errorf("--prefix %q overlaps %q", a, b)
			}
		}
	}
	st, err := storeFromEnv()
	if err != nil {
		return err
	}
	w := io.Writer(os.Stdout)
	if *out != "-" {
		f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	bw := bufio.NewWriterSize(w, 1<<20)
	n, bytes, err := listBucket(ctx, st, *workers, *depth, prefixes, bw)
	if err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "listed %d objects, %d bytes at %s\n", n, bytes, time.Now().UTC().Format(time.RFC3339))
	return nil
}

// listBucket crawls each root prefix with "/" as delimiter down to depth, then
// lists each deeper prefix flat. Every delimiter page returns both its objects
// and its sub-prefixes, so the union is every key under the roots.
func listBucket(ctx context.Context, st store, workers, depth int, roots []string, w io.Writer) (int64, int64, error) {
	var mu sync.Mutex
	var n, total int64
	emit := func(objs []object) error {
		mu.Lock()
		defer mu.Unlock()
		for _, o := range objs {
			if strings.ContainsAny(o.Key, "\t\n\r") {
				return fmt.Errorf("key %q contains a tab or newline", o.Key)
			}
			if _, err := fmt.Fprintf(w, "%s\t%d\t%s\t%d\n", o.Key, o.Size, o.ETag, o.MTime); err != nil {
				return err
			}
			n++
			total += o.Size
		}
		return nil
	}
	g, ctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, workers)
	var walk func(prefix string, level int)
	walk = func(prefix string, level int) {
		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()
			delim := level < depth
			subs, err := listPrefix(ctx, st, prefix, delim, emit)
			if err != nil {
				return err
			}
			for _, sub := range subs {
				walk(sub, level+1)
			}
			return nil
		})
	}
	for _, root := range roots {
		walk(root, 0)
	}
	return n, total, g.Wait()
}

func listPrefix(ctx context.Context, st store, prefix string, delim bool, emit func([]object) error) ([]string, error) {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(st.bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1000)}
	if delim {
		in.Delimiter = aws.String("/")
	}
	var subs []string
	for {
		out, err := st.s3.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("list %q: %w", prefix, err)
		}
		objs := make([]object, 0, len(out.Contents))
		for _, o := range out.Contents {
			objs = append(objs, object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: strings.Trim(aws.ToString(o.ETag), `"`), MTime: aws.ToTime(o.LastModified).Unix()})
		}
		if err := emit(objs); err != nil {
			return nil, err
		}
		for _, p := range out.CommonPrefixes {
			subs = append(subs, aws.ToString(p.Prefix))
		}
		if !aws.ToBool(out.IsTruncated) {
			return subs, nil
		}
		next := aws.ToString(out.NextContinuationToken)
		if next == "" || next == aws.ToString(in.ContinuationToken) {
			return nil, errors.New("truncated listing without an advancing continuation token")
		}
		in.ContinuationToken = aws.String(next)
	}
}
