// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"io"
	"path"
	"log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3ArchiveBackend struct {
	svc *s3.S3
	bucket string
	prefix string
	dryrun bool
}

func (b *S3ArchiveBackend) GetFile(pth string) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key: aws.String(path.Join(b.prefix, pth)),
	}
	resp, err := b.svc.GetObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (b *S3ArchiveBackend) PutFile(pth string, in io.ReadCloser) error {
	if b.dryrun {
		log.Printf("dryrun: put file %s", pth)
		in.Close()
		return nil
	}
	params := &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key: aws.String(path.Join(b.prefix, pth)),
		ACL: aws.String(s3.ObjectCannedACLPublicRead),
		Body: aws.ReadSeekCloser(in),
	}
	_, err := b.svc.PutObject(params)
	return err
}

func (b *S3ArchiveBackend) ListFiles(pth string) (chan string, chan error) {
	prefix := path.Join(b.prefix, pth)
	ch := make(chan string)
	errs := make(chan error)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(b.bucket),
		MaxKeys: aws.Int64(1000),
		Prefix: aws.String(prefix),
	}
	resp, err := b.svc.ListObjects(params)
	if err != nil {
		errs <- err
		close(ch)
		return ch, errs
	}
	go func() {
		for {
			for _, c := range resp.Contents {
				params.Marker = c.Key
				ch <- *c.Key
			}
			if *resp.IsTruncated {
				resp, err = b.svc.ListObjects(params)
				if err != nil {
					errs <- err
				}
			} else {
				break
			}
		}
		close(ch)
	}()
	return ch, errs
}

func MakeS3Backend(bucket string, prefix string, opts *ConnectOptions) ArchiveBackend {
	cfg := aws.Config{}
	if opts != nil && opts.S3Region != "" {
		cfg.Region = aws.String(opts.S3Region)
	}
	sess := session.New(&cfg)
	return &S3ArchiveBackend{
		svc: s3.New(sess),
		bucket: bucket,
		prefix: prefix,
		dryrun: opts.DryRun,
	}
}
