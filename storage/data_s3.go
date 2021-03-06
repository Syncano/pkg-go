package storage

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/Syncano/pkg-go/v2/util"
)

type s3Storage struct {
	uploader *s3manager.Uploader
	client   *s3.S3
	buckets  map[BucketKey]*bucketInfo
}

func newS3Storage(loc string, buckets map[BucketKey]*bucketInfo) DataStorage {
	accessKeyID := util.GetPrefixEnv(loc, "S3_ACCESS_KEY_ID")
	secretAccessKey := util.GetPrefixEnv(loc, "S3_SECRET_ACCESS_KEY")
	region := util.GetPrefixEnv(loc, "S3_REGION")
	endpoint := util.GetPrefixEnv(loc, "S3_ENDPOINT")

	sess := createS3Session(accessKeyID, secretAccessKey, region, endpoint)
	client := s3.New(sess)
	uploader := s3manager.NewUploaderWithClient(client)

	return &s3Storage{
		uploader: uploader,
		client:   client,
		buckets:  buckets,
	}
}

// Client returns s3 client.
func (s *s3Storage) Client() interface{} {
	return s.client
}

func (s *s3Storage) URL(bucket BucketKey, key string) string {
	return s.buckets[bucket].URL + key
}

func createS3Session(accessKeyID, secretAccessKey, region, endpoint string) *session.Session {
	conf := aws.Config{
		Region:   aws.String(region),
		Endpoint: aws.String(endpoint),
	}
	creds := credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	sess, err := session.NewSession(conf.WithCredentials(creds))
	util.Must(err)

	return sess
}

func (s *s3Storage) Upload(ctx context.Context, bucket BucketKey, key string, f io.Reader) error {
	_, err := s.uploader.UploadWithContext(ctx,
		&s3manager.UploadInput{
			Bucket: aws.String(s.buckets[bucket].Name),
			Key:    aws.String(key),
			ACL:    aws.String("public-read"),
			Body:   f,
		})

	return err
}

func (s *s3Storage) Delete(ctx context.Context, bucket BucketKey, key string) error {
	_, err := s.client.DeleteObjectWithContext(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(s.buckets[bucket].Name),
			Key:    aws.String(key),
		})

	return err
}
