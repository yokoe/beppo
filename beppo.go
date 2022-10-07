package beppo

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// Client provides helper methods commonly used with GCS.
type Client struct {
	storageClient *storage.Client
}

// NewClient return an instance of Client.
func NewClient(storageClient *storage.Client) *Client {
	if storageClient == nil {
		log.Printf("Warning: Tried to instantiate Beppo.Client with empty storage client.")
		return nil
	}
	return &Client{storageClient: storageClient}
}

// Download copies a file from bucket to local.
func (c *Client) Download(bucket string, object string, dstFilepath string) error {
	f, err := os.Create(dstFilepath)
	if err != nil {
		return err
	}

	obj := c.storageClient.Bucket(bucket).Object(object)
	reader, err := obj.NewReader(context.Background())
	if err != nil {
		return err
	}
	defer reader.Close()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Client) UploadText(bucket string, object string, text string) error {
	wc := s.storageClient.Bucket(bucket).Object(object).NewWriter(context.Background())
	wc.ContentType = "text/plain"

	if _, err := wc.Write([]byte(text)); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Client) UploadFile(bucket string, srcFilepath string, dstObject string) error {
	wc := s.storageClient.Bucket(bucket).Object(dstObject).NewWriter(context.Background())

	f, err := os.Open(srcFilepath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(wc, f); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Client) ListObjectsWithPrefix(bucket, prefix string) ([]string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	files := []string{}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Bucket(%q).Objects(): %v", bucket, err)
		}
		files = append(files, attrs.Name)
	}
	return files, nil
}

func (s *Client) GenerateSignedUrl(bucket string, object string, expirationMinutes time.Duration) (string, error) {
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(expirationMinutes * time.Minute),
	}

	u, err := s.storageClient.Bucket(bucket).SignedURL(object, opts)
	if err != nil {
		return "", fmt.Errorf("Bucket(%q).SignedURL: %v", bucket, err)
	}
	return u, nil
}
