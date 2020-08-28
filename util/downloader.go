package util

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

// Downloader defines file downloader interface.
//go:generate go run github.com/vektra/mockery/cmd/mockery -name Downloader
type Downloader interface {
	Download(ctx context.Context, urls []string) <-chan *DownloadResult
}

// HTTPDownloader is used to download files asynchronously.
type HTTPDownloader struct {
	sem    chan bool
	client *http.Client
	cfg    DownloaderConfig
}

// DownloadResult is returned after download is finished.
type DownloadResult struct {
	URL   string
	Data  []byte
	Error error
}

func (res *DownloadResult) String() string {
	if res.Data != nil {
		return fmt.Sprintf("{URL:%s, DataLen:%d}", res.URL, len(res.Data))
	}

	return fmt.Sprintf("{URL:%s, Error:%s}", res.URL, res.Error)
}

// DownloaderConfig holds information about downloader settable options.
type DownloaderConfig struct {
	Concurrency uint
	Timeout     time.Duration
	RetryCount  int
	RetrySleep  time.Duration
}

var DefaultDownloaderConfig = DownloaderConfig{
	Concurrency: 2,
	Timeout:     15 * time.Second,
	RetryCount:  3,
	RetrySleep:  100 * time.Millisecond,
}

func WithConcurrency(conc uint) func(*DownloaderConfig) {
	return func(config *DownloaderConfig) {
		config.Concurrency = conc
	}
}

func WithTimeout(val time.Duration) func(*DownloaderConfig) {
	return func(config *DownloaderConfig) {
		config.Timeout = val
	}
}

func WithRetry(count int, sleep time.Duration) func(*DownloaderConfig) {
	return func(config *DownloaderConfig) {
		config.RetryCount = count
		config.RetrySleep = sleep
	}
}

// NewDownloader initializes a new downloader.
func NewDownloader(opts ...func(*DownloaderConfig)) *HTTPDownloader {
	cfg := DefaultDownloaderConfig

	for _, opt := range opts {
		opt(&cfg)
	}

	sem := make(chan bool, cfg.Concurrency)
	client := &http.Client{
		Timeout: cfg.Timeout,
	}

	return &HTTPDownloader{
		cfg:    cfg,
		sem:    sem,
		client: client,
	}
}

// Config returns a copy of downloader config struct.
func (d *HTTPDownloader) Config() DownloaderConfig {
	return d.cfg
}

func (d *HTTPDownloader) downloadURL(ctx context.Context, url string) ([]byte, error) {
	// Create request.
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)

	// Retry request if needed - stop when context error happens.
	var data []byte

	_, err = RetryWithCritical(d.cfg.RetryCount, d.cfg.RetrySleep, func() (bool, error) {
		var e error
		resp, e := d.client.Do(req)
		if e != nil {
			// If it was not a context error - it is not a critical one.
			return ctx.Err() != nil, e
		}

		if resp.StatusCode != 200 {
			return false, fmt.Errorf("error downloading URL: %s. Status: %d", url, resp.StatusCode)
		}

		data, e = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return ctx.Err() != nil, e
	})

	return data, err
}

// Download downloads all files in url list and sends them to returned channel.
func (d *HTTPDownloader) Download(ctx context.Context, urls []string) <-chan *DownloadResult {
	ch := make(chan *DownloadResult)

	go func() {
		var wg sync.WaitGroup

		for _, url := range urls {
			wg.Add(1)

			go func(url string) {
				d.sem <- true

				defer func() {
					<-d.sem
					wg.Done()
				}()

				data, err := d.downloadURL(ctx, url)
				select {
				case <-ctx.Done():
				case ch <- &DownloadResult{URL: url, Error: err, Data: data}:
				}
			}(url)
		}

		// Close channel after all urls are downloaded.
		wg.Wait()
		close(ch)
	}()

	return ch
}
