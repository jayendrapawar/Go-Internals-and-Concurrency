
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
)

// Follow Up Questions
// 1. With Timeout – Add 3-second timeout per HTTP request.
// 2. With Retry – Retry failed requests up to 3 times.
// 3. With Parent Cancellation – Cancel all requests when parent context is cancelled.
// 4. With Concurrency Limit – Fetch 100 URLs with a maximum of 10 concurrent requests.

const (
	maxConcurrency = 10
	requestTimeout = 3 * time.Second
	maxRetries     = 3
)

func FetchURLs(parentCtx context.Context, urls []string) error {
	g, ctx := errgroup.WithContext(parentCtx)

	// Maximum 10 goroutines executing concurrently.
	g.SetLimit(maxConcurrency)

	client := &http.Client{}

	for _, url := range urls {
		url := url

		// Don't schedule additional work after cancellation.
		if ctx.Err() != nil {
			break
		}

		g.Go(func() error {
			err := fetchWithRetry(ctx, client, url)

			if err != nil {
				return fmt.Errorf("failed to fetch %s: %w", url, err)
			}

			fmt.Println("Successfully fetched:", url)
			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return err
	}

	// Propagate parent cancellation, including an empty URL list.
	return parentCtx.Err()
}

func fetchWithRetry(
	ctx context.Context,
	client *http.Client,
	url string,
) error {

	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {

		// Stop immediately if parent/group context is cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := fetchOnce(ctx, client, url)

		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if the entire operation was cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt == maxRetries {
			break
		}

		fmt.Printf(
			"Retrying %s (attempt %d/%d): %v\n",
			url,
			attempt+1,
			maxRetries,
			err,
		)

		// Exponential backoff: 100ms, 200ms, 400ms.
		delay := 100 * time.Millisecond * time.Duration(1<<attempt)

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()

		case <-timer.C:
		}
	}

	return fmt.Errorf("exhausted %d retries: %w", maxRetries, lastErr)
}

func fetchOnce(
	ctx context.Context,
	client *http.Client,
	url string,
) error {

	// Each HTTP attempt gets its own 3-second timeout.
	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		reqCtx,
		http.MethodGet,
		url,
		nil,
	)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP status: %d", resp.StatusCode)
	}

	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	urls := []string{
		"https://google.com",
		"https://github.com",
		"https://golang.org",
		"https://stackoverflow.com",
		"https://example.com",
		"https://httpbin.org/get",
		"https://jsonplaceholder.typicode.com/posts",
		"https://go.dev",
		"https://wikipedia.org",
		"https://cloudflare.com",
		"https://redis.io",
		"https://kubernetes.io",
	}

	// Parent context controls the entire operation.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := FetchURLs(ctx, urls)

	if err != nil {
		fmt.Println("Fetch operation failed:", err)
		return
	}

	fmt.Println("All URLs fetched successfully")
}
