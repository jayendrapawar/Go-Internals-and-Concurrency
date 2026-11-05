package main

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"golang.org/x/sync/errgroup"
)

func FetchURLs(urls []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	client := &http.Client{}

	for _, url := range urls {
		url := url

		g.Go(func() error {
			req, err := http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				url,
				nil,
			)
			if err != nil {
				return err
			}

			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("%s: %w", url, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return fmt.Errorf("%s: HTTP status %d", url, resp.StatusCode)
			}

			_, err = io.Copy(io.Discard, resp.Body)
			if err != nil {
				return fmt.Errorf("%s: %w", url, err)
			}

			fmt.Println("Successfully fetched:", url)
			return nil
		})
	}

	return g.Wait()
}

func main() {
	urls := []string{
		"https://google.com",
		"https://github.com",
		"https://invalid-url-example-123.com",
		"https://golang.org",
	}

	err := FetchURLs(urls)
	if err != nil {
		fmt.Println("Fetch failed:", err)
		return
	}

	fmt.Println("All URLs fetched successfully")
}
