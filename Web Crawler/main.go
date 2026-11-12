// Build a web crawler that performs BFS or DFS starting from a seed URL, 
// discovering and fetching all reachable pages within the same domain.
// Implement concurrent fetching using multithreading 
// to improve crawling speed while maintaining thread safety for shared data 
// structures like the URL queue and visited set. For example, starting from 
// https://example.com, the crawler should visit https://example.com/about and 
// https://example.com/contact but skip https://other.com

// Input ----------------------------------------------
// start_url = "https://example.com"
// max_threads = 4

// Page structure:
// example.com → [example.com/about, example.com/blog]
// example.com/about → [example.com/contact]
// example.com/blog → [example.com/about, external.com]


// Output ----------------------------------
// Crawled URLs (in BFS order):
// 1. https://example.com
// 2. https://example.com/about
// 3. https://example.com/blog
// 4. https://example.com/contact

// Total pages crawled: 4
// (external.com skipped - different domain)

package main

import (
	"fmt"
	"net/url"
	"sync"
)

type queue struct {
	items []string
}

type fetchJob struct {
	index   int
	url     string
	results chan fetchResult
}

type fetchResult struct {
	index int
	links []string
}

var pages = map[string][]string{
	"https://example.com": {
		"https://example.com/home",
		"https://example.com/about",
		"https://example.com/contacts",
		"https://example.com/blog",
	},

	"https://example.com/home": {
		"https://example.com/home/india",
	},

	"https://example.com/home/india": {
		"https://example.com/home/india/delhi",
	},

	"https://example.com/about": {
		"https://example.com/about/canada",
	},

	"https://example.com/contacts": {
		"https://example.com/contacts/uk",
	},

	"https://example.com/blog": {
		"https://example.com/about",
		"https://external.com/about",
	},
}

// Mock fetch operation
func fetchURL(u string) []string {
	fmt.Println("Fetching:", u)
	return pages[u]
}

func sameDomain(link, domain string) bool {
	parsed, err := url.Parse(link)
	return err == nil && parsed.Host == domain
}

// BFS with bounded worker pool
func (q *queue) BFSWebCrawler(seedURL string, maxThreads int) []string {
	if maxThreads <= 0 {
		maxThreads = 1
	}

	parsed, _ := url.Parse(seedURL)
	domain := parsed.Host

	visited := make(map[string]bool)
	visited[seedURL] = true

	q.items = []string{seedURL}

	jobs := make(chan fetchJob)
	var workers sync.WaitGroup

	// Start fixed number of workers
	for i := 0; i < maxThreads; i++ {
		workers.Add(1)

		go func() {
			defer workers.Done()

			for job := range jobs {
				links := fetchURL(job.url)

				job.results <- fetchResult{
					index: job.index,
					links: links,
				}
			}
		}()
	}

	var crawled []string

	// Process one BFS level at a time
	for len(q.items) > 0 {
		currentLevel := q.items
		q.items = nil

		results := make(chan fetchResult, len(currentLevel))

		// Submit current level URLs
		for i, u := range currentLevel {
			crawled = append(crawled, u)

			jobs <- fetchJob{
				index:   i,
				url:     u,
				results: results,
			}
		}

		// Collect results in original BFS order
		levelLinks := make([][]string, len(currentLevel))

		for i := 0; i < len(currentLevel); i++ {
			result := <-results
			levelLinks[result.index] = result.links
		}

		// Add newly discovered URLs to next level
		for _, links := range levelLinks {
			for _, link := range links {
				if !sameDomain(link, domain) {
					continue
				}

				if !visited[link] {
					visited[link] = true
					q.items = append(q.items, link)
				}
			}
		}
	}

	close(jobs)
	workers.Wait()

	return crawled
}

// DFS using a stack
func (q *queue) DFSWebCrawler(seedURL string) []string {
	parsed, _ := url.Parse(seedURL)
	domain := parsed.Host

	visited := make(map[string]bool)
	visited[seedURL] = true

	q.items = []string{seedURL}

	var crawled []string

	for len(q.items) > 0 {
		// Pop last element (LIFO)
		n := len(q.items) - 1
		current := q.items[n]
		q.items = q.items[:n]

		crawled = append(crawled, current)

		links := fetchURL(current)

		// Reverse to preserve original link order
		for i := len(links) - 1; i >= 0; i-- {
			link := links[i]

			if !sameDomain(link, domain) {
				continue
			}

			if !visited[link] {
				visited[link] = true
				q.items = append(q.items, link)
			}
		}
	}

	return crawled
}

func main() {
	seedURL := "https://example.com"
	maxThreads := 4

	var q queue

	fmt.Println("BFS Crawler:")
	bfsResult := q.BFSWebCrawler(seedURL, maxThreads)

	for i, u := range bfsResult {
		fmt.Printf("%d. %s\n", i+1, u)
	}

	fmt.Println("Total pages crawled:", len(bfsResult))

	fmt.Println("\nDFS Crawler:")
	dfsResult := q.DFSWebCrawler(seedURL)

	for i, u := range dfsResult {
		fmt.Printf("%d. %s\n", i+1, u)
	}

	fmt.Println("Total pages crawled:", len(dfsResult))
}
