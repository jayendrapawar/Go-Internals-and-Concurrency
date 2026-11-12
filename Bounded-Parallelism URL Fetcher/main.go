// You are building a data ingestion service that needs to fetch data from a large list of URLs. 
// Firing off an unbounded number of goroutines could overwhelm downstream servers or exhaust local file descriptors.
// Implement a function ProcessURLs that processes a slice of input 
// strings (representing URLs) concurrently, limiting the maximum number of simultaneous 
// in-flight tasks to maxConcurrency.


package main

func main() {
	input := []string{
		"https://example.com",
		"https://example.org",
		"https://example.net",
		"https://example.edu",
	}

	ProcessURLs(input, 2)
}

func ProcessURLs(urls []string, maxConcurrency int) {
	
}