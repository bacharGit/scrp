package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/html"
)

const (
	MainPage              string        = "https://www.akbw.de/kammer/datenbanken/architektenliste/suchergebnisse-architektenliste"
	PagesCount            int           = 218
	Unique                string        = "/detail/eintrag/"
	BaseURL               string        = "https://www.akbw.de"
	MaxRetries            int           = 3
	RetryDelay            time.Duration = 2 * time.Second
	MaxConcurrentRequests int           = 5
	RequestDelay          time.Duration = 1 * time.Second
	BatchSize             int           = 50                      // Process 50 pages before delay
	BatchDelay            time.Duration = 1500 * time.Millisecond // 1.5 seconds delay
)

type ScrapedData struct {
	Name         string
	PrivateEmail string
	WorkEmail    string
	URL          string
}

type FailedURL struct {
	URL     string
	Retries int
	Error   error
}

func main() {
	start := time.Now()
	fmt.Println("Starting the scraping process...")

	// Channel for collecting scraped data - increased buffer size
	dataChan := make(chan ScrapedData, 5000)
	failedChan := make(chan FailedURL, 1000)

	// Rate limiting semaphore
	semaphore := make(chan struct{}, MaxConcurrentRequests)

	// Collect all scraped data
	var allData []ScrapedData
	var failedURLs []FailedURL
	var dataMutex sync.Mutex
	var failedMutex sync.Mutex

	// Progress tracking
	var processedPages int32
	var totalDetailPages int32
	var processedDetailPages int32

	// Start data collectors
	go func() {
		for data := range dataChan {
			dataMutex.Lock()
			allData = append(allData, data)
			atomic.AddInt32(&processedDetailPages, 1)
			dataMutex.Unlock()
		}
	}()

	go func() {
		for failed := range failedChan {
			failedMutex.Lock()
			failedURLs = append(failedURLs, failed)
			atomic.AddInt32(&processedDetailPages, 1)
			failedMutex.Unlock()
		}
	}()

	// Progress reporter goroutine
	progressDone := make(chan bool)
	startTime := time.Now()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pages := atomic.LoadInt32(&processedPages)
				detailPages := atomic.LoadInt32(&processedDetailPages)
				totalDetail := atomic.LoadInt32(&totalDetailPages)

				pageProgress := float64(pages) / float64(PagesCount) * 100
				var detailProgress float64
				if totalDetail > 0 {
					detailProgress = float64(detailPages) / float64(totalDetail) * 100
				}

				// Calculate time estimates
				elapsed := time.Since(startTime)
				var pageETA, detailETA string

				if pages > 0 {
					avgTimePerPage := elapsed / time.Duration(pages)
					remainingPages := PagesCount - int(pages)
					pageETADuration := time.Duration(remainingPages) * avgTimePerPage
					pageETA = formatDuration(pageETADuration)
				} else {
					pageETA = "calculating..."
				}

				if detailPages > 0 && totalDetail > 0 {
					avgTimePerDetail := elapsed / time.Duration(detailPages)
					remainingDetails := int(totalDetail) - int(detailPages)
					detailETADuration := time.Duration(remainingDetails) * avgTimePerDetail
					detailETA = formatDuration(detailETADuration)
				} else {
					detailETA = "calculating..."
				}

				fmt.Printf("\n[PROGRESS] Pages: %d/%d (%.1f%%) ETA: %s | Detail pages: %d/%d (%.1f%%) ETA: %s | Elapsed: %s\n",
					pages, PagesCount, pageProgress, pageETA, detailPages, totalDetail, detailProgress, detailETA, formatDuration(elapsed))
			case <-progressDone:
				return
			}
		}
	}()

	// Process pages in batches with delays
	for batchStart := 1; batchStart <= PagesCount; batchStart += BatchSize {
		batchEnd := batchStart + BatchSize - 1
		if batchEnd > PagesCount {
			batchEnd = PagesCount
		}

		fmt.Printf("Processing batch: pages %d-%d\n", batchStart, batchEnd)

		var batchWg sync.WaitGroup

		// Process pages in current batch
		for page := batchStart; page <= batchEnd; page++ {
			batchWg.Add(1)

			go func(pageNum int) {
				defer batchWg.Done()

				fmt.Printf("Processing page %d/%d\n", pageNum, PagesCount)
				urls, err := fetchDetailLinks(pageNum)
				if err != nil {
					fmt.Printf("Error fetching page %d: %v\n", pageNum, err)
					atomic.AddInt32(&processedPages, 1)
					return
				}

				// Add to total detail pages count
				atomic.AddInt32(&totalDetailPages, int32(len(urls)))

				var pwg sync.WaitGroup

				for _, url := range urls {
					pwg.Add(1)

					go func(pageURL string) {
						defer pwg.Done()

						// Rate limiting
						semaphore <- struct{}{}
						defer func() { <-semaphore }()

						time.Sleep(RequestDelay) // Add delay between requests
						processDetailPageWithRetry(dataChan, failedChan, pageURL, 0)
					}(url)
				}

				pwg.Wait()
				atomic.AddInt32(&processedPages, 1)
			}(page)
		}

		// Wait for current batch to complete before adding delay
		batchWg.Wait()

		// Add delay after every batch (except the last one)
		if batchEnd < PagesCount {
			fmt.Printf("Batch %d-%d completed. Waiting %v before next batch...\n",
				batchStart, batchEnd, BatchDelay)
			time.Sleep(BatchDelay)
		}
	}

	// Close channels after all processing is done
	close(dataChan)
	close(failedChan)

	// Stop progress reporter
	close(progressDone)

	// Give time for collectors to finish
	time.Sleep(1 * time.Second)

	// Save failed URLs to file for later processing
	failedMutex.Lock()
	failedURLsCopy := make([]FailedURL, len(failedURLs))
	copy(failedURLsCopy, failedURLs)
	failedMutex.Unlock()

	saveFailedURLs(failedURLsCopy, "failed_urls.txt")

	// Retry failed URLs with more conservative approach
	fmt.Printf("\nRetrying %d failed URLs with longer delays...\n", len(failedURLsCopy))
	dataMutex.Lock()
	retryFailedURLs(&allData, failedURLsCopy)

	// Clean and write data
	cleanedData := cleanData(allData)
	dataMutex.Unlock()

	err := writeCleanedData(cleanedData, "emails_test.txt")
	if err != nil {
		fmt.Printf("Error writing cleaned data: %v\n", err)
		return
	}

	dataMutex.Lock()
	totalRecords := len(allData)
	dataMutex.Unlock()

	fmt.Printf("\nScraping completed in %v\n", time.Since(start))
	fmt.Printf("Total records: %d\n", totalRecords)
	fmt.Printf("Clean records (with emails): %d\n", len(cleanedData))
	fmt.Printf("Failed URLs: %d\n", len(failedURLsCopy))

	if len(failedURLsCopy) > 0 {
		fmt.Printf("Failed URLs saved to 'failed_urls.txt' for manual retry\n")
	}
}

func processDetailPageWithRetry(dataChan chan<- ScrapedData, failedChan chan<- FailedURL, url string, retryCount int) {
	data, err := processDetailPage(url)
	if err != nil {
		if retryCount < MaxRetries {
			// Exponential backoff
			delay := time.Duration(retryCount+1) * RetryDelay
			fmt.Printf("Retry %d/%d for %s after %v: %v\n", retryCount+1, MaxRetries, url, delay, err)
			time.Sleep(delay)
			processDetailPageWithRetry(dataChan, failedChan, url, retryCount+1)
			return
		}

		fmt.Printf("Failed after %d retries %s: %v\n", MaxRetries, url, err)
		failedChan <- FailedURL{URL: url, Retries: retryCount, Error: err}
		return
	}

	dataChan <- data
}

func retryFailedURLs(allData *[]ScrapedData, failedURLs []FailedURL) {
	if len(failedURLs) == 0 {
		return
	}

	// More conservative retry approach
	semaphore := make(chan struct{}, 2) // Only 2 concurrent requests for retries
	var wg sync.WaitGroup
	dataChan := make(chan ScrapedData, len(failedURLs))

	for _, failed := range failedURLs {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			// Rate limiting for retries
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Longer delay for retries
			time.Sleep(3 * time.Second)

			data, err := processDetailPageWithTimeout(url, 30*time.Second)
			if err != nil {
				fmt.Printf("Final retry failed for %s: %v\n", url, err)
				return
			}
			fmt.Printf("Retry successful for %s\n", url)
			dataChan <- data
		}(failed.URL)
	}

	go func() {
		wg.Wait()
		close(dataChan)
	}()

	for data := range dataChan {
		*allData = append(*allData, data)
	}
}

func cleanData(data []ScrapedData) []ScrapedData {
	seen := make(map[string]bool)
	var cleaned []ScrapedData

	for _, item := range data {
		// Skip if no emails at all
		if item.PrivateEmail == "" && item.WorkEmail == "" {
			continue
		}

		// Create a unique key for deduplication
		key := fmt.Sprintf("%s|%s|%s", item.Name, item.PrivateEmail, item.WorkEmail)

		if !seen[key] {
			seen[key] = true
			cleaned = append(cleaned, item)
		}
	}

	return cleaned
}

func writeCleanedData(data []ScrapedData, filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, item := range data {
		output := fmt.Sprintf("name: %s", strings.ReplaceAll(item.Name, "  ", " "))
		if item.PrivateEmail != "" {
			output += fmt.Sprintf(", private_email: %s", item.PrivateEmail)
		}
		if item.WorkEmail != "" {
			output += fmt.Sprintf(", work_email: %s", item.WorkEmail)
		}
		output += "\n"

		_, err := io.WriteString(file, output)
		if err != nil {
			return err
		}
	}

	return nil
}

// fetchDetailLinks fetches the detail page links from a given page number
func fetchDetailLinks(page int) ([]string, error) {
	filePath := path.Join("html", fmt.Sprintf("/page_%d.html", page))
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	var links []string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" && strings.Contains(attr.Val, Unique) {
					absoluteURL := attr.Val
					if strings.HasPrefix(attr.Val, "/") {
						absoluteURL = "https://www.akbw.de" + attr.Val
					}
					links = append(links, absoluteURL)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	return links, nil
}

// processDetailPage fetches and processes a detail page, returning the extracted information
func processDetailPage(url string) (ScrapedData, error) {
	return processDetailPageWithTimeout(url, 15*time.Second)
}

// processDetailPageWithTimeout fetches and processes a detail page with custom timeout
func processDetailPageWithTimeout(url string, timeout time.Duration) (ScrapedData, error) {
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 2,
		},
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ScrapedData{}, err
	}

	// Add headers to appear more like a regular browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	res, err := client.Do(req)
	if err != nil {
		return ScrapedData{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return ScrapedData{}, fmt.Errorf("HTTP %d: %s", res.StatusCode, res.Status)
	}

	doc, err := html.Parse(res.Body)
	if err != nil {
		return ScrapedData{}, err
	}

	name := extractName(doc)
	privateEmail, workEmail := extractEmails(doc)

	return ScrapedData{
		Name:         name,
		PrivateEmail: privateEmail,
		WorkEmail:    workEmail,
		URL:          url,
	}, nil
}

// extractName searches for the <div class="detailText"> and extracts the text from its <h2> child
func extractName(n *html.Node) string {
	var name string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "div" {
			for _, attr := range n.Attr {
				if attr.Key == "class" && attr.Val == "detailText" {
					for c := n.FirstChild; c != nil; c = c.NextSibling {
						if c.Type == html.ElementNode && c.Data == "h2" {
							name = extractText(c)
							return
						}
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(n)
	return name
}

// extractEmails searches for <h4> elements with specific text and extracts the email from the subsequent <span><a href="mailto:...">
func extractEmails(n *html.Node) (string, string) {
	var privateEmail string
	var workEmail string

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "h4" {
			headerText := strings.ToLower(extractText(n))
			if headerText == "privatadresse" || headerText == "büroadresse" {
				// Find the next sibling <span>
				for s := n.NextSibling; s != nil; s = s.NextSibling {
					if s.Type == html.ElementNode && s.Data == "span" {
						for a := s.FirstChild; a != nil; a = a.NextSibling {
							if a.Type == html.ElementNode && a.Data == "a" {
								for _, attr := range a.Attr {
									if attr.Key == "href" && strings.HasPrefix(attr.Val, "mailto:") {
										email := strings.TrimPrefix(attr.Val, "mailto:")
										email = strings.TrimSpace(email)
										if strings.EqualFold(email, "info@akbw.de") || strings.EqualFold(email, "info@exyte.net") {
											continue
										}
										if headerText == "privatadresse" {
											privateEmail = email
										} else if headerText == "büroadresse" {
											workEmail = email
										}
									}
								}
							}
						}
						break
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(n)
	return privateEmail, workEmail
}

// extractText retrieves the concatenated text content of a node
func extractText(n *html.Node) string {
	var text string
	var extract func(*html.Node)
	extract = func(n *html.Node) {
		if n.Type == html.TextNode {
			text += n.Data
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extract(c)
		}
	}
	extract(n)
	return strings.TrimSpace(text)
}

// Additional utility functions

// formatDuration formats a duration into a readable string
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "0s"
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	} else {
		return fmt.Sprintf("%ds", seconds)
	}
}

// saveFailedURLs saves failed URLs to a file for later manual processing
func saveFailedURLs(failedURLs []FailedURL, filename string) error {
	if len(failedURLs) == 0 {
		return nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, failed := range failedURLs {
		line := fmt.Sprintf("%s | Retries: %d | Error: %v\n", failed.URL, failed.Retries, failed.Error)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	return nil
}
