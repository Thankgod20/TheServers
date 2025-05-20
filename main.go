package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/tebeka/selenium"
	"github.com/tebeka/selenium/chrome"
)

const (
	chromeDriverPath = "./chromedriver" // Update this with your ChromeDriver path
	//port             = 8080             // Port for Selenium WebDriver
	twitterSearchURL = "https://x.com/search?q="
)

// Result structure to return from the goroutine
type Result struct {
	Index    int    // Index of the goroutine
	Success  bool   // Whether the login was successful
	ErrorMsg string // Error message if the login failed
}
type Address struct {
	Address string
	Name    string
}

var usedIndex int

func generateUniqueFingerprint(proxyExtension string, userAgents string, instanceID int) selenium.Capabilities {

	randomUserAgent := userAgents
	fmt.Println("Extensions", proxyExtension)
	// Set up Chrome options with the random user agent
	caps := selenium.Capabilities{"browserName": "chrome"}

	caps["goog:chromeOptions"] = map[string]interface{}{
		"args": []string{"--headless", "--no-sandbox", "--disable-dev-shm-usage"},
	}
	//userDataDir := fmt.Sprintf("/data/data/com.termux/files/home/chrome_profile_%d", instanceID)

	chromeCaps := chrome.Capabilities{
		Prefs: map[string]interface{}{},
		Args: []string{
			fmt.Sprintf("--user-agent=%s", randomUserAgent),

			"--headless",              // Run in headless mode.
			"--disable-gpu",           // Disable GPU acceleration.
			"--no-sandbox",            // Necessary in many containerized/limited environments.
			"--disable-dev-shm-usage", // Avoid issues with limited /dev/shm space.
			"--incognito",             // Use incognito mode to reduce profile overhead.
			"--disable-background-networking",
			"--disable-sync",
			"--disable-translate",
			"--no-first-run",
			//fmt.Sprintf("--user-data-dir=%s", userDataDir),
		},
		Extensions: []string{proxyExtension},
	}
	caps.AddChrome(chromeCaps)

	return caps
}
func scrollToBottom(wd selenium.WebDriver) error {
	_, err := wd.ExecuteScript("window.scrollTo(0, document.body.scrollHeight);", nil)
	return err
}
func hashTweet(s string) string {
	x := sha256.New()
	x.Write([]byte(s))
	return hex.EncodeToString(x.Sum(nil))
}
func waitForPageLoad(driver selenium.WebDriver) {
	for {
		// Check if the page has loaded its initial state
		state, err := driver.ExecuteScript("return document.readyState", nil)
		if err != nil {
			log.Printf("Error checking document.readyState: %v", err)
		}

		// Break if the initial page load is complete
		if state == "complete" {
			// Add logic to detect Twitter's dynamic content
			contentLoaded, err := driver.ExecuteScript(`
				return document.querySelector('[role="progressbar"]') === null &&
					   document.querySelectorAll('article').length > 0;
			`, nil)
			if err != nil {
				log.Printf("Error checking dynamic content load: %v", err)
			}

			// If no progress bar is present and articles are loaded, consider the page loaded
			if contentLoaded == true {
				break
			}
		}

		time.Sleep(500 * time.Millisecond) // Poll every 500ms
	}
}
func getRandomPort() (int, error) {
	rand.Seed(time.Now().UnixNano())
	for {
		port := rand.Intn(64511) + 1024 // Ports range: 1024 to 65535
		if isPortAvailable(port) {
			return port, nil
		}
	}
}

func isPortAvailable(port int) bool {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	defer listener.Close()
	return true
}

type SessionStatus struct {
	ID      int    `json:"id"`
	Port    int    `json:"port"`
	Status  string `json:"status"`
	Updated string `json:"updated"`
}

func updateSessionStatus(id int, port int, status string) error {
	filePath := "./datacenter/activesession.json"

	// Ensure datacenter directory exists
	if err := os.MkdirAll("./datacenter", os.ModePerm); err != nil {
		return err
	}

	var sessions []SessionStatus
	// Read existing sessions if the file exists
	if data, err := ioutil.ReadFile(filePath); err == nil {
		if err := json.Unmarshal(data, &sessions); err != nil {
			log.Printf("Warning: could not unmarshal existing sessions: %v", err)
		}
	}
	if status == "New" {
		sessions = []SessionStatus{}
	}
	// Check if a session with the same ID exists
	found := false
	now := time.Now().Format(time.RFC3339)
	for i, s := range sessions {
		if s.ID == id {
			sessions[i].Port = port
			sessions[i].Status = status
			sessions[i].Updated = now
			found = true
			break
		}
	}
	if !found {
		newSession := SessionStatus{
			ID:      id,
			Port:    port,
			Status:  status,
			Updated: now,
		}
		sessions = append(sessions, newSession)
	}

	// Marshal back to JSON and write to file
	out, err := json.MarshalIndent(sessions, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filePath, out, 0644)
}
func scrapeTwitterSearch(query_ chan []AddressEntry, index int, totalNode int, caps selenium.Capabilities, cookies_ string, resultChan chan Result) {
	//defer wg.Done() // Mark this goroutine as done when it finishes
	port, err := getRandomPort()
	if err != nil {
		log.Fatalf("Failed to get a random port: %v", err)
	}
	if err := updateSessionStatus(index, port, "inactive"); err != nil {
		log.Printf("Failed to update session status: %v", err)
	}

	fmt.Printf("Using random port: %d\n", port)
	// Start a WebDriver instance
	opts := []selenium.ServiceOption{}
	service, err := selenium.NewChromeDriverService(chromeDriverPath, port, opts...)
	if err != nil {
		//log.Printf("Error starting ChromeDriver service: %v", err)
		log.Printf("Error starting ChromeDriver service: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error starting ChromeDriver service: %v", err)}
		return
	}
	defer service.Stop()

	// Connect to the WebDriver
	wd, err := selenium.NewRemote(caps, fmt.Sprintf("http://localhost:%d/wd/hub", port))
	if err != nil {
		//log.Printf("Error connecting to WebDriver: %v", err)
		log.Printf("Error connecting to WebDriver: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error connecting to WebDriver: %v", err)}
		return
	}
	defer wd.Quit()
	// Navigate to the Twitter homepage first
	fmt.Println("Navigating to Twitter homepage.")
	err = wd.Get("https://x.com")
	if err != nil {
		log.Printf("Failed to load Twitter homepage: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Failed to load Twitter homepage: %v", err)}
		return
	}
	time.Sleep(10 * time.Second)

	fmt.Println("Navigated to Twitter homepage.")
	// import the cookie json
	file, err := os.Open("./session_token.json")
	if err != nil {
		fmt.Println("Error opening cookie file:", err)
		return
	}
	defer file.Close()

	var cookies []selenium.Cookie
	json.NewDecoder(file).Decode(&cookies)

	cookie := &selenium.Cookie{
		Name:   "auth_token", // The cookie name
		Value:  cookies_,
		Path:   "/",      // Cookie path
		Domain: ".x.com", // Twitter domain
		Expiry: uint(time.Now().Add(24 * time.Hour).Unix()),
	}

	//for _, cookie := range cookies {

	if err := wd.AddCookie(cookie); err != nil {
		log.Printf("Error adding cookie: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error adding cookie: %v", err)}
		return
	}
	//}
	fmt.Println("Cookie added.")
	time.Sleep(5 * time.Second)
	// Open the Twitter search page

	// Refresh the page to apply the cookie
	if err := wd.Refresh(); err != nil {
		log.Printf("Error refreshing page: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error refreshing page: %v", err)}
		defer wd.Quit()
	}
	fmt.Println("Page refreshed with authenticated session.")

	time.Sleep(5 * time.Second)
	currentURL, err := wd.CurrentURL()
	if err != nil {
		log.Printf("Error retrieving current URL: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error retrieving current URL: %v", err)}
		return
	}
	_, err = wd.FindElement(selenium.ByCSSSelector, "[data-testid='AppTabBar_Home_Link']")
	if err != nil {

		if currentURL == "https://twitter.com/?mx=2" || currentURL == "https://x.com/?mx=2" || strings.Contains(currentURL, "https://x.com/i/flow/login") {
			fmt.Println("Login unsuccessful: Redirected back to the login page.")
			if err := updateSessionStatus(index, port, "inactive"); err != nil {
				log.Printf("Failed to update session status: %v", err)
			}
			fmt.Println("Login unsuccessful: Could not find logged-in element.", cookies_)
			resultChan <- Result{Index: index, Success: false, ErrorMsg: "Redirected back to login page."}
			return

		}
	} else {
		fmt.Println("Login successful: User is authenticated.")
	}
	fmt.Println("Browser index", index)
	var query []AddressEntry
	searchURL := "" //twitterSearchURL + query[index]
	if err := updateSessionStatus(index, port, "active"); err != nil {
		log.Printf("Failed to update session status: %v", err)
	}
	forMe := false
	var q_index int
	//outer:
	for {
		for qu := range query_ { // Listen for messages from the broadcast channel
			query = qu
			fmt.Println("Searched Quert", query, "index", index, " SS", len(query))
			for i, qux := range qu {
				if qux.Index == int64(index) {
					forMe = true
					q_index = i
					break
				}
			}
			if forMe {
				break
			}
		}
		/*if len(query) > index {
			fmt.Println("Breaking")
			break
		}*/
		if forMe {
			break
		}
	}
	//4x77NhFuVzWWDGEMUyB17e3nhvVdkV7HT2AZNmz6pump OR "Sorter Labs" since:2025-01-13
	now := time.Now()
	// Subtract 24 hours to get yesterday's date
	yesterday := now.Add(-24 * time.Hour)
	yesterdate := yesterday.Format("2006-01-02")
	searchURL = fmt.Sprintf(`%s%s  OR "$%s" since:%s`, twitterSearchURL, query[q_index].Address, query[q_index].Symbol, yesterdate) //twitterSearchURL + query[index].Address + " OR " + query[index].Name + " since:" + yesterdate
	err = wd.Get(searchURL)
	if err != nil {
		log.Printf("Failed to load Twitter search page: %v", err)
	}
	fmt.Println("Searched Query Completed", query[q_index], "Of Brower:-", index)
	time.Sleep(5 * time.Second)
	currentURL, _ = wd.CurrentURL()
	// current url
	if strings.Contains(currentURL, "https://x.com/i/flow/login") {
		fmt.Println("Login unsuccessful: Redirected back to the login page.")
		if err := updateSessionStatus(index, port, "inactive"); err != nil {
			log.Printf("Failed to update session status: %v", err)
		}
		fmt.Println("Login unsuccessful: Could not find logged-in element.", cookies_)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: "Redirected back to login page."}
		return

	}
	time.Sleep(5 * time.Second)
	n_index := q_index
	if err := updateSessionStatus(index, port, "active"); err != nil {
		log.Printf("Failed to update session status: %v", err)
	}

	for i := 0; i < 1000; i++ {
		fmt.Println()
		timeCount := 2 * (i + 1)
		currentTime := time.Now().UTC()
		waitForPageLoad(wd)
		if err := updateSessionStatus(index, port, "Running "+query[n_index].Address); err != nil {
			log.Printf("Failed to update session status: %v", err)
		}
		scrapeAndSaveTweet(query[n_index].Address, wd, int64(timeCount), currentTime, int64(index))
		time.Sleep(2 * time.Minute)
		if err := wd.Refresh(); err != nil {
			log.Printf("Error refreshing page: %v", err)
		}
		time.Sleep(1 * time.Second)
		fmt.Println("Page refreshed for Search Query.")
		//query = <-query_
		select {
		case query_ := <-query_:
			// Successfully received a value from the channel
			qn_index := -1
			for i, qux := range query_ {
				if qux.Index == int64(index) {

					qn_index = i
				}
			}
			if len(query_) > totalNode && qn_index > -1 {

				fmt.Println("== Adjusting to New Query ===Index", qn_index, "Brower Index:-", index)
				n_index = qn_index                         //(len(query_) - totalNode) + index
				if query_[n_index].Index == int64(index) { //if len(query_) > n_index {
					if err := updateSessionStatus(index, port, "Running Updated "+query_[n_index].Address); err != nil {
						log.Printf("Failed to update session status: %v", err)
					}
					i = 0
					searchURL := fmt.Sprintf(`%s%s OR  "$%s" since:%s`, twitterSearchURL, query_[n_index].Address, query_[n_index].Symbol, yesterdate) // twitterSearchURL + query[index].Address + " OR " + query[index].Name + " since:" + yesterdate
					err = wd.Get(searchURL)
					if err != nil {
						log.Printf("Failed to load Twitter search page: %v", err)
					}
					fmt.Println("Searched New Updated Query Completed", query_[n_index].Address, "Of Brower:-", index)
					query = query_
				}
			}
		default:
			// No value available, avoid waiting
			qn_index := -1
			for x, qux_ := range query {
				if qux_.Index == int64(index) {

					qn_index = x

				}
			}
			if len(query) > totalNode && qn_index > -1 {

				fmt.Println("== Adjusting to Forgotten Query === Index", qn_index, "Brower Index:-", index)
				n_index = qn_index //(len(query) - totalNode) + index
				i = 0
				if query[n_index].Index == int64(index) {
					searchURL := fmt.Sprintf(`%s%s OR  "$%s" since:%s`, twitterSearchURL, query[n_index].Address, query[n_index].Symbol, yesterdate) //twitterSearchURL + query[index].Address + " OR " + query[index].Name + " since:" + yesterdate
					err = wd.Get(searchURL)
					if err != nil {
						log.Printf("Failed to load Twitter search page: %v", err)
					}
					fmt.Println("Searched Forgotten Query Completed", query[n_index].Address, "Of Brower:-", index)
				}
			}
			fmt.Println("No value in the channel")
		}

	}
	if err := updateSessionStatus(index, port, "Inactive"); err != nil {
		log.Printf("Failed to update session status: %v", err)
	}
	resultChan <- Result{Index: index, Success: true, ErrorMsg: ""}

}

// Utility function to check for duplicates
func contains(slice []selenium.WebElement, element selenium.WebElement) bool {
	for _, item := range slice {
		if item == element {
			return true
		}
	}
	return false
}
func containstring(slice []string, element string) bool {
	for _, item := range slice {
		if item == element {
			return true
		}
	}
	return false
}
func containAddress(slice []AddressEntry, element AddressEntry) bool {
	for _, item := range slice {
		if item.Address == element.Address {
			return true
		}
	}
	return false
}

// Safely convert a slice of interface{} to a slice of int64
func convertToInt64Slice(input interface{}) ([]int64, error) {
	interfaceSlice, ok := input.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected []interface{}, got %T", input)
	}
	result := make([]int64, len(interfaceSlice))
	for i, v := range interfaceSlice {
		floatVal, ok := v.(float64) // JSON numbers are decoded as float64
		if !ok {
			return nil, fmt.Errorf("expected float64, got %T", v)
		}
		result[i] = int64(floatVal)
	}
	return result, nil
}
func convertToTimeSlice(input interface{}) ([]time.Time, error) {
	interfaceSlice, ok := input.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected []interface{}, got %T", input)
	}
	result := make([]time.Time, len(interfaceSlice))
	for i, v := range interfaceSlice {
		switch val := v.(type) {
		case time.Time:
			result[i] = val
		case string:
			timeVal, err := time.Parse(time.RFC3339, val) // Assuming input strings follow RFC3339 format
			if err != nil {
				return nil, fmt.Errorf("invalid time format for string %q: %v", val, err)
			}
			result[i] = timeVal
		default:
			return nil, fmt.Errorf("expected time.Time or string, got %T", v)
		}
	}
	return result, nil
}

// Safely appends a time.Time to a slice of time.Time stored in an interface{}
func safeAppendTimeSlice(input interface{}, value time.Time) ([]time.Time, error) {
	timeSlice, err := convertToTimeSlice(input)
	if err != nil {
		return nil, err
	}
	return append(timeSlice, value), nil
}
func convertToStringSlice(input interface{}) ([]string, error) {
	interfaceSlice, ok := input.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected []interface{}, got %T", input)
	}
	result := make([]string, len(interfaceSlice))
	for i, v := range interfaceSlice {
		strVal, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", v)
		}
		result[i] = strVal
	}
	return result, nil
}

// Safely appends a string to a slice of strings stored in an interface{}
func safeAppendStringSlice(input interface{}, value string) ([]string, error) {
	stringSlice, err := convertToStringSlice(input)
	if err != nil {
		return nil, err
	}
	return append(stringSlice, value), nil
}

// Safely appends an int64 to a slice of int64 stored in an interface{}
func safeAppendInt64Slice(input interface{}, value int64) ([]int64, error) {
	int64Slice, err := convertToInt64Slice(input)
	if err != nil {
		return nil, err
	}
	return append(int64Slice, value), nil
}

// TweetData represents the structure of the data we store in Redis
type TweetData struct {
	Tweet        string                 `json:"tweet"`
	Status       string                 `json:"status"`
	PostTime     string                 `json:"post_time"`
	ProfileImage string                 `json:"profile_image"`
	Params       map[string]interface{} `json:"params"`
}

func getElementData(query string, tweets []selenium.WebElement, timeCount int64, plotTime time.Time, redisClient *redis.Client) {
	redisKeyPrefix := fmt.Sprintf("spltoken:%s:", query)
	wg := sync.WaitGroup{}
	dataChan := make(chan selenium.WebElement, len(tweets)) // Channel for tweets

	// Worker function for concurrent processing
	worker := func() {
		for tweet := range dataChan {
			processTweet(tweet, redisKeyPrefix, timeCount, plotTime, redisClient)
		}
		wg.Done()
	}

	// Start worker goroutines
	numWorkers := 4 // Adjust based on system resources
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	// Feed the channel with tweets
	for _, tweet := range tweets {
		dataChan <- tweet
	}
	close(dataChan)

	// Wait for all workers to finish
	wg.Wait()
	fmt.Println("All tweets processed.")
}
func isSameMinute(t1, t2 time.Time) bool {
	return t1.Year() == t2.Year() &&
		t1.Month() == t2.Month() &&
		t1.Day() == t2.Day() &&
		t1.Hour() == t2.Hour() &&
		t1.Minute() == t2.Minute()
}

// Process an individual tweet
func processTweet(tweet selenium.WebElement, redisKeyPrefix string, timeCount int64, plotTime time.Time, redisClient *redis.Client) {
	// Extract data in bulk
	content, err := tweet.Text()
	if err != nil {
		log.Printf("Error getting text of tweet: %v", err)
		return
	}

	// Extract status URL
	linkElement, err := tweet.FindElement(selenium.ByCSSSelector, "a[role='link'][href*='/status/']")
	if err != nil {
		log.Printf("Error locating link element: %v", err)
		return
	}
	statusURL, err := linkElement.GetAttribute("href")
	if err != nil {
		log.Printf("Error retrieving href attribute: %v", err)
		return
	}
	redisKey := redisKeyPrefix + statusURL

	// Check if the tweet already exists in Redis
	existingData, err := redisClient.Get(redisKey).Result()
	if err == redis.Nil {
		// Extract other attributes
		datetimeValue := getAttr(tweet, "time", "datetime")
		profileImgURL := getAttr(tweet, `div[data-testid="Tweet-User-Avatar"] img`, "src")

		// Create new data
		newData := TweetData{
			Tweet:        content,
			Status:       statusURL,
			PostTime:     datetimeValue,
			ProfileImage: profileImgURL,
			Params: map[string]interface{}{
				"likes":     []string{getText(tweet, "[data-testid='like']")},
				"retweet":   []string{getText(tweet, "[data-testid='retweet']")},
				"comment":   []string{getText(tweet, "[data-testid='reply']")},
				"views":     []string{getText(tweet, "a[aria-label*='views']")},
				"time":      []int64{timeCount},
				"plot_time": []time.Time{plotTime},
			},
		}

		// Save the new data in Redis
		dataJSON, _ := json.Marshal(newData)
		err := redisClient.Set(redisKey, dataJSON, 0).Err()
		if err != nil {
			log.Printf("Error saving new tweet data to Redis: %v", err)
		}
	} else if err == nil {
		// Update existing data
		var existingEntry TweetData
		err := json.Unmarshal([]byte(existingData), &existingEntry)
		if err != nil {
			log.Printf("Error unmarshaling existing Redis data: %v", err)
			return
		}

		// Update params
		params := existingEntry.Params
		/*timeSlice, err := convertToInt64Slice(params["time"])
		if err != nil {
			log.Printf("Error converting time to []int64: %v", err)

		}*/
		time_plt, err := convertToTimeSlice(params["plot_time"])
		if err != nil {
			log.Printf("Error converting time to []int64: %v", err)
		}
		if len(time_plt) == 0 || !isSameMinute(time_plt[len(time_plt)-1], plotTime) {
			//if len(timeSlice) == 0 || timeSlice[len(timeSlice)-1] != timeCount {
			params["likes"], _ = safeAppendStringSlice(params["likes"], getText(tweet, "[data-testid='like']"))
			params["retweet"], _ = safeAppendStringSlice(params["retweet"], getText(tweet, "[data-testid='retweet']"))
			params["comment"], _ = safeAppendStringSlice(params["comment"], getText(tweet, "[data-testid='reply']"))
			params["views"], _ = safeAppendStringSlice(params["views"], getText(tweet, "a[aria-label*='views']"))

			// Update time and plot_time
			params["time"], _ = safeAppendInt64Slice(params["time"], timeCount)
			params["plot_time"], _ = safeAppendTimeSlice(params["plot_time"], plotTime)

			// Save updated data back to Redis
			updatedDataJSON, _ := json.Marshal(existingEntry)
			err = redisClient.Set(redisKey, updatedDataJSON, 0).Err()
			if err != nil {
				log.Printf("Error updating tweet data in Redis: %v", err)
			}
		}
	} else {
		log.Printf("Redis error: %v", err)
	}
}
func initRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Update host and port if needed
	})
}

func getAttr(elem selenium.WebElement, selector, attr string) string {
	child, err := elem.FindElement(selenium.ByCSSSelector, selector)
	if err != nil {
		return ""
	}
	value, _ := child.GetAttribute(attr)
	return value
}

func getText(elem selenium.WebElement, selector string) string {
	child, err := elem.FindElement(selenium.ByCSSSelector, selector)
	if err != nil {
		return ""
	}
	text, _ := child.Text()
	return text
}

func scrapeAndSaveTweet(query string, wd selenium.WebDriver, timeCount int64, plot_time time.Time, index int64) {

	var allTweets []selenium.WebElement
	var previousTweetCount int
	var allstatus []string
	numberScroll := 0
	redis_client := initRedisClient()
	for {
		// Find all tweets on the current page
		tweets, err := wd.FindElements(selenium.ByCSSSelector, "article")
		if err != nil {
			log.Printf("Failed to find tweet elements: %v", err)
		}

		// Add new tweets to the allTweets slice if they are not already present
		for i, tweet := range tweets {
			// Check if the tweet is already in `allTweets` to avoid duplication
			if contains(allTweets, tweet) {
				continue
			}
			linkElement, err := tweet.FindElement(selenium.ByCSSSelector, "a[role='link'][href*='/status/']")
			if err != nil {
				log.Printf("Error locating link element for tweet %d: %v", i+1, err)
				continue
			}
			statusURL, err := linkElement.GetAttribute("href")
			if err != nil {
				log.Printf("Error retrieving href attribute for tweet %d: %v", i+1, err)
				continue
			}

			if !containstring(allstatus, statusURL) {
				fmt.Println("=== Twitte Ecist for Brower:-", index, " Query:-", query)
				allstatus = append(allstatus, statusURL)
				getElementData(query, tweets, timeCount, plot_time, redis_client)
			}
		}
		fmt.Println("Tweet Count", len(allstatus), "Previous Tweet Count", previousTweetCount)
		// If no new tweets are added after scrolling, assume we've reached the end
		if len(allstatus) == previousTweetCount {
			log.Println("====== Reached the end of the page.======")
			break
		}
		if len(allstatus) > 60 {
			break
		}
		if numberScroll > 8 {
			break
		}
		// Update the count of tweets collected so far
		previousTweetCount = len(allstatus)

		// Scroll to the bottom to load more tweets
		err = scrollToBottom(wd)
		if err != nil {
			log.Printf("Error scrolling to bottom: %v", err)
			break
		}
		numberScroll += 1
		// Pause to allow content to load
		time.Sleep(5 * time.Second)
	}
}
func createProxyExtension(ip, port, username, password string) string {
	// Create an in-memory ZIP archive
	buffer := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buffer)

	// Add manifest.json to the ZIP
	manifest := []byte(`{
		"version": "1.0.0",
		"manifest_version": 2,
		"name": "Chrome Proxy",
		"permissions": [
			"proxy",
			"webRequest",
			"webRequestBlocking",
			"tabs",
			"unlimitedStorage",
			"storage",
    		"<all_urls>"
		],
		"background": {
			"scripts": ["background.js"]
		},
		"minimum_chrome_version": "76.0.0"
	}`)
	w, err := zipWriter.Create("manifest.json")
	if err != nil {
		panic(err)
	}
	_, err = w.Write(manifest)
	if err != nil {
		panic(err)
	}

	// Add background.js to the ZIP
	background := []byte(fmt.Sprintf(`var config = {
    mode: "fixed_servers",
    rules: {
        singleProxy: {
            scheme: "http",
            host: "%s",
            port: %s
        },
        bypassList: ["localhost"]
    }
};

// Set the proxy configuration
chrome.proxy.settings.set({ value: config, scope: "regular" }, function() {
    console.log("Proxy settings applied");
});

// Listen for authentication requests
chrome.webRequest.onAuthRequired.addListener(
    function(details) {
        console.log("Auth required for proxy", details);
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    },
    { urls: ["<all_urls>"] },
    ["blocking"]
);`, ip, port, username, password))
	w, err = zipWriter.Create("background.js")
	if err != nil {
		panic(err)
	}
	_, err = w.Write(background)
	if err != nil {
		panic(err)
	}

	// Close the ZIP writer
	err = zipWriter.Close()
	if err != nil {
		panic(err)
	}

	// Base64 encode the ZIP content
	return base64.StdEncoding.EncodeToString(buffer.Bytes())
}

type AddressEntry struct {
	Address string `json:"address"`
	Name    string `json:"name"`
	Symbol  string `json:"symbol"`
	Index   int64  `json:"index"`
}
type CookieEntry struct {
	Address string `json:"cookies"`
}
type ProxiesEntry struct {
	Address string `json:"proxies"`
}

/*var (
	addresses = make(map[string]bool) // Use a map to track unique addresses
	mu        sync.Mutex              // Mutex to prevent race conditions
)*/

func readJSONFileCookie(filePath string) []CookieEntry {
	//for {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		time.Sleep(5 * time.Second) // Retry after 5 seconds
		//continue
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		_ = file.Close()
		time.Sleep(5 * time.Second)
		//continue
	}
	_ = file.Close()

	// Parse JSON into a slice of AddressEntry
	var jsonAddresses []CookieEntry
	err = json.Unmarshal(data, &jsonAddresses)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		time.Sleep(5 * time.Second)
		//continue
	}
	return jsonAddresses
	//}
}
func readJSONFileProxy(filePath string) []ProxiesEntry {
	//for {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		time.Sleep(5 * time.Second) // Retry after 5 seconds
		//continue
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		_ = file.Close()
		time.Sleep(5 * time.Second)
		//continue
	}
	_ = file.Close()

	// Parse JSON into a slice of AddressEntry
	var jsonAddresses []ProxiesEntry
	err = json.Unmarshal(data, &jsonAddresses)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		time.Sleep(5 * time.Second)
		//continue
	}
	return jsonAddresses
	//}
}
func readJSONFile(filePath string) []AddressEntry {
	//for {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		time.Sleep(5 * time.Second) // Retry after 5 seconds
		//continue
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		_ = file.Close()
		time.Sleep(5 * time.Second)
		//continue
	}
	_ = file.Close()

	// Parse JSON into a slice of AddressEntry
	var jsonAddresses []AddressEntry
	err = json.Unmarshal(data, &jsonAddresses)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		time.Sleep(5 * time.Second)
		//continue
	}
	return jsonAddresses
	//}
}
func main() {

	var addrArray []AddressEntry
	var cookArray []string
	var prxArray []string

	var previousLength int = 0
	addArryChn := make(chan []AddressEntry, 100)
	resultChan := make(chan Result)
	index := 0
	semaphore := make(chan struct{}, 10)
	var wg sync.WaitGroup
	go func() {
		wg.Wait()
		//close(addArryChn)
	}()
	if err := updateSessionStatus(0, 0, "New"); err != nil {
		log.Printf("Failed to update session status: %v", err)
	}
	for {
		addressArray := readJSONFile("addresses/address.json")
		for _, entry := range addressArray {
			if !containAddress(addrArray, entry) {
				addrArray = append(addrArray, entry)
				fmt.Println("Address List", addrArray)
			}
		}
		cookiesArray := readJSONFileCookie("datacenter/cookies.json")
		for _, entry := range cookiesArray {
			if !containstring(cookArray, entry.Address) {
				cookArray = append(cookArray, entry.Address)
			}
		}
		proxiesArray := readJSONFileProxy("datacenter/proxies.json")
		for _, entry := range proxiesArray {
			if !containstring(prxArray, entry.Address) {
				prxArray = append(prxArray, entry.Address)
			}
		}

		//if len(addrArray) != previousLength {
		if index < len(cookiesArray) { //for i := previousLength; i < len(addrArray); i++ {
			//query := addrArray[i]
			userAgents := []string{
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
				"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
				"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
				"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
				"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0",
			}

			cookies := cookArray
			rand.Seed(time.Now().UnixNano())
			rndInput := index //rand.Intn(len(userAgents))
			randomUserAgent := userAgents[index%len(userAgents)]
			fmt.Println("USer Agents:-s", index%len(userAgents))
			randomProxy := prxArray[rndInput] //proxyArray[rndInput]
			proxyPart := strings.Split(randomProxy, ":")
			ipAddress := proxyPart[0]
			port := proxyPart[1]
			username := proxyPart[2]
			password := proxyPart[3]
			thecookies := cookies[rndInput]
			semaphore <- struct{}{} // Acquire semaphore
			wg.Add(1)

			go func(ipAddress_ string, port_ string, query_ chan []AddressEntry, randomUserAgent_ string, cookies string, username string, password string, index_ int) {

				defer wg.Done()
				//defer func() { <-semaphore }() // Release semaphore
				fmt.Println("Query:-", query_, "Agent:- ", randomUserAgent_, "Ip:- ", ipAddress_, "Port:- ", port_, "UserNAme And Password:-", username, "&", password, "Cookies:-", cookies, "Index:-", index_)

				extent := createProxyExtension(ipAddress_, port_, "uyxzmtjn", "ietipyjz5ls7")

				fmt.Printf("Starting scrape with fingerprint %d\n", index_)
				caps := generateUniqueFingerprint(extent, randomUserAgent_, index_)
				scrapeTwitterSearch(query_, index_, len(cookiesArray), caps, cookies, resultChan)

			}(ipAddress, port, addArryChn, randomUserAgent, thecookies, username, password, index)
			index += 1

			time.Sleep(5 * time.Second)
		}

		if len(addrArray) != previousLength {
			//addArryChn <- addrArray
			for i := range len(cookiesArray) {
				addArryChn <- addrArray
				fmt.Println("Sending To IndexL-", i)
			}

		}
		previousLength = len(addrArray)

		select {
		case result := <-resultChan:
			if result.Success {
				fmt.Printf("Goroutine %d succeeded.\n", result.Index)
			} else {
				fmt.Printf("Goroutine %d failed: %s\n", result.Index, result.ErrorMsg)
			}
		default:
			continue

		}
	}

	// Wait for all goroutines to finish

}
