package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/tebeka/selenium"
)

const (
	chromedriverPath = "./chromedriver" // Update this with your ChromeDriver path for Termux
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

// generateUniqueFingerprint configures Chrome capabilities for Android.
// It sets required options and an experimental "androidPackage" for Chromium.
func generateUniqueFingerprint(userAgent, proxy string) selenium.Capabilities {
	caps := selenium.Capabilities{"browserName": "chrome"}
	chromeOptions := map[string]interface{}{
		"args": []string{
			"--no-sandbox",
			"--disable-dev-shm-usage",
		},
		"androidPackage": "org.chromium.chrome.stable",
		"prefs": map[string]interface{}{
			"general.useragent.override": userAgent,
		},
	}
	caps["goog:chromeOptions"] = chromeOptions

	if proxy != "" {
		caps["proxy"] = map[string]interface{}{
			"proxyType": "manual",
			"httpProxy": proxy,
			"sslProxy":  proxy,
		}
	}
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
		state, err := driver.ExecuteScript("return document.readyState", nil)
		if err != nil {
			log.Printf("Error checking document.readyState: %v", err)
		}

		if state == "complete" {
			contentLoaded, err := driver.ExecuteScript(`
				return document.querySelector('[role="progressbar"]') === null &&
					   document.querySelectorAll('article').length > 0;
			`, nil)
			if err != nil {
				log.Printf("Error checking dynamic content load: %v", err)
			}

			if contentLoaded == true {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
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

func scrapeTwitterSearch(query_ chan []AddressEntry, index int, totalNode int, caps selenium.Capabilities, cookies_ string, resultChan chan Result) {
	port, err := getRandomPort()
	if err != nil {
		log.Fatalf("Failed to get a random port: %v", err)
	}
	fmt.Printf("Using random port: %d\n", port)
	// Start a ChromeDriver service for Chromium on Termux via ADB.
	opts := []selenium.ServiceOption{}
	service, err := selenium.NewChromeDriverService(chromedriverPath, port, opts...)
	if err != nil {
		log.Printf("Error starting ChromeDriver service: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error starting ChromeDriver service: %v", err)}
		return
	}
	defer service.Stop()

	// Connect to the WebDriver
	wd, err := selenium.NewRemote(caps, fmt.Sprintf("http://localhost:%d/wd/hub", port))
	if err != nil {
		log.Printf("Error connecting to WebDriver: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error connecting to WebDriver: %v", err)}
		return
	}
	defer wd.Quit()

	// Navigate to Twitter homepage
	fmt.Println("Navigating to Twitter homepage.")
	err = wd.Get("https://x.com")
	if err != nil {
		log.Printf("Failed to load Twitter homepage: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Failed to load Twitter homepage: %v", err)}
		return
	}
	time.Sleep(10 * time.Second)
	fmt.Println("Navigated to Twitter homepage.")

	// Import the cookie JSON
	file, err := os.Open("./session_token.json")
	if err != nil {
		fmt.Println("Error opening cookie file:", err)
		return
	}
	defer file.Close()

	var cookies []selenium.Cookie
	json.NewDecoder(file).Decode(&cookies)

	cookie := &selenium.Cookie{
		Name:   "auth_token",
		Value:  cookies_,
		Path:   "/",
		Domain: ".x.com",
		Expiry: uint(time.Now().Add(24 * time.Hour).Unix()),
	}

	if err := wd.AddCookie(cookie); err != nil {
		log.Printf("Error adding cookie: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error adding cookie: %v", err)}
		return
	}
	fmt.Println("Cookie added.")
	time.Sleep(5 * time.Second)

	// Refresh the page to apply the cookie
	if err := wd.Refresh(); err != nil {
		log.Printf("Error refreshing page: %v", err)
		resultChan <- Result{Index: index, Success: false, ErrorMsg: fmt.Sprintf("Error refreshing page: %v", err)}
		return
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
		if currentURL == "https://twitter.com/?mx=2" || currentURL == "https://x.com/?mx=2" {
			fmt.Println("Login unsuccessful: Redirected back to the login page.")
			resultChan <- Result{Index: index, Success: false, ErrorMsg: "Redirected back to login page."}
			return
		}
	} else {
		fmt.Println("Login successful: User is authenticated.")
	}
	fmt.Println("Browser index", index)

	var query []AddressEntry
	searchURL := ""

	forMe := false
	var q_index int
	for {
		for qu := range query_ { // Listen for messages from the broadcast channel
			query = qu
			fmt.Println("Searched Query", query, "index", index, "SS", len(query))
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
		if forMe {
			break
		}
	}

	now := time.Now()
	yesterday := now.Add(-24 * time.Hour)
	yesterdate := yesterday.Format("2006-01-02")
	searchURL = fmt.Sprintf(`%s%s OR "$%s" since:%s`, twitterSearchURL, query[q_index].Address, query[q_index].Symbol, yesterdate)
	err = wd.Get(searchURL)
	if err != nil {
		log.Printf("Failed to load Twitter search page: %v", err)
	}
	fmt.Println("Searched Query Completed", query[q_index], "Of Browser:", index)

	time.Sleep(5 * time.Second)
	n_index := q_index
	for i := 0; i < 1000; i++ {
		timeCount := 2 * (i + 1)
		currentTime := time.Now().UTC()
		waitForPageLoad(wd)
		scrapeAndSaveTweet(query[n_index].Address, wd, int64(timeCount), currentTime, int64(index))
		time.Sleep(2 * time.Minute)
		if err := wd.Refresh(); err != nil {
			log.Printf("Error refreshing page: %v", err)
		}
		time.Sleep(1 * time.Second)
		fmt.Println("Page refreshed for Search Query.")

		select {
		case query_ := <-query_:
			qn_index := -1
			for i, qux := range query_ {
				if qux.Index == int64(index) {
					qn_index = i
				}
			}
			if len(query_) > totalNode && qn_index > -1 {
				fmt.Println("== Adjusting to New Query === Index", qn_index, "Browser Index:", index)
				n_index = qn_index
				if query_[n_index].Index == int64(index) {
					i = 0
					searchURL := fmt.Sprintf(`%s%s OR "$%s" since:%s`, twitterSearchURL, query_[n_index].Address, query_[n_index].Symbol, yesterdate)
					err = wd.Get(searchURL)
					if err != nil {
						log.Printf("Failed to load Twitter search page: %v", err)
					}
					fmt.Println("Searched New Updated Query Completed", query_[n_index].Address, "Of Browser:", index)
					query = query_
				}
			}
		default:
			qn_index := -1
			for x, qux_ := range query {
				if qux_.Index == int64(index) {
					qn_index = x
				}
			}
			if len(query) > totalNode && qn_index > -1 {
				fmt.Println("== Adjusting to Forgotten Query === Index", qn_index, "Browser Index:", index)
				n_index = qn_index
				i = 0
				if query[n_index].Index == int64(index) {
					searchURL := fmt.Sprintf(`%s%s OR "$%s" since:%s`, twitterSearchURL, query[n_index].Address, query[n_index].Symbol, yesterdate)
					err = wd.Get(searchURL)
					if err != nil {
						log.Printf("Failed to load Twitter search page: %v", err)
					}
					fmt.Println("Searched Forgotten Query Completed", query[n_index].Address, "Of Browser:", index)
				}
			}
			fmt.Println("No value in the channel")
		}
	}
	resultChan <- Result{Index: index, Success: true, ErrorMsg: ""}
}

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

func convertToInt64Slice(input interface{}) ([]int64, error) {
	interfaceSlice, ok := input.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected []interface{}, got %T", input)
	}
	result := make([]int64, len(interfaceSlice))
	for i, v := range interfaceSlice {
		floatVal, ok := v.(float64)
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
			timeVal, err := time.Parse(time.RFC3339, val)
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

func safeAppendStringSlice(input interface{}, value string) ([]string, error) {
	stringSlice, err := convertToStringSlice(input)
	if err != nil {
		return nil, err
	}
	return append(stringSlice, value), nil
}

func safeAppendInt64Slice(input interface{}, value int64) ([]int64, error) {
	int64Slice, err := convertToInt64Slice(input)
	if err != nil {
		return nil, err
	}
	return append(int64Slice, value), nil
}

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
	dataChan := make(chan selenium.WebElement, len(tweets))

	worker := func() {
		for tweet := range dataChan {
			processTweet(tweet, redisKeyPrefix, timeCount, plotTime, redisClient)
		}
		wg.Done()
	}

	numWorkers := 4
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	for _, tweet := range tweets {
		dataChan <- tweet
	}
	close(dataChan)
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

func processTweet(tweet selenium.WebElement, redisKeyPrefix string, timeCount int64, plotTime time.Time, redisClient *redis.Client) {
	content, err := tweet.Text()
	if err != nil {
		log.Printf("Error getting text of tweet: %v", err)
		return
	}

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

	existingData, err := redisClient.Get(redisKey).Result()
	if err == redis.Nil {
		datetimeValue := getAttr(tweet, "time", "datetime")
		profileImgURL := getAttr(tweet, `div[data-testid="Tweet-User-Avatar"] img`, "src")

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

		dataJSON, _ := json.Marshal(newData)
		err := redisClient.Set(redisKey, dataJSON, 0).Err()
		if err != nil {
			log.Printf("Error saving new tweet data to Redis: %v", err)
		}
	} else if err == nil {
		var existingEntry TweetData
		err := json.Unmarshal([]byte(existingData), &existingEntry)
		if err != nil {
			log.Printf("Error unmarshaling existing Redis data: %v", err)
			return
		}
		params := existingEntry.Params
		time_plt, err := convertToTimeSlice(params["plot_time"])
		if err != nil {
			log.Printf("Error converting time to []time.Time: %v", err)
		}
		if len(time_plt) == 0 || !isSameMinute(time_plt[len(time_plt)-1], plotTime) {
			params["likes"], _ = safeAppendStringSlice(params["likes"], getText(tweet, "[data-testid='like']"))
			params["retweet"], _ = safeAppendStringSlice(params["retweet"], getText(tweet, "[data-testid='retweet']"))
			params["comment"], _ = safeAppendStringSlice(params["comment"], getText(tweet, "[data-testid='reply']"))
			params["views"], _ = safeAppendStringSlice(params["views"], getText(tweet, "a[aria-label*='views']"))
			params["time"], _ = safeAppendInt64Slice(params["time"], timeCount)
			params["plot_time"], _ = safeAppendTimeSlice(params["plot_time"], plotTime)

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
		Addr: "localhost:6379",
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
		tweets, err := wd.FindElements(selenium.ByCSSSelector, "article")
		if err != nil {
			log.Printf("Failed to find tweet elements: %v", err)
		}

		for i, tweet := range tweets {
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
				fmt.Println("=== Tweet Exists for Browser:", index, " Query:", query)
				allstatus = append(allstatus, statusURL)
				getElementData(query, tweets, timeCount, plot_time, redis_client)
			}
		}
		fmt.Println("Tweet Count", len(allstatus), "Previous Tweet Count", previousTweetCount)
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
		previousTweetCount = len(allstatus)
		err = scrollToBottom(wd)
		if err != nil {
			log.Printf("Error scrolling to bottom: %v", err)
			break
		}
		numberScroll++
		time.Sleep(5 * time.Second)
	}
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

func readJSONFileCookie(filePath string) []CookieEntry {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		time.Sleep(5 * time.Second)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		_ = file.Close()
		time.Sleep(5 * time.Second)
	}
	_ = file.Close()
	var jsonAddresses []CookieEntry
	err = json.Unmarshal(data, &jsonAddresses)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		time.Sleep(5 * time.Second)
	}
	return jsonAddresses
}

func readJSONFileProxy(filePath string) []ProxiesEntry {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		time.Sleep(5 * time.Second)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		_ = file.Close()
		time.Sleep(5 * time.Second)
	}
	_ = file.Close()
	var jsonAddresses []ProxiesEntry
	err = json.Unmarshal(data, &jsonAddresses)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		time.Sleep(5 * time.Second)
	}
	return jsonAddresses
}

func readJSONFile(filePath string) []AddressEntry {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		time.Sleep(5 * time.Second)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		_ = file.Close()
		time.Sleep(5 * time.Second)
	}
	_ = file.Close()
	var jsonAddresses []AddressEntry
	err = json.Unmarshal(data, &jsonAddresses)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		time.Sleep(5 * time.Second)
	}
	return jsonAddresses
}

func main() {
	// Optional: Run ADB devices command to ensure your Android device is connected.
	cmd := exec.Command("adb", "devices")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Error running adb devices: %v", err)
	} else {
		fmt.Printf("ADB devices output:\n%s\n", out)
	}

	var addrArray []AddressEntry
	var cookArray []string
	var prxArray []string
	previousLength := 0
	addArryChn := make(chan []AddressEntry, 100)
	resultChan := make(chan Result)
	index := 0
	semaphore := make(chan struct{}, 10)
	var wg sync.WaitGroup

	go func() {
		wg.Wait()
	}()

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

		if index < len(cookiesArray) {
			userAgents := []string{
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/90.0",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/91.0",
				"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
				"Mozilla/5.0 (Windows NT 6.1; WOW64) Gecko/20100101 Firefox/89.0",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/91.0",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) Firefox/93.0",
				"Mozilla/5.0 (X11; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/91.0",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) Firefox/94.0",
				"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0",
			}

			cookies := cookArray
			rand.Seed(time.Now().UnixNano())
			rndInput := index
			randomUserAgent := userAgents[index%len(userAgents)]
			fmt.Println("User Agent index:", index%len(userAgents))
			randomProxy := prxArray[rndInput]
			proxyParts := strings.Split(randomProxy, ":")
			ipAddress := proxyParts[0]
			port := proxyParts[1]
			// For Chrome, we set the proxy in capabilities.
			proxyStr := fmt.Sprintf("%s:%s", ipAddress, port)
			thecookies := cookies[rndInput]
			semaphore <- struct{}{}
			wg.Add(1)

			go func(ipAddress_ string, port_ string, query_ chan []AddressEntry, randomUserAgent_ string, cookies string, index_ int) {
				defer wg.Done()
				fmt.Println("Query:", query_, "Agent:", randomUserAgent_, "IP:", ipAddress_, "Port:", port_, "Cookies:", cookies, "Index:", index_)
				caps := generateUniqueFingerprint(randomUserAgent_, proxyStr)
				scrapeTwitterSearch(query_, index_, len(cookiesArray), caps, cookies, resultChan)
			}(ipAddress, port, addArryChn, randomUserAgent, thecookies, index)
			index++
			time.Sleep(5 * time.Second)
		}

		if len(addrArray) != previousLength {
			for i := range cookiesArray {
				addArryChn <- addrArray
				fmt.Println("Sending to Index:", i)
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
}
