// Package twitternotify provides a notification system that posts messages to Twitter via GraphQL.
package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// TwitterCookieAuth handles authentication with Twitter using cookies
type TwitterCookieAuth struct {
	BaseURL   string
	APIURL    string
	GQLURL    string
	Debug     bool
	Proxy     string
	UserAgent string
	Cookies   map[string]string
	Headers   map[string]string
	Client    *http.Client
}

// TweetCreateRequest contains data needed to create a tweet
type TweetCreateRequest struct {
	Variables struct {
		Tweet struct {
			Text string `json:"text"`
		} `json:"tweet"`
		DarkRequest bool `json:"darkRequest"`
	} `json:"variables"`
	Features map[string]bool `json:"features"`
}

// TweetResponse represents a response from the Twitter API after posting a tweet
type TweetResponse struct {
	Data struct {
		CreateTweet struct {
			TweetResults struct {
				Result struct {
					RestID string `json:"rest_id"`
				} `json:"result"`
			} `json:"tweet_results"`
		} `json:"create_tweet"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors,omitempty"`
}

// Impression represents a tweet impression data point

// Entry represents a single JSON entry from the fetch-data endpoint
type Entry struct {
	Tweet    string `json:"tweet"`
	Params   Params `json:"params"`
	PostTime string `json:"post_time"`
	Status   string `json:"status"`
	Profile  string `json:"profile_image"`
}

// Params holds the metrics arrays for an Entry
type Params struct {
	Views    []string `json:"views"`
	Likes    []string `json:"likes"`
	Comment  []string `json:"comment"`
	Retweet  []string `json:"retweet"`
	Time     []int64  `json:"time"`
	PlotTime []string `json:"plot_time"`
}

// Impression represents a name/value pair for charts
type Impression struct {
	Name  string
	Value int
}

// CompImpression extends Impression with Prev value
type CompImpression struct {
	Name  string
	Value int
	Prev  int
}

// EmojiData holds timestamped emoji markers
type EmojiData struct {
	EmTime int64
	Emoji  string
}

// TweetData holds filtered tweet details
type TweetData struct {
	Tweet     string
	Views     int
	Likes     int
	Timestamp time.Time
}

// Result bundles all processed outputs
type Result struct {
	TweetsWithAddress   []TweetData
	Impressions         []Impression
	Engagements         []Impression
	TweetsPerMinute     []Impression
	TweetViewsPerMinute []CompImpression
	EmojiRawData        []EmojiData
	Usernames           []string
	Tweets              []string
	ViewCounts          []string
	LikeCounts          []string
	Times               []string
	ProfileImages       []string
}

// parseViewsCount converts strings like "1.2K" to integer counts
func parseViewsCount(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	mult := 1.0
	last := s[len(s)-1]
	num := s[:len(s)-1]
	switch last {
	case 'K':
		mult = 1e3
	case 'M':
		mult = 1e6
	default:
		num = s
		mult = 1
	}
	v, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0
	}
	return int(v * mult)
}

// NewTwitterCookieAuth creates a new Twitter cookie authentication client
func NewTwitterCookieAuth(debug bool, proxy, userAgent string) *TwitterCookieAuth {
	if userAgent == "" {
		userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
	}

	var transport *http.Transport
	if proxy != "" {
		// Remove the scheme if present
		proxy = strings.TrimPrefix(proxy, "https://")
		proxy = strings.TrimPrefix(proxy, "http://")

		// Split the proxy string into parts
		parts := strings.Split(proxy, ":")
		if len(parts) != 4 {
			log.Fatalf("Invalid proxy format. Expected format: [scheme]://host:port:username:password")
		}

		host := parts[0]
		port := parts[1]
		username := parts[2]
		password := parts[3]

		// Construct the proxy URL
		proxyURL := &url.URL{
			Scheme: "http",
			User:   url.UserPassword(username, password),
			Host:   fmt.Sprintf("%s:%s", host, port),
		}

		// Set up the transport with the proxy
		transport = &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		}
	} else {
		transport = &http.Transport{}
	}

	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}

	return &TwitterCookieAuth{
		BaseURL:   "https://x.com",
		APIURL:    "https://api.twitter.com",
		GQLURL:    "https://x.com/i/api/graphql",
		Debug:     debug,
		Proxy:     proxy,
		UserAgent: userAgent,
		Cookies:   make(map[string]string),
		Headers: map[string]string{
			"User-Agent":      userAgent,
			"Accept":          "*/*",
			"Accept-Language": "en-US,en;q=0.9",
			"Content-Type":    "application/json",
			"Referer":         "https://x.com/",
			"Origin":          "https://x.com",
		},
		Client: client,
	}
}

// log logs a message if debug mode is enabled
func (t *TwitterCookieAuth) log(message string) {
	if t.Debug {
		fmt.Printf("[DEBUG] %s\n", message)
	}
}

// SetCookies sets the cookies for authentication
func (t *TwitterCookieAuth) SetCookies(cookies map[string]string) {
	t.Cookies = cookies

	// Extract the CSRF token from cookies if present
	if csrfToken, ok := cookies["ct0"]; ok {
		t.Headers["x-csrf-token"] = csrfToken
	}

	// Add the authorization headers
	t.Headers["authorization"] = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
	t.Headers["x-twitter-auth-type"] = "OAuth2Session"
	t.Headers["x-twitter-active-user"] = "yes"
	t.Headers["x-twitter-client-language"] = "en"

	t.log(fmt.Sprintf("Set cookies: %v", cookies))
}

// VerifyAuth verifies if the provided cookies are valid for authentication
func (t *TwitterCookieAuth) VerifyAuth(ctx context.Context) (bool, error) {
	// Try to access the home timeline, which requires authentication
	url := fmt.Sprintf("%s/ci_OQZ2k0rG0Ax_lXRiWVA/HomeTimeline", t.GQLURL)

	// Prepare the request data
	data := map[string]interface{}{
		"variables": map[string]interface{}{
			"count":                  1,
			"includePromotedContent": true,
			"latestControlAvailable": true,
			"requestContext":         "launch",
			"withCommunity":          true,
		},
		"features": map[string]bool{
			"rweb_video_screen_enabled":                                               false,
			"profile_label_improvements_pcf_label_in_post_enabled":                    true,
			"rweb_tipjar_consumption_enabled":                                         true,
			"responsive_web_graphql_exclude_directive_enabled":                        true,
			"verified_phone_label_enabled":                                            false,
			"creator_subscriptions_tweet_preview_api_enabled":                         true,
			"responsive_web_graphql_timeline_navigation_enabled":                      true,
			"responsive_web_graphql_skip_user_profile_image_extensions_enabled":       false,
			"premium_content_api_read_enabled":                                        false,
			"communities_web_enable_tweet_community_results_fetch":                    true,
			"c9s_tweet_anatomy_moderator_badge_enabled":                               true,
			"responsive_web_grok_analyze_button_fetch_trends_enabled":                 false,
			"responsive_web_grok_analyze_post_followups_enabled":                      true,
			"responsive_web_jetfuel_frame":                                            false,
			"responsive_web_grok_share_attachment_enabled":                            true,
			"articles_preview_enabled":                                                true,
			"responsive_web_edit_tweet_api_enabled":                                   true,
			"graphql_is_translatable_rweb_tweet_is_translatable_enabled":              true,
			"view_counts_everywhere_api_enabled":                                      true,
			"longform_notetweets_consumption_enabled":                                 true,
			"responsive_web_twitter_article_tweet_consumption_enabled":                true,
			"tweet_awards_web_tipping_enabled":                                        false,
			"responsive_web_grok_show_grok_translated_post":                           false,
			"responsive_web_grok_analysis_button_from_backend":                        true,
			"creator_subscriptions_quote_tweet_preview_enabled":                       false,
			"freedom_of_speech_not_reach_fetch_enabled":                               true,
			"standardized_nudges_misinfo":                                             true,
			"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": true,
			"longform_notetweets_rich_text_read_enabled":                              true,
			"longform_notetweets_inline_media_enabled":                                true,
			"responsive_web_grok_image_annotation_enabled":                            true,
			"responsive_web_enhance_cards_enabled":                                    false,
		},
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		t.log(fmt.Sprintf("Error marshaling request data: %v", err))
		return false, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		t.log(fmt.Sprintf("Error creating request: %v", err))
		return false, err
	}

	// Add headers
	for key, value := range t.Headers {
		req.Header.Set(key, value)
	}

	// Add cookies
	for name, value := range t.Cookies {
		req.AddCookie(&http.Cookie{
			Name:  name,
			Value: value,
		})
	}

	resp, err := t.Client.Do(req)
	if err != nil {
		t.log(fmt.Sprintf("Auth verification error: %v", err))
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.log(fmt.Sprintf("Auth verification failed with status code: %d", resp.StatusCode))
		return false, fmt.Errorf("authentication failed with status code: %d", resp.StatusCode)
	}

	// Parse response
	var responseData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&responseData); err != nil {
		t.log(fmt.Sprintf("Error decoding response: %v", err))
		return false, err
	}

	// Check if we got a valid home timeline response
	data, ok := responseData["data"].(map[string]interface{})
	if !ok {
		t.log("Auth verification failed: data field missing")
		return false, nil
	}

	_, ok = data["home"]
	if !ok {
		t.log("Auth verification failed: home field missing")
		return false, nil
	}

	t.log("Authentication verified successfully!")
	return true, nil
}

// PostTweet posts a tweet to Twitter
// PostTweet posts a new Tweet via Twitter’s internal GraphQL API.
func (t *TwitterCookieAuth) PostTweet(ctx context.Context, message string) (string, error) {
	// 1. Ensure we’re logged in
	ok, err := t.VerifyAuth(ctx)
	if err != nil {
		return "", fmt.Errorf("verify auth: %w", err)
	}
	if !ok {
		return "", errors.New("not authenticated")
	}
	// 2. Activate guest
	gReq, _ := http.NewRequestWithContext(ctx, "POST",
		"https://api.twitter.com/1.1/guest/activate.json", nil)
	gReq.Header.Set("Authorization", t.Headers["Authorization"])
	gResp, _ := t.Client.Do(gReq)
	var gData struct {
		GuestToken string `json:"guest_token"`
	}
	json.NewDecoder(gResp.Body).Decode(&gData)

	// 2. Define GraphQL payload details in‑func
	const (
		queryID = "IVdJU2Vjw2llhmJOAZy9Ow" // scrape from Network tab if it changes
		opName  = "CreateTweet"
	)
	featureFlags := map[string]bool{
		"rweb_video_screen_enabled":                                               false,
		"profile_label_improvements_pcf_label_in_post_enabled":                    true,
		"rweb_tipjar_consumption_enabled":                                         true,
		"responsive_web_graphql_exclude_directive_enabled":                        true,
		"verified_phone_label_enabled":                                            false,
		"creator_subscriptions_tweet_preview_api_enabled":                         true,
		"responsive_web_graphql_timeline_navigation_enabled":                      true,
		"responsive_web_graphql_skip_user_profile_image_extensions_enabled":       false,
		"premium_content_api_read_enabled":                                        false,
		"communities_web_enable_tweet_community_results_fetch":                    true,
		"c9s_tweet_anatomy_moderator_badge_enabled":                               true,
		"responsive_web_grok_analyze_button_fetch_trends_enabled":                 false,
		"responsive_web_grok_analyze_post_followups_enabled":                      true,
		"responsive_web_jetfuel_frame":                                            false,
		"responsive_web_grok_share_attachment_enabled":                            true,
		"articles_preview_enabled":                                                true,
		"responsive_web_edit_tweet_api_enabled":                                   true,
		"graphql_is_translatable_rweb_tweet_is_translatable_enabled":              true,
		"view_counts_everywhere_api_enabled":                                      true,
		"longform_notetweets_consumption_enabled":                                 true,
		"responsive_web_twitter_article_tweet_consumption_enabled":                true,
		"tweet_awards_web_tipping_enabled":                                        false,
		"responsive_web_grok_show_grok_translated_post":                           false,
		"responsive_web_grok_analysis_button_from_backend":                        true,
		"creator_subscriptions_quote_tweet_preview_enabled":                       false,
		"freedom_of_speech_not_reach_fetch_enabled":                               true,
		"standardized_nudges_misinfo":                                             true,
		"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": true,
		"longform_notetweets_rich_text_read_enabled":                              true,
		"longform_notetweets_inline_media_enabled":                                true,
		"responsive_web_grok_image_annotation_enabled":                            true,
		"responsive_web_enhance_cards_enabled":                                    false,
	}

	// 3. Build the request payload
	reqBody := struct {
		OperationName string `json:"operationName"`
		Variables     struct {
			TweetText   string `json:"tweet_text"`
			DarkRequest bool   `json:"dark_request"`
		} `json:"variables"`
		QueryID  string          `json:"queryId"`
		Features map[string]bool `json:"features"`
	}{
		OperationName: opName,
		Variables: struct {
			TweetText   string `json:"tweet_text"`
			DarkRequest bool   `json:"dark_request"`
		}{
			TweetText:   message,
			DarkRequest: false,
		},
		QueryID:  queryID,
		Features: featureFlags, // as before
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal request payload: %w", err)
	}

	// 4. Construct endpoint and HTTP request
	endpoint := fmt.Sprintf("%s/%s/CreateTweet", t.GQLURL, queryID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return "", fmt.Errorf("create HTTP request: %w", err)
	}

	// 5. Headers & cookies
	req.Header.Set("Content-Type", "application/json")
	//req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", t.Headers["Authorization"])
	req.Header.Set("x-guest-token", gData.GuestToken)
	req.Header.Set("X-CSRF-Token", t.Cookies["ct0"])
	req.Header.Set("x-twitter-active-user", "yes")
	req.Header.Set("x-twitter-client-language", "en")
	// Fetch-metadata headers:
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-User", "?1")
	for k, v := range t.Headers {
		req.Header.Set(k, v)
	}
	for name, val := range t.Cookies {
		req.AddCookie(&http.Cookie{Name: name, Value: val})
	}
	fmt.Printf("Posting to: %s\nHeaders: %+v\n", req.URL.String(), req.Header)

	// 6. Send and handle response
	resp, err := t.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("post tweet: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("tweet failed %d: %s", resp.StatusCode, string(body))
	}

	var tr TweetResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if len(tr.Errors) > 0 {
		return "", fmt.Errorf("twitter error: %s", tr.Errors[0].Message)
	}

	return tr.Data.CreateTweet.TweetResults.Result.RestID, nil
}

// NotificationSystem manages notifications and sends them to Twitter
type NotificationSystem struct {
	TwitterAuth     *TwitterCookieAuth
	LastNotiTime    time.Time
	MinIntervalSecs int
}

// NewNotificationSystem creates a new notification system
func NewNotificationSystem(twitterAuth *TwitterCookieAuth, minIntervalSecs int) *NotificationSystem {
	return &NotificationSystem{
		TwitterAuth:     twitterAuth,
		LastNotiTime:    time.Now().Add(-1 * time.Hour), // Initialize with time in the past
		MinIntervalSecs: minIntervalSecs,
	}
}

// TriggerAlert sends a notification to Twitter if conditions are met
func (n *NotificationSystem) TriggerAlert(ctx context.Context, address string, symbolName string, impressions []CompImpression, threshold int) (bool, error) {
	// Check if enough time has passed since the last notification
	if time.Since(n.LastNotiTime).Seconds() < float64(n.MinIntervalSecs) {
		return false, nil
	}

	// Find the most recent impression data
	if len(impressions) == 0 {
		return false, nil
	}

	// Sort impressions by time (newest first)
	// In Go we'd typically use sort.Slice here, but for simplicity we'll just find the newest
	var newestImpression CompImpression
	var newestTime time.Time

	for _, imp := range impressions {
		t, err := time.Parse("2006-01-02T15:04", imp.Name)
		if err != nil {
			continue
		}

		if newestTime.IsZero() || t.After(newestTime) {
			newestTime = t
			newestImpression = imp
		}
	}

	// Check if view count has increased beyond threshold
	viewDiff := newestImpression.Value - newestImpression.Prev
	if viewDiff < threshold {
		return false, nil
	}

	// Format the notification message
	message := fmt.Sprintf("🚨 Most Win: %s (%s) has gained %d new views in the last period! Current views: %d #crypto #trending",
		address,
		symbolName,
		viewDiff,
		newestImpression.Value)

	// Post the tweet
	tweetID, err := n.TwitterAuth.PostTweet(ctx, message)
	if err != nil {
		return false, fmt.Errorf("failed to post alert tweet: %w", err)
	}
	fmt.Println("Tweet ID", tweetID)
	// Update last notification time
	n.LastNotiTime = time.Now()

	return true, nil
}

// apiEntry maps the expected JSON structure from the /fetch-data endpoint.
type apiEntry struct {
	Tweet    string `json:"tweet"`
	PostTime string `json:"post_time"`
	Status   string `json:"status"`
	Params   struct {
		Time  []string `json:"time"`
		Views []string `json:"views"`
	} `json:"params"`
}

// FetchAndProcessImpressions fetches and processes impressions for a given address.

// contains checks if substr is within str.
func contains(str, substr string) bool {
	return strings.Contains(str, substr)
}

// parseViewsCount converts a view-count string to an integer.

// fetchHostnameFromConfig should be implemented to retrieve your service hostname.
func fetchHostnameFromConfig() string {
	// e.g., read from env var or config file
	return "localhost"
}

type Address struct {
	Address string `json:"address"`
}

// fetchAddresses retrieves and parses the JSON array of addresses.
func FetchAddresses(hostname string) ([]Address, error) {
	url := fmt.Sprintf("http://%s:3300/addresses/address.json", hostname)
	resp, err := http.Get(url) // http.Get makes an HTTP GET request :contentReference[oaicite:3]{index=3}
	if err != nil {
		return nil, fmt.Errorf("failed GET %s: %w", url, err) // Wrap error with context :contentReference[oaicite:4]{index=4}
	}
	defer resp.Body.Close() // Always close the body to avoid leaks :contentReference[oaicite:5]{index=5}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body) // Read the entire response body
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var addrs []Address
	if err := json.Unmarshal(data, &addrs); err != nil { // json.Unmarshal decodes JSON into Go structs :contentReference[oaicite:6]{index=6}
		return nil, fmt.Errorf("unmarshal JSON: %w", err)
	}
	return addrs, nil
}

// FetchData retrieves and processes tweet metrics for a given address
func FetchAndProcessImpressions(hostname, address string) (*Result, error) {
	url := fmt.Sprintf("http://%s:3300/fetch-data?search=%s", hostname, address)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, 0)
	if err := json.Unmarshal(body, &entries); err != nil {
		fmt.Println("Error Error", err)
		return nil, err
	}
	result := &Result{}

	// Maps for aggregations
	viewCounts := make(map[string]int)
	engCounts := make(map[string]int)
	tweetCounts := make(map[string]int)
	tweetViews := make(map[string]CompImpression)
	emojiMap := make(map[int64]string)

	for _, e := range entries {
		// Filter tweets containing address
		pt, err := time.Parse(time.RFC3339Nano, e.PostTime)
		if err != nil {
			continue
		}
		if strings.Contains(e.Tweet, address) {
			t := TweetData{
				Tweet:     e.Tweet,
				Views:     parseViewsCount(lastOrEmpty(e.Params.Views)),
				Likes:     parseViewsCount(lastOrEmpty(e.Params.Likes)),
				Timestamp: pt,
			}
			result.TweetsWithAddress = append(result.TweetsWithAddress, t)
		}

		// For all valid entries (tweet contains address or symbol)
		// here, symbol logic omitted for brevity

		// Extract profile and username
		if e.Profile != "" {
			result.ProfileImages = append(result.ProfileImages, e.Profile)
		}
		if parts := strings.Split(e.Status, "https://x.com/"); len(parts) > 1 {
			user := strings.Split(parts[1], "/status/")[0]
			result.Usernames = append(result.Usernames, "@"+user)
		}
		result.Tweets = append(result.Tweets, e.Tweet)
		result.ViewCounts = append(result.ViewCounts, lastOrEmpty(e.Params.Views))
		result.LikeCounts = append(result.LikeCounts, lastOrEmpty(e.Params.Likes))
		result.Times = append(result.Times, pt.Format(time.RFC3339))

		// Aggregation per minute
		minuteKey := pt.UTC().Format("2006-01-02T15:04")
		tweetCounts[minuteKey]++

		// Engagement per plot_time
		for i, pt := range e.Params.PlotTime {
			tsTime, err := time.Parse(time.RFC3339Nano, pt)
			if err != nil {
				continue
			}
			plotTime := tsTime.UTC().Format("2006-01-02T15:04")
			views := parseViewsCount(at(e.Params.Views, i))
			likes := parseViewsCount(at(e.Params.Likes, i))
			comments := parseViewsCount(at(e.Params.Comment, i))
			retweets := parseViewsCount(at(e.Params.Retweet, i))
			viewCounts[plotTime] += views
			engCounts[plotTime] += likes + comments + retweets

			// Emoji assignment
			ts := tsTime.Unix() - (tsTime.Unix() % 60)
			//ts := pt - (pt % 60)
			if _, ok := emojiMap[ts]; !ok {
				sent := views
				switch {
				case sent > 10000:
					emojiMap[ts] = "💎"
				case sent > 5000:
					emojiMap[ts] = "♦️"
				case sent > 1000:
					emojiMap[ts] = "🥇"
				case sent > 500:
					emojiMap[ts] = "🥈"
				default:
					emojiMap[ts] = "😎"
				}
			}
		}

		// Tweet views per minute delta
		last := parseViewsCount(lastOrEmpty(e.Params.Views))
		prev := parseViewsCount(prevOrEmpty(e.Params.Views))
		t := tweetViews[minuteKey]
		t.Name = minuteKey
		t.Value += last
		t.Prev += prev
		tweetViews[minuteKey] = t
	}

	// Convert maps to slices and sort
	for name, v := range viewCounts {
		result.Impressions = append(result.Impressions, Impression{Name: name, Value: v})
	}
	for name, v := range engCounts {
		result.Engagements = append(result.Engagements, Impression{Name: name, Value: v})
	}
	for name, v := range tweetCounts {
		result.TweetsPerMinute = append(result.TweetsPerMinute, Impression{Name: name, Value: v})
	}
	for _, c := range tweetViews {
		result.TweetViewsPerMinute = append(result.TweetViewsPerMinute, c)
	}
	for ts, emo := range emojiMap {
		result.EmojiRawData = append(result.EmojiRawData, EmojiData{EmTime: ts, Emoji: emo})
	}

	// Sorting omitted for brevity; use sort.Slice

	return result, nil
}

// Helper: safe index or empty
func at(arr []string, i int) string {
	if i < 0 || i >= len(arr) {
		return ""
	}
	return arr[i]
}

// Helper: last element or empty
func lastOrEmpty(arr []string) string {
	if len(arr) == 0 {
		return ""
	}
	return arr[len(arr)-1]
}

// Helper: previous element or empty
func prevOrEmpty(arr []string) string {
	if len(arr) < 2 {
		return ""
	}
	return arr[len(arr)-2]
}
func ProcessTweet(addresses []Address) {
	for _, addr := range addresses { // Iterate over slice with range :contentReference[oaicite:9]{index=9}
		impressions, err := FetchAndProcessImpressions("localhost", addr.Address)
		if err != nil {
			fmt.Printf("error processing %s: %v\n", addr.Address, err)
			continue
		}
		fmt.Printf("Processed %s: %+v\n", addr.Address, impressions)
	}
}

// Example usage
func ExampleUsage() {
	// Create auth client
	auth := NewTwitterCookieAuth(true, "", "")

	// Set cookies (these would need to be obtained from a browser or login process)
	cookies := map[string]string{
		"auth_token": "your-auth-token",
		"ct0":        "your-csrf-token",
		// Add other required cookies
	}
	auth.SetCookies(cookies)

	// Create notification system
	notifier := NewNotificationSystem(auth, 3600) // Minimum 1 hour between notifications

	// Example address and metadata
	address := "0x123456789abcdef"
	symbol := "ETH"

	// In a real application, you'd have a monitoring loop
	ctx := context.Background()

	// Mock impressions data
	impressions, _ := FetchAndProcessImpressions("", address)

	// Set threshold to trigger notification at 1000 new views
	sent, err := notifier.TriggerAlert(ctx, address, symbol, impressions.TweetViewsPerMinute, 1000)
	if err != nil {
		fmt.Printf("Error triggering alert: %v\n", err)
	} else if sent {
		fmt.Println("Alert notification sent successfully!")
	} else {
		fmt.Println("No alert triggered (threshold not met or too soon)")
	}
}
