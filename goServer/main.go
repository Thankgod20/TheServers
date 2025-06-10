package main

import (
	"bytes"
	"compress/gzip"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blocto/solana-go-sdk/common"
	"github.com/blocto/solana-go-sdk/program/metaplex/token_metadata"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
	aggregate "github.com/thankgod20/scraperServer/API"
	flow "github.com/thankgod20/scraperServer/API"
	holderhistory "github.com/thankgod20/scraperServer/API"
	snapshot "github.com/thankgod20/scraperServer/API"
	srs "github.com/thankgod20/scraperServer/API"
	holders "github.com/thankgod20/scraperServer/Holders"
	"github.com/thankgod20/scraperServer/notify"
)

const (
	filePath_                    = "extractedData.json"
	staticFolder                 = "/static"
	tokenFolder                  = "/spltoken"
	contentTypeKey               = "Content-Type"
	contentTypeVal               = "application/json"
	MAX_CONCURRENT_FETCHES       = 20
	GeckoTerminalRateLimit       = 30
	GeckoTerminalRateLimitPeriod = 1 * time.Minute
)

var (
	// jobQueue will hold the addresses of tokens that need their holder history updated.
	jobQueue               = make(chan Address, 500) // Buffer of 500 jobs
	highPriorityOhlcvQueue = make(chan Address, 50)
)

const (
	apiLimitPerMinute = 10
	tickInterval      = 1 * time.Second
)

// --- END OF NEW LINES ---

// ... (your struct definitions like NFTMetadata, Address, etc.) ...
// ----------------------
// Structures for Add-Address and Migration Events
// ----------------------

type NFTMetadata struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
	URI    string `json:"uri"`
}
type NullTime struct {
	time.Time
}
type Transfer struct {
	Address string   `json:"address"`
	Amount  float64  `json:"amount"`
	Time    NullTime `json:"time"`
	Price   float64  `json:"price"`
}

type Address struct {
	Address      string     `json:"address"`
	Name         string     `json:"name"`
	Symbol       string     `json:"symbol"`
	Index        int64      `json:"index"`
	AddedAt      *time.Time `json:"added_at"`
	LastActivity *time.Time `json:"last_activity"`
}
type Session struct {
	ID      int    `json:"id"`
	Port    int    `json:"port"`
	Status  string `json:"status"`
	Updated string `json:"updated"`
}
type Cookies struct {
	AuthToken string `json:"auth_token"`
	Ct0       string `json:"ct0"`
}
type Proxy struct {
	Address string `json:"proxies"`
}
type TokenMetadata struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
	URI    string `json:"uri"`
}

var ExcludeAddrs = make(map[string]bool)

// MigrationEvent represents the JSON payload for a migration event.
type MigrationEvent struct {
	Signature string `json:"signature"`
	Mint      string `json:"mint"`
	TxType    string `json:"txType"`
	Pool      string `json:"pool"`
}
type HolderData struct {
	Address string    `json:"address"`
	Price   float64   `json:"price"`
	Amount  []float64 `json:"amount"`
	Time    []string  `json:"time"`
}

type TokenData struct {
	Holders []HolderData `json:"holders"`
}

var RedisClient *redis.Client

func init() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Update with your Redis server address
		Password: "",               // Update with your Redis password, if any
		DB:       0,                // Default DB

	})

	// Test the connection
	_, err := RedisClient.Ping().Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis!")
}

// API response structure
type APIResponse struct {
	Keys   []string      `json:"keys"`
	Values []interface{} `json:"values"`
}

// ----------------------
// HTTP Handlers (Unchanged)
// ----------------------
// --- ADD THIS ENTIRE NEW FUNCTION ---

func fetchHolderKeysAndDataHandler(w http.ResponseWriter, r *http.Request) {
	searchWord := r.URL.Query().Get("search")
	pattern := fmt.Sprintf("%s", searchWord)
	//fmt.Println("Partern", pattern)

	client := holders.NewBitqueryClient()

	output, err := client.UpdateAndGetTransfers(RedisClient, pattern)
	if err != nil {
		log.Fatalf("Error updating and getting transfers: %v", err)
	}
	//fmt.Println("Data Fatched")
	var transfers []Transfer
	if err := json.Unmarshal([]byte(output), &transfers); err != nil {
		log.Fatalf("Error decoding transfers: %v", err)
	}
	if len(transfers) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "No matching keys found"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(transfers); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

func fetchKeysAndDataHandler(w http.ResponseWriter, r *http.Request) {
	searchWord := r.URL.Query().Get("search")
	pattern := fmt.Sprintf("*spltoken:%s*", searchWord)

	keys, err := RedisClient.Keys(pattern).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to retrieve keys: %v", err), http.StatusInternalServerError)
		return
	}

	var values []interface{}
	for _, key := range keys {
		value_, err := RedisClient.Get(key).Result()
		if err != nil {
			log.Printf("Error retrieving value for key %s: %v", key, err)
			continue
		}
		value, _ := decompress([]byte(value_))
		var rawValue interface{}
		if err := json.Unmarshal((value), &rawValue); err == nil {
			values = append(values, rawValue)
		} else {
			cleanedValue := strings.ReplaceAll(string(value), "\\\"", "\"")
			values = append(values, cleanedValue)
		}
	}

	if len(values) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "No matching keys found"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(values); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

func getTokenMetadata(w http.ResponseWriter, r *http.Request) {
	mintAddress := r.URL.Query().Get("mint")
	if mintAddress == "" {
		http.Error(w, "Missing 'mint' query parameter", http.StatusBadRequest)
		return
	}
	mintPubKey, err := solana.PublicKeyFromBase58(mintAddress)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid mint address: %v", err), http.StatusBadRequest)
		return
	}
	client := rpc.New(rpc.MainNetBeta_RPC)
	commonMintPubKey := common.PublicKeyFromBytes(mintPubKey.Bytes())
	metadataPubKey, err := token_metadata.GetTokenMetaPubkey(commonMintPubKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to derive metadata public key: %v", err), http.StatusInternalServerError)
		return
	}
	accountInfo, err := client.GetAccountInfo(context.Background(), solana.PublicKeyFromBytes(metadataPubKey[:]))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch account info: %v", err), http.StatusInternalServerError)
		return
	}
	if accountInfo == nil || accountInfo.Value == nil {
		http.Error(w, "Metadata account not found", http.StatusNotFound)
		return
	}
	data := accountInfo.Value.Data.GetBinary()
	if data == nil {
		http.Error(w, "Failed to retrieve binary data from account info", http.StatusInternalServerError)
		return
	}
	metadata, err := token_metadata.MetadataDeserialize(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to deserialize metadata: %v", err), http.StatusInternalServerError)
		return
	}

	response := NFTMetadata{
		Name:   metadata.Data.Name,
		Symbol: metadata.Data.Symbol,
		URI:    metadata.Data.Uri,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

func saveDataHandler(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var newData map[string]map[string]interface{}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Unable to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	//fmt.Println("Raw body:", string(body))
	if err := json.Unmarshal(body, &newData); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	rootDir := ensureFolder(staticFolder)
	existingData := make(map[string]map[string]interface{})
	filePath := filepath.Join(rootDir, filePath_)
	if _, err := os.Stat(filePath); err == nil {
		data, err := ioutil.ReadFile(filePath)
		if err == nil {
			json.Unmarshal(data, &existingData)
		}
	}
	for tokenAddress, tokenData := range newData {
		if _, exists := existingData[tokenAddress]; !exists {
			existingData[tokenAddress] = tokenData
		}
	}
	data, _ := json.MarshalIndent(existingData, "", "  ")
	if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
		http.Error(w, "Error saving data", http.StatusInternalServerError)
		return
	}

	w.Write([]byte("Data saved successfully"))
}

func tweetHandler(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Unable to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	//fmt.Println("Recieveing", string(body))
	var requestData map[string]map[string]interface{}
	if err := json.Unmarshal(body, &requestData); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	rootDir := ensureFolder(tokenFolder)
	for tweetURL, data := range requestData {
		address := fmt.Sprintf("%v", data["address"])
		if address == "" {
			continue
		}
		filePath := filepath.Join(rootDir, address+".json")
		var fileData map[string]interface{}
		if _, err := os.Stat(filePath); err == nil {
			fileContent, _ := ioutil.ReadFile(filePath)
			json.Unmarshal(fileContent, &fileData)
		} else {
			fileData = make(map[string]interface{})
		}
		tweetContent, ok := data["tweet"].(string)
		if !ok || tweetContent == "" {
			fmt.Println("Missing or invalid tweet content")
			continue
		}
		if _, exists := fileData[tweetURL]; !exists {
			fileData[tweetURL] = map[string]interface{}{
				"tweetData": map[string]string{
					"tweet": tweetContent,
				},
			}
			dataToWrite, _ := json.MarshalIndent(fileData, "", "  ")
			ioutil.WriteFile(filePath, dataToWrite, 0644)
		}
	}

	w.Write([]byte(`{"message": "Data processed successfully."}`))
}

func tweetURLHandler(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Unable to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	if strings.TrimSpace(string(body)) == "" {
		http.Error(w, "Request body is empty", http.StatusBadRequest)
		return
	}

	w.Write([]byte(`{"message": "Data saved successfully"}`))
}

func ensureFolder(folder string) string {
	rootDir := filepath.Join("..", folder)
	if err := os.MkdirAll(rootDir, os.ModePerm); err != nil {
		fmt.Println(err)
	}
	return rootDir
}

func contains(slice []string, item string) bool {
	for _, value := range slice {
		if value == item {
			return true
		}
	}
	return false
}

// ----------------------
// New Helper Function to Get Token Metadata via API
// ----------------------

func getTokenMetadataFromAPI(mint string) (*TokenMetadata, error) {
	url := fmt.Sprintf("http://localhost:3300/api/token-metadata?mint=%s", mint)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch token metadata, status code: %d", resp.StatusCode)
	}
	var metadata TokenMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// ----------------------
// WebSocket Migration Subscription and Address Update Logic
// ----------------------

// subscribeMigration connects to the WebSocket endpoint and subscribes for migration events.
func subscribeMigration() {
	wsURL := "wss://pumpportal.fun/api/data"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Println("WebSocket dial error:", err)
		return
	}
	defer conn.Close()
	log.Println("Connected to migration subscription at", wsURL)

	// Send subscription request for migration events.
	subscription := map[string]string{"method": "subscribeMigration"}
	msg, err := json.Marshal(subscription)
	if err != nil {
		log.Println("Error marshalling subscription:", err)
		return
	}

	if err = conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		log.Println("Error sending subscription message:", err)
		return
	}
	log.Println("Sent migration subscription message.")

	// Continuously read messages.
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Error reading message:", err)
			break
		}
		log.Println("Received migration event:", string(message))
		// Parse the incoming migration event.
		var event MigrationEvent
		err = json.Unmarshal(message, &event)
		if err != nil {
			log.Println("Error parsing migration event:", err)
			continue
		}
		// Only process if it is a migration event.
		if event.TxType == "migrate" {
			err = addMigratedTokenToAddresses(event)
			if err != nil {
				log.Println("Error adding migrated token to addresses:", err)
			} else {
				log.Println("Added migrated token", event.Mint, "to addresses")
			}
		}
	}
}

// addMigratedTokenToAddresses reads the current addresses file and appends the new token if not already present.
/*
func addMigratedTokenToAddresses(event MigrationEvent) error {
	addressesFile := filepath.Join("..", "addresses", "address.json")
	var addresses []Address
	if data, err := ioutil.ReadFile(addressesFile); err == nil {
		json.Unmarshal(data, &addresses)
	} else {
		addresses = []Address{}
	}
	if int64(len(addresses)) < 25 {
		// Check for duplicates.
		for _, addr := range addresses {
			if strings.EqualFold(addr.Address, event.Mint) {
				log.Println("Token already exists in addresses:", event.Mint)
				return nil
			}
		}

		// Fetch token metadata.
		metadata, err := getTokenMetadataFromAPI(event.Mint)
		if err != nil {
			log.Println("Failed to get metadata for", event.Mint, err)
			metadata = &TokenMetadata{Name: "", Symbol: "", URI: ""}
		}

		newIndex := int64(len(addresses))
		if newIndex > 4 {
			newIndex = int64(len(addresses)) % 5
		}
		// Record addition time
		now := time.Now().UTC()
		fmt.Println("Time Now", now)
		newAddress := Address{
			Address:      event.Mint,
			Name:         metadata.Name,
			Symbol:       metadata.Symbol,
			Index:        newIndex,
			AddedAt:      &now,
			LastActivity: &now,
		}
		addresses = append(addresses, newAddress)
		os.MkdirAll(filepath.Dir(addressesFile), os.ModePerm)
		file, err := os.Create(addressesFile)
		if err != nil {
			return err
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")

		return encoder.Encode(addresses)
	}
	return nil
}
*/

func addMigratedTokenToAddresses(event MigrationEvent) error {
	addressesFile := filepath.Join("..", "addresses", "address.json")
	var addresses []Address

	// Read existing addresses
	if data, err := ioutil.ReadFile(addressesFile); err == nil {
		json.Unmarshal(data, &addresses)
	} else {
		addresses = []Address{}
	}

	if int64(len(addresses)) < 500 {
		// Check for duplicates
		for _, addr := range addresses {
			if strings.EqualFold(addr.Address, event.Mint) {
				log.Println("Token already exists in addresses:", event.Mint)
				return nil
			}
		}

		// Fetch token metadata
		metadata, err := getTokenMetadataFromAPI(event.Mint)
		if err != nil {
			log.Println("Failed to get metadata for", event.Mint, err)
			metadata = &TokenMetadata{Name: "", Symbol: "", URI: ""}
		}

		newIndex := int64(len(addresses))
		if newIndex > 49 {
			newIndex = int64(len(addresses)) % 50
		}

		// Record addition time
		now := time.Now().UTC()
		newAddress := Address{
			Address:      event.Mint,
			Name:         metadata.Name,
			Symbol:       metadata.Symbol,
			Index:        newIndex,
			AddedAt:      &now,
			LastActivity: &now,
		}

		addresses = append(addresses, newAddress)
		os.MkdirAll(filepath.Dir(addressesFile), os.ModePerm)
		file, err := os.Create(addressesFile)
		if err != nil {
			return err
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")

		return encoder.Encode(addresses)
	}

	// If addresses are 25 or more, read activesession.json
	sessionsFile := filepath.Join("..", "datacenter", "activesession.json")
	data, err := ioutil.ReadFile(sessionsFile)
	if err != nil {
		return err
	}

	var sessions []Session
	if err := json.Unmarshal(data, &sessions); err != nil {
		return err
	}

	for _, session := range sessions {
		if strings.Contains(session.Status, "ArraySize") {
			arraySize, err := extractArraySize(session.Status)
			if err != nil {
				continue
			}

			if arraySize < 5 {
				// Fetch token metadata
				metadata, err := getTokenMetadataFromAPI(event.Mint)
				if err != nil {
					log.Println("Failed to get metadata for", event.Mint, err)
					metadata = &TokenMetadata{Name: "", Symbol: "", URI: ""}
				}

				// Record addition time
				now := time.Now().UTC()
				newAddress := Address{
					Address:      event.Mint,
					Name:         metadata.Name,
					Symbol:       metadata.Symbol,
					Index:        int64(session.ID),
					AddedAt:      &now,
					LastActivity: &now,
				}

				addresses = append(addresses, newAddress)
				os.MkdirAll(filepath.Dir(addressesFile), os.ModePerm)
				file, err := os.Create(addressesFile)
				if err != nil {
					return err
				}
				defer file.Close()
				encoder := json.NewEncoder(file)
				encoder.SetIndent("", "  ")

				return encoder.Encode(addresses)
			}
		}
	}

	return errors.New("no suitable session found to add the new address")
}

// Helper function to extract ArraySize value from status string
func extractArraySize(status string) (int, error) {
	re := regexp.MustCompile(`ArraySize\s+(\d+)`)
	matches := re.FindStringSubmatch(status)
	if len(matches) < 2 {
		return 0, errors.New("ArraySize not found in status")
	}
	var size int
	fmt.Sscanf(matches[1], "%d", &size)
	return size, nil
}

// ----------------------
// New Endpoints: Add Proxies, Cookies, and Addresses
// ----------------------

func addProxiesHandler(w http.ResponseWriter, r *http.Request) {
	outputPath := filepath.Join("..", "datacenter", "proxies.json")
	var proxies []Proxy
	if data, err := ioutil.ReadFile(outputPath); err == nil {
		json.Unmarshal(data, &proxies)
	}

	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}
		action := r.FormValue("action")
		switch action {
		case "add":
			newProxiesText := r.FormValue("newProxies")
			lines := strings.Split(newProxiesText, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					proxies = append(proxies, Proxy{Address: line})
				}
			}
		case "edit":
			indexStr := r.FormValue("index")
			newProxyValue := strings.TrimSpace(r.FormValue("proxy"))
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(proxies) && newProxyValue != "" {
				proxies[index].Address = newProxyValue
			}
		case "delete":
			indexStr := r.FormValue("index")
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(proxies) {
				proxies = append(proxies[:index], proxies[index+1:]...)
			}
		default:
			http.Error(w, "Unknown action", http.StatusBadRequest)
			return
		}
		os.MkdirAll(filepath.Dir(outputPath), os.ModePerm)
		file, err := os.Create(outputPath)
		if err != nil {
			http.Error(w, "Error updating file", http.StatusInternalServerError)
			return
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(proxies); err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/add-proxies", http.StatusSeeOther)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Manage Proxies</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    textarea { width: 100%; height: 100px; }
    input[type="text"] { width: 300px; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
    th { background-color: #f2f2f2; }
    form.inline { display: inline; }
    button { margin: 2px; }
  </style>
</head>
<body>
  <h1>Manage Proxies</h1>
  <h2>Add New Proxies</h2>
  <form method="POST" action="/add-proxies">
    <textarea name="newProxies" placeholder="Enter proxies, one per line (IP:PORT)"></textarea><br>
    <input type="hidden" name="action" value="add">
    <button type="submit">Add Proxies</button>
  </form>
  <h2>Current Proxies</h2>
  <table>
    <tr>
      <th>#</th>
      <th>Proxy</th>
      <th>Actions</th>
    </tr>`
	for i, proxy := range proxies {
		html += `<tr>
      <td>` + fmt.Sprintf("%d", i) + `</td>
      <td>
        <form method="POST" action="/add-proxies" class="inline">
          <input type="text" name="proxy" value="` + proxy.Address + `" required>
          <input type="hidden" name="index" value="` + fmt.Sprintf("%d", i) + `">
          <input type="hidden" name="action" value="edit">
          <button type="submit">Update</button>
        </form>
      </td>
      <td>
        <form method="POST" action="/add-proxies" class="inline">
          <input type="hidden" name="index" value="` + fmt.Sprintf("%d", i) + `">
          <input type="hidden" name="action" value="delete">
          <button type="submit" onclick="return confirm('Delete proxy?');">Delete</button>
        </form>
      </td>
    </tr>`
	}
	html += `</table>
</body>
</html>`
	w.Write([]byte(html))
}

func addCookiesHandler(w http.ResponseWriter, r *http.Request) {
	outputPath := filepath.Join("..", "datacenter", "cookies.json")
	var cookies []Cookies

	// Load existing cookies.json if present
	if data, err := ioutil.ReadFile(outputPath); err == nil {
		err = json.Unmarshal(data, &cookies)
		if err != nil {
			http.Error(w, "Error parsing existing JSON", http.StatusInternalServerError)
			return
		}
	}

	if r.Method == http.MethodPost {
		// Parse form values
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}

		action := r.FormValue("action")
		switch action {
		case "add", "edit":
			// Update both fields
			authToken := strings.TrimSpace(r.FormValue("auth_token"))
			ct0 := strings.TrimSpace(r.FormValue("ct0"))
			if authToken == "" || ct0 == "" {
				http.Error(w, "Both auth_token and ct0 are required", http.StatusBadRequest)
				return
			}
			cookies = append(cookies, Cookies{AuthToken: authToken, Ct0: ct0})

		case "delete":
			// Clear the JSON
			indexStr := r.FormValue("index")
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(cookies) {
				cookies = append(cookies[:index], cookies[index+1:]...)
			}
			//cookies = []Cookies{}

		default:
			http.Error(w, "Unknown action", http.StatusBadRequest)
			return
		}

		// Ensure directory exists
		os.MkdirAll(filepath.Dir(outputPath), os.ModePerm)

		// Write updated JSON
		file, err := os.Create(outputPath)
		if err != nil {
			http.Error(w, "Error updating file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(cookies); err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
			return
		}

		// Redirect to refresh page
		http.Redirect(w, r, "/add-cookies", http.StatusSeeOther)
		return
	}

	// Render the HTML form
	// Render HTML form
	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Manage Cookies Array</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    input[type="text"] { width: 400px; margin-bottom: 10px; }
    button { margin-top: 10px; padding: 6px 12px; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; }
    th, td { border: 1px solid #ccc; padding: 8px; }
    th { background-color: #f2f2f2; }
    form.inline { display: inline; }
  </style>
</head>
<body>
  <h1>Manage Cookies Array JSON</h1>

  <h2>Add New Entry</h2>
  <form method="POST" action="/add-cookies">
    <div>
      <label for="auth_token">Auth Token:</label><br>
      <input type="text" id="auth_token" name="auth_token" required>
    </div>
    <div>
      <label for="ct0">ct0:</label><br>
      <input type="text" id="ct0" name="ct0" required>
    </div>
    <input type="hidden" name="action" value="add">
    <button type="submit">Add</button>
  </form>

  <h2>Current Entries</h2>
  <table>
    <tr><th>#</th><th>Auth Token</th><th>ct0</th><th>Actions</th></tr>` + renderRows(cookies) + `
  </table>
</body>
</html>`

	w.Write([]byte(html))
}
func renderRows(cookies []Cookies) string {
	rows := ""
	for i, c := range cookies {
		rows += fmt.Sprintf(`
<tr>
  <td>%d</td>
  <td><form method="POST" action="/add-cookies" class="inline">
      <input type="text" name="auth_token" value="%s" required>
      <input type="text" name="ct0" value="%s" required>
      <input type="hidden" name="index" value="%d">
      <input type="hidden" name="action" value="edit">
      <button type="submit">Update</button>
    </form></td>
  <td>
    <form method="POST" action="/add-cookies" class="inline">
      <input type="hidden" name="index" value="%d">
      <input type="hidden" name="action" value="delete">
      <button type="submit" onclick="return confirm('Delete this entry?');">Delete</button>
    </form>
  </td>
</tr>`, i, c.AuthToken, c.Ct0, i, i)
	}
	return rows
}

// Helper to preview JSON in the page
func toJSONPreview(c Cookies) string {
	data, _ := json.MarshalIndent(c, "", "  ")
	return string(data)
}

func addAddressHandler(w http.ResponseWriter, r *http.Request) {
	outputPath := filepath.Join("..", "addresses", "address.json")
	var addresses []Address
	if data, err := ioutil.ReadFile(outputPath); err == nil {
		json.Unmarshal(data, &addresses)
	}

	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}
		action := r.FormValue("action")
		switch action {
		case "add":
			newAddress := strings.TrimSpace(r.FormValue("newAddress"))
			newIndexStr := strings.TrimSpace(r.FormValue("newIndex"))
			if newAddress != "" && newIndexStr != "" {
				newIndex, err := strconv.ParseInt(newIndexStr, 10, 64)
				if err == nil {
					metadata, err := getTokenMetadataFromAPI(newAddress)
					if err != nil {
						metadata = &TokenMetadata{Name: "", Symbol: "", URI: ""}
					}
					now := time.Now().UTC()
					addresses = append(addresses, Address{
						Address:      newAddress,
						Name:         metadata.Name,
						Symbol:       metadata.Symbol,
						Index:        newIndex,
						AddedAt:      &now,
						LastActivity: &now,
					})
				}
			}
		case "edit":
			rowStr := r.FormValue("row")
			newAddress := strings.TrimSpace(r.FormValue("address"))
			newIndexStr := strings.TrimSpace(r.FormValue("index"))
			row, err := strconv.Atoi(rowStr)
			if err == nil && row >= 0 && row < len(addresses) && newAddress != "" && newIndexStr != "" {
				newIndex, err2 := strconv.ParseInt(newIndexStr, 10, 64)
				if err2 == nil {
					metadata, err3 := getTokenMetadataFromAPI(newAddress)
					if err3 != nil {
						metadata = &TokenMetadata{Name: "", Symbol: "", URI: ""}
					}
					now := time.Now().UTC()
					addresses[row] = Address{
						Address:      newAddress,
						Name:         metadata.Name,
						Symbol:       metadata.Symbol,
						Index:        newIndex,
						AddedAt:      &now,
						LastActivity: &now,
					}
				}
			}
		case "delete":
			rowStr := r.FormValue("row")
			row, err := strconv.Atoi(rowStr)
			if err == nil && row >= 0 && row < len(addresses) {
				addresses = append(addresses[:row], addresses[row+1:]...)
			}
		default:
			http.Error(w, "Unknown action", http.StatusBadRequest)
			return
		}
		os.MkdirAll(filepath.Dir(outputPath), os.ModePerm)
		file, err := os.Create(outputPath)
		if err != nil {
			http.Error(w, "Error updating file", http.StatusInternalServerError)
			return
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(addresses); err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/add-address", http.StatusSeeOther)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Manage Addresses</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    input[type="text"], input[type="number"] { width: 200px; padding: 5px; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
    th { background-color: #f2f2f2; }
    form.inline { display: inline; }
    button { margin: 2px; }
  </style>
</head>
<body>
  <h1>Manage Addresses</h1>
  <h2>Add New Address</h2>
  <form method="POST" action="/add-address">
    <label for="newAddress">Mint Address:</label>
    <input type="text" name="newAddress" required>
    <label for="newIndex">Index:</label>
    <input type="number" name="newIndex" required>
    <input type="hidden" name="action" value="add">
    <button type="submit">Add Address</button>
  </form>
  <h2>Current Addresses</h2>
  <table>
    <tr>
      <th>#</th>
      <th>Mint Address</th>
      <th>Name</th>
      <th>Symbol</th>
      <th>Index</th>
      <th>Actions</th>
    </tr>`
	for i, addr := range addresses {
		html += fmt.Sprintf(`<tr>
      <td>%d</td>
      <td>
        <form method="POST" action="/add-address" class="inline">
          <input type="text" name="address" value="%s" required>
      </td>
      <td>%s</td>
      <td>%s</td>
      <td>
          <input type="number" name="index" value="%d" required>
          <input type="hidden" name="row" value="%d">
          <input type="hidden" name="action" value="edit">
          <button type="submit">Update</button>
        </form>
      </td>
      <td>
        <form method="POST" action="/add-address" class="inline">
          <input type="hidden" name="row" value="%d">
          <input type="hidden" name="action" value="delete">
          <button type="submit" onclick="return confirm('Delete this address?');">Delete</button>
        </form>
      </td>
    </tr>`, i, addr.Address, addr.Name, addr.Symbol, addr.Index, i, i)
	}
	html += `</table>
</body>
</html>`
	w.Write([]byte(html))
}

type SessionStatus struct {
	ID      int    `json:"id"`
	Port    int    `json:"port"`
	Status  string `json:"status"`
	Updated string `json:"updated"`
}

func readSessionStatusHandler(w http.ResponseWriter, r *http.Request) {
	filePath := "../datacenter/activesession.json"
	var sessions []SessionStatus
	if _, err := os.Stat(filePath); err == nil {
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			http.Error(w, "Error reading session file", http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(data, &sessions); err != nil {
			http.Error(w, "Error parsing session data", http.StatusInternalServerError)
			return
		}
	} else {
		sessions = []SessionStatus{}
	}

	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Active Sessions</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; }
    th { background-color: #f2f2f2; }
  </style>
</head>
<body>
  <h1>Active Sessions</h1>`
	if len(sessions) == 0 {
		html += `<p>No active sessions found.</p>`
	} else {
		html += `<table>
      <tr>
        <th>ID</th>
        <th>Port</th>
        <th>Status</th>
        <th>Updated</th>
      </tr>`
		for _, s := range sessions {
			html += fmt.Sprintf(`<tr>
        <td>%d</td>
        <td>%d</td>
        <td>%s</td>
        <td>%s</td>
      </tr>`, s.ID, s.Port, s.Status, s.Updated)
		}
		html += `</table>`
	}
	html += `
</body>
</html>`
	w.Write([]byte(html))
}

// ----------------------
// Middleware Helpers
// ----------------------

func setHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set(contentTypeKey, contentTypeVal)
}

func withCORS(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		handler(w, r)
	}
}
func withCORSStat(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

// Pool response structure
type PoolResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// OHLCV structure (partial, based on your sample)
type OhlcvResponse struct {
	Data struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		Attributes struct {
			OhlcvList [][]float64 `json:"ohlcv_list"`
		} `json:"attributes"`
	} `json:"data"`
	Meta json.RawMessage `json:"meta"` // you can expand this if needed
}
type OHLCV struct {
	Timestamp int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

// parseRawToOHLCV converts [][]float64 to []OHLCV
func parseRawToOHLCV(rawList [][]float64) []OHLCV {
	out := make([]OHLCV, 0, len(rawList))
	for _, row := range rawList {
		if len(row) < 6 {
			continue
		}
		out = append(out, OHLCV{
			Timestamp: int64(row[0]),
			Open:      row[1],
			High:      row[2],
			Low:       row[3],
			Close:     row[4],
			Volume:    row[5],
		})
	}
	return out
}

// Compress data before saving
func compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(data)
	if err != nil {
		return nil, err
	}
	gw.Close()
	return buf.Bytes(), nil
}

// Decompress when reading
func decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// in main.go, add this new function

// watchForNewAddressesAndQueueOhlcv runs continuously, detects newly added addresses,
// and sends them to the high-priority queue for immediate processing.
func watchForNewAddressesAndQueueOhlcv() {
	// This map keeps track of addresses we have already seen and queued
	// to prevent sending them repeatedly.
	knownAddresses := make(map[string]bool)
	log.Println("[ohlcv-watcher] Starting to watch for new addresses.")

	// On startup, populate the map with all existing addresses so we don't re-process them.
	initialAddrs := loadAddressesFromFile()
	for _, addr := range initialAddrs {
		knownAddresses[addr.Address] = true
	}

	ticker := time.NewTicker(5 * time.Second) // Check for new addresses every 5 seconds
	defer ticker.Stop()

	for range ticker.C {
		allAddrs := loadAddressesFromFile()
		for _, addr := range allAddrs {
			// Check if the address is new (i.e., not in our map) and not excluded
			if _, exists := knownAddresses[addr.Address]; !exists && !ExcludeAddrs[addr.Address] {
				log.Printf("[ohlcv-watcher] Detected new high-priority address: %s", addr.Address)

				// Send the new address to the high-priority queue
				select {
				case highPriorityOhlcvQueue <- addr:
					// Mark this address as "known" only after it's successfully queued
					knownAddresses[addr.Address] = true
				default:
					log.Printf("[ohlcv-watcher] High-priority queue is full. Will retry for %s later.", addr.Address)
				}
			}
		}
	}
}

// In main.go, add this new helper function

func processSingleOhlcv(a Address) error {
	if ExcludeAddrs[a.Address] {
		return nil // Double-check it's not excluded
	}

	fmt.Println("===== Getting ohlcv for", a.Address)
	/*
		// Step 1: Get pool ID
		geckoTerminalRequestLock()
		poolUrl := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/solana/tokens/%s/pools", a.Address)
		resp, err := http.Get(poolUrl)
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Printf("[holders] failed to fetch pool data for %s: %v", a.Address, err)
			return nil // Use return instead of continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var poolData PoolResponse
		if err := json.Unmarshal(body, &poolData); err != nil || len(poolData.Data) == 0 {
			log.Printf("[holders] no pool data found for %s: %v", a.Address, err)
			return nil// Use return
		}

		rawPoolID := poolData.Data[0].ID
		poolID := strings.TrimPrefix(rawPoolID, "solana_")

		// Step 2: Get OHLCV
		geckoTerminalRequestLock()
		ohlcvUrl := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/solana/pools/%s/ohlcv/minute?aggregate=1&limit=1000¤cy=USD", poolID)
		ohlcvResp, err := http.Get(ohlcvUrl)
		if err != nil || ohlcvResp.StatusCode != http.StatusOK {
			log.Printf("[holders] failed to fetch OHLCV data for %s: %v", a.Address, err)
			return // Use return
		}
		defer ohlcvResp.Body.Close()

		ohlcvBody, _ := io.ReadAll(ohlcvResp.Body)

		// Parse the new OHLCV data
		var newOhlcv OhlcvResponse
		if err := json.Unmarshal(ohlcvBody, &newOhlcv); err != nil {
			log.Printf("[holders] failed to parse OHLCV JSON for %s: %v", a.Address, err)
			return // Use return
		}
	*/
	// --- Try to get pool ID with rate limiting ---
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // 30-second timeout for example
	defer cancel()

	var poolID string
	var poolData PoolResponse
	select {
	case <-geckoTerminalRateLimiter:
		log.Println("[RATE_LIMITER] Token acquired for pool ID fetch.")
		poolUrl := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/solana/tokens/%s/pools", a.Address)
		req, _ := http.NewRequestWithContext(ctx, "GET", poolUrl, nil)
		resp, err := http.DefaultClient.Do(req)

		if err != nil {
			log.Printf("[ohlcv-process] failed to fetch pool data for %s (network/timeout): %v", a.Address, err)
			return fmt.Errorf("pool_fetch_network_error: %w", err) // Specific error
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			log.Printf("[ohlcv-process] Rate limit hit (429) fetching pool ID for %s", a.Address)
			// Crucially, put the token back if we got one but API said rate limited
			select {
			case geckoTerminalRateLimiter <- struct{}{}:
			default:
			}
			return errors.New("gecko_rate_limit_pool")
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("[ohlcv-process] failed to fetch pool data for %s (status %d): %v", a.Address, resp.StatusCode, err)
			return fmt.Errorf("pool_fetch_status_error_%d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if err := json.Unmarshal(body, &poolData); err != nil || len(poolData.Data) == 0 {
			log.Printf("[ohlcv-process] no pool data found or unmarshal error for %s: %v", a.Address, err)
			return errors.New("pool_data_parse_error")
		}
		rawPoolID := poolData.Data[0].ID
		poolID = strings.TrimPrefix(rawPoolID, "solana_")
	case <-ctx.Done(): // Timeout waiting for rate limit token
		log.Printf("[ohlcv-process] Timeout waiting for rate limit token (pool ID) for %s", a.Address)
		return errors.New("gecko_rate_limit_timeout_pool")
	}

	// Step 2: Get OHLCV
	var newOhlcv OhlcvResponse
	select {
	case <-geckoTerminalRateLimiter:
		log.Println("[RATE_LIMITER] Token acquired for OHLCV fetch.")
		ohlcvUrl := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/solana/pools/%s/ohlcv/minute?aggregate=1&limit=1000¤cy=USD", poolID)
		req, _ := http.NewRequestWithContext(ctx, "GET", ohlcvUrl, nil)
		ohlcvResp, err := http.DefaultClient.Do(req)

		if err != nil {
			log.Printf("[ohlcv-process] failed to fetch OHLCV data for %s (network/timeout): %v", a.Address, err)
			return fmt.Errorf("ohlcv_fetch_network_error: %w", err)
		}
		defer ohlcvResp.Body.Close()
		if ohlcvResp.StatusCode == http.StatusTooManyRequests {
			log.Printf("[ohlcv-process] Rate limit hit (429) fetching OHLCV for %s", a.Address)
			select {
			case geckoTerminalRateLimiter <- struct{}{}:
			default:
			}
			return errors.New("gecko_rate_limit_ohlcv")
		}
		if ohlcvResp.StatusCode != http.StatusOK {
			log.Printf("[ohlcv-process] failed to fetch OHLCV data for %s (status %d): %v", a.Address, ohlcvResp.StatusCode, err)
			return fmt.Errorf("ohlcv_fetch_status_error_%d", ohlcvResp.StatusCode)
		}
		ohlcvBody, _ := io.ReadAll(ohlcvResp.Body)
		if err := json.Unmarshal(ohlcvBody, &newOhlcv); err != nil {
			log.Printf("[ohlcv-process] failed to parse OHLCV JSON for %s: %v", a.Address, err)
			return errors.New("ohlcv_parse_error")
		}
	case <-ctx.Done(): // Timeout waiting for rate limit token
		log.Printf("[ohlcv-process] Timeout waiting for rate limit token (OHLCV) for %s", a.Address)
		return errors.New("gecko_rate_limit_timeout_ohlcv")
	}
	// Add poolID to meta data
	var metaData map[string]interface{}
	if err := json.Unmarshal(newOhlcv.Meta, &metaData); err != nil {
		metaData = make(map[string]interface{})
	}
	metaData["poolID"] = poolID
	updatedMeta, err := json.Marshal(metaData)
	if err != nil {
		log.Printf("[holders] failed to marshal updated meta data for %s: %v", a.Address, err)
	} else {
		newOhlcv.Meta = updatedMeta
	}

	// Step 3: Check if we already have data in Redis
	redisKey := fmt.Sprintf("ohlcv:%s", a.Address)
	existingData_, err := RedisClient.Get(redisKey).Result()

	if err == nil {
		// Data exists, we need to merge
		existingData, err := decompress([]byte(existingData_))
		if err != nil {
			log.Printf("[decompress] Error decompressing data for %s: %v", a.Address, err)
			return err
		}
		var existingOhlcv OhlcvResponse
		if err := json.Unmarshal(existingData, &existingOhlcv); err != nil {
			log.Printf("[holders] failed to parse existing OHLCV JSON for %s: %v", a.Address, err)
			newData, _ := json.Marshal(newOhlcv)
			compressedData, _ := compress(newData)
			RedisClient.Set(redisKey, compressedData, 0)
			return err // Use return
		}

		// (Rest of the merging logic is identical)
		var existingMetaData map[string]interface{}
		if err := json.Unmarshal(existingOhlcv.Meta, &existingMetaData); err != nil {
			existingMetaData = make(map[string]interface{})
		}
		existingMetaData["poolID"] = poolID
		updatedExistingMeta, err := json.Marshal(existingMetaData)
		if err == nil {
			existingOhlcv.Meta = updatedExistingMeta
		}

		existingOhlcvMap := make(map[float64]bool)
		for _, ohlcv := range existingOhlcv.Data.Attributes.OhlcvList {
			if len(ohlcv) > 0 {
				existingOhlcvMap[ohlcv[0]] = true
			}
		}

		var uniqueNewOhlcv [][]float64
		for _, ohlcv := range newOhlcv.Data.Attributes.OhlcvList {
			if len(ohlcv) > 0 && !existingOhlcvMap[ohlcv[0]] {
				uniqueNewOhlcv = append(uniqueNewOhlcv, ohlcv)
			}
		}

		existingOhlcv.Data.Attributes.OhlcvList = append(existingOhlcv.Data.Attributes.OhlcvList, uniqueNewOhlcv...)

		ohlcvList := existingOhlcv.Data.Attributes.OhlcvList
		sort.Slice(ohlcvList, func(i, j int) bool {
			return ohlcvList[i][0] < ohlcvList[j][0]
		})
		allOHLCV := parseRawToOHLCV(ohlcvList)
		if len(allOHLCV) >= 10 {
			windowSize, priceDropPct, stdDevPctThresh, rugDropPct := 10, 0.80, 0.30, 0.70
			lastIdx := len(allOHLCV) - 1
			if isDeadSlice(allOHLCV, lastIdx, windowSize, priceDropPct, stdDevPctThresh, rugDropPct) {
				log.Printf("[holders] detected dead coin for %s (timestamp %d)\n", a.Address, allOHLCV[lastIdx].Timestamp)
				ExcludeAddrs[a.Address] = true
				if err := SaveExcludedAddresses(ExcludeAddrs, "../addresses/excluded.json"); err != nil {
					log.Printf("Error saving excluded addresses: %v", err)
				}
				return nil // Use return
			}
		}

		mergedData, err := json.Marshal(existingOhlcv)
		if err != nil {
			log.Printf("[holders] failed to marshal merged OHLCV data for %s: %v", a.Address, err)
			return err // Use return
		}

		compressedData, _ := compress(mergedData)
		RedisClient.Set(redisKey, compressedData, 0)

	} else if err == redis.Nil {
		// No existing data
		newData, _ := json.Marshal(newOhlcv)
		compressedData, _ := compress(newData)
		RedisClient.Set(redisKey, compressedData, 0)
	} else {
		// Some other Redis error
		log.Printf("[holders] Redis error for %s: %v", a.Address, err)
	}
	return nil
}
func FetchAndSaveOhlcv() {
	// Step 1: Get the full list of addresses for the main, low-priority cycle.
	addrs := loadAddressesFromFile()
	if addrs == nil {
		log.Println("[ohlcv-loop] No addresses to process in main cycle.")
		return
	}

	// Step 2: Sort the list to process them in a predictable (newest-first) order.
	sort.Slice(addrs, func(i, j int) bool {
		if addrs[i].AddedAt == nil {
			return false
		}
		if addrs[j].AddedAt == nil {
			return true
		}
		return addrs[i].AddedAt.After(*addrs[j].AddedAt)
	})

	log.Printf("[ohlcv-loop] Starting main cycle for %d addresses.", len(addrs))

	// Step 3: Start the interruptible loop.
	for i := 0; i < len(addrs); {

		// This `select` statement is the core of the solution.
		select {
		case highPriorityAddr := <-highPriorityOhlcvQueue:
			// HIGH-PRIORITY PATH: A new address arrived on the channel.
			log.Printf("[ohlcv-loop] INTERRUPT: Processing high-priority address %s", highPriorityAddr.Address)
			//processSingleOhlcv(highPriorityAddr) // Process it immediately.
			// We do NOT increment `i`. We pause the main loop to handle this, then resume.
			err := processSingleOhlcv(highPriorityAddr)
			if err != nil {
				// Check if the error is due to rate limiting or a transient API issue
				errMsg := err.Error()
				if strings.HasPrefix(errMsg, "gecko_rate_limit") ||
					strings.HasPrefix(errMsg, "pool_fetch_status_error_5") || // 5xx server errors
					strings.HasPrefix(errMsg, "ohlcv_fetch_status_error_5") ||
					strings.HasPrefix(errMsg, "pool_fetch_network_error") ||
					strings.HasPrefix(errMsg, "ohlcv_fetch_network_error") {

					log.Printf("[ohlcv-loop] High-priority task for %s failed due to retryable error (%v). Re-queuing.", highPriorityAddr.Address, err)
					// Re-queue with a small delay to prevent tight loops on persistent rate limits
					time.AfterFunc(5*time.Second, func() { // 5-second delay
						select {
						case highPriorityOhlcvQueue <- highPriorityAddr:
							log.Printf("[ohlcv-loop] Re-queued %s to high-priority.", highPriorityAddr.Address)
						default:
							log.Printf("[ohlcv-loop] High-priority queue full. Failed to re-queue %s.", highPriorityAddr.Address)
							// Consider adding to a secondary, slightly lower priority "retry" queue if this happens often
						}
					})
				} else {
					// Non-retryable error or error we don't want to fast-retry for high-priority
					log.Printf("[ohlcv-loop] High-priority task for %s failed with non-retryable error (%v). Not re-queuing.", highPriorityAddr.Address, err)
				}
			}

		default:
			// LOW-PRIORITY PATH: No high-priority message is waiting.
			// Process the next address from our main sorted list.
			a := addrs[i]
			log.Printf("[ohlcv-loop] Processing regular cycle address %s (%d/%d)", a.Address, i+1, len(addrs))
			processSingleOhlcv(a)
			i++ // Increment the loop counter ONLY after a regular item is processed.
		}
	}
	log.Println("[ohlcv-loop] Main cycle finished. Awaiting any remaining high-priority tasks.")

	// Step 4: After the main loop, drain any remaining high-priority tasks that
	// might have arrived while the last regular item was processing.
	for {
		select {
		case highPriorityAddr := <-highPriorityOhlcvQueue:
			log.Printf("[ohlcv-loop] Draining remaining high-priority address %s", highPriorityAddr.Address)
			processSingleOhlcv(highPriorityAddr)
		default:
			// The queue is empty, we are done.
			log.Println("[ohlcv-loop] High-priority queue drained. Exiting.")
			return
		}
	}
}

/*
func FetchAndSaveOhlcv() {

	addrFile := filepath.Join("..", "addresses", "address.json")

	// read and parse
	data, err := ioutil.ReadFile(addrFile)
	if err != nil {
		log.Printf("[holders] could not read addresses file: %v", err)
		return
	}
	var addrs []Address
	if err := json.Unmarshal(data, &addrs); err != nil {
		log.Printf("[holders] invalid JSON in addresses file: %v", err)
		return
	}
	sort.Slice(addrs, func(i, j int) bool {
		// Handle cases where AddedAt might be nil to prevent panics.
		// A nil time is considered the "oldest".
		if addrs[i].AddedAt == nil {
			return false
		}
		if addrs[j].AddedAt == nil {
			return true
		}
		// Use .After() to sort the newest timestamps to the beginning of the slice.
		return addrs[i].AddedAt.After(*addrs[j].AddedAt)
	})
	log.Printf("Prioritized %d addresses for OHLCV fetch. Newest will be processed first.", len(addrs))
	//client := holders.NewBitqueryClient()
	//for _, a := range addrs {
	for i := len(addrs) - 1; i >= 0; i-- {
		a := addrs[i]
		if ExcludeAddrs[a.Address] {
			log.Printf("[holders] skipping excluded address: %s", a.Address)
			continue
		}
		fmt.Println("===== Getting ohlcv for", a.Address)
		// Step 1: Get pool ID
		geckoTerminalRequestLock()
		poolUrl := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/solana/tokens/%s/pools", a.Address)
		resp, err := http.Get(poolUrl)
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Printf("[holders] failed to fetch pool data for %s: %v", a.Address, err)
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var poolData PoolResponse
		if err := json.Unmarshal(body, &poolData); err != nil || len(poolData.Data) == 0 {
			log.Printf("[holders] no pool data found for %s: %v", a.Address, err)
			continue
		}

		rawPoolID := poolData.Data[0].ID
		poolID := strings.TrimPrefix(rawPoolID, "solana_")

		// Step 2: Get OHLCV
		geckoTerminalRequestLock()
		ohlcvUrl := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/solana/pools/%s/ohlcv/minute?aggregate=1&limit=1000&currency=USD", poolID)
		ohlcvResp, err := http.Get(ohlcvUrl)
		if err != nil || ohlcvResp.StatusCode != http.StatusOK {
			log.Printf("[holders] failed to fetch OHLCV data for %s: %v", a.Address, err)
			continue
		}
		defer ohlcvResp.Body.Close()

		ohlcvBody, _ := io.ReadAll(ohlcvResp.Body)

		// Parse the new OHLCV data
		var newOhlcv OhlcvResponse
		if err := json.Unmarshal(ohlcvBody, &newOhlcv); err != nil {
			log.Printf("[holders] failed to parse OHLCV JSON for %s: %v", a.Address, err)
			continue
		}

		// Add poolID to meta data
		// First, parse the current meta data
		var metaData map[string]interface{}
		if err := json.Unmarshal(newOhlcv.Meta, &metaData); err != nil {
			log.Printf("[holders] failed to parse meta data for %s: %v", a.Address, err)
			// Initialize empty meta if it doesn't exist or can't be parsed
			metaData = make(map[string]interface{})
		}

		// Add poolID to meta data
		metaData["poolID"] = poolID

		// Update the Meta field with the modified data including poolID
		updatedMeta, err := json.Marshal(metaData)
		if err != nil {
			log.Printf("[holders] failed to marshal updated meta data for %s: %v", a.Address, err)
		} else {
			newOhlcv.Meta = updatedMeta
		}

		// Step 3: Check if we already have data in Redis
		redisKey := fmt.Sprintf("ohlcv:%s", a.Address)
		existingData_, err := RedisClient.Get(redisKey).Result()

		if err == nil {
			// Data exists, we need to merge
			existingData, err := decompress([]byte(existingData_))
			var existingOhlcv OhlcvResponse
			if err := json.Unmarshal((existingData), &existingOhlcv); err != nil {
				log.Printf("[holders] failed to parse existing OHLCV JSON for %s: %v", a.Address, err)
				// If we can't parse existing data, just overwrite with new data
				newData, _ := json.Marshal(newOhlcv)
				compressedData, _ := compress(newData)
				RedisClient.Set(redisKey, compressedData, 0)
				continue
			}

			// Update the existing meta data with poolID
			var existingMetaData map[string]interface{}
			if err := json.Unmarshal(existingOhlcv.Meta, &existingMetaData); err != nil {
				// If we can't parse existing meta, initialize it
				existingMetaData = make(map[string]interface{})
			}

			// Add poolID to existing meta data
			existingMetaData["poolID"] = poolID

			// Update the Meta field with modified data
			updatedExistingMeta, err := json.Marshal(existingMetaData)
			if err != nil {
				log.Printf("[holders] failed to marshal updated existing meta data for %s: %v", a.Address, err)
			} else {
				existingOhlcv.Meta = updatedExistingMeta
			}

			// Create a map for existing data to filter out duplicates
			// We'll use timestamp (first element of each OHLCV array) as the key
			existingOhlcvMap := make(map[float64]bool)
			for _, ohlcv := range existingOhlcv.Data.Attributes.OhlcvList {
				if len(ohlcv) > 0 {
					existingOhlcvMap[ohlcv[0]] = true
				}
			}

			// Filter out duplicates from new data
			var uniqueNewOhlcv [][]float64
			for _, ohlcv := range newOhlcv.Data.Attributes.OhlcvList {
				if len(ohlcv) > 0 && !existingOhlcvMap[ohlcv[0]] {
					uniqueNewOhlcv = append(uniqueNewOhlcv, ohlcv)
				}
			}

			// Append unique new data to existing data
			existingOhlcv.Data.Attributes.OhlcvList = append(existingOhlcv.Data.Attributes.OhlcvList, uniqueNewOhlcv...)

			// Sort OHLCV candles from oldest to newest based on timestamp (element 0)
			ohlcvList := existingOhlcv.Data.Attributes.OhlcvList
			sort.Slice(ohlcvList, func(i, j int) bool {
				return ohlcvList[i][0] < ohlcvList[j][0]
			})
			allOHLCV := parseRawToOHLCV(ohlcvList)
			fmt.Println("Total OHLCV candles after merge:", len(allOHLCV))
			if len(allOHLCV) >= 10 {
				// Parameters for isDeadSlice
				windowSize := 10
				priceDropPct := 0.80
				stdDevPctThresh := 0.30
				rugDropPct := 0.70

				lastIdx := len(allOHLCV) - 1
				if isDeadSlice(allOHLCV, lastIdx, windowSize, priceDropPct, stdDevPctThresh, rugDropPct) {
					log.Printf("[holders] detected dead coin for %s (timestamp %d)\n", a.Address, allOHLCV[lastIdx].Timestamp)
					ExcludeAddrs[a.Address] = true
					if err := SaveExcludedAddresses(ExcludeAddrs, "../addresses/excluded.json"); err != nil {
						log.Printf("Error saving excluded addresses: %v", err)
					}
					continue
				}
			}

			// Convert back to JSON
			mergedData, err := json.Marshal(existingOhlcv)
			if err != nil {
				log.Printf("[holders] failed to marshal merged OHLCV data for %s: %v", a.Address, err)
				continue
			}

			// Save back to Redis
			compressedData, _ := compress(mergedData)
			err = RedisClient.Set(redisKey, compressedData, 0).Err()
			if err != nil {
				log.Printf("[holders] failed to save merged OHLCV to Redis for %s: %v", a.Address, err)
			}
		} else if err == redis.Nil {
			// No existing data, just save new data with the updated meta containing poolID
			newData, _ := json.Marshal(newOhlcv)
			compressedData, _ := compress(newData)
			err = RedisClient.Set(redisKey, compressedData, 0).Err()
			if err != nil {
				log.Printf("[holders] failed to save new OHLCV to Redis for %s: %v", a.Address, err)
			}
		} else {
			// Some other Redis error
			log.Printf("[holders] Redis error when checking for existing OHLCV data for %s: %v", a.Address, err)
			// Try to save new data anyway with the updated meta
			newData, _ := json.Marshal(newOhlcv)
			compressedData, _ := compress(newData)
			RedisClient.Set(redisKey, compressedData, 0)
		}

		//time.Sleep(6 * time.Second)
	}
}
*/
// In main.go, replace your old FetchAndSaveOhlcv with this one.

func isDeadSlice(
	data []OHLCV,
	endIdx, windowSize int,
	priceDropPct, stdDevPctThresh, rugDropPct float64,
) bool {
	if endIdx+1 < windowSize {
		log.Printf("Index %d: not enough data (need %d candles, have %d)\n",
			endIdx, windowSize, endIdx+1)
		return false
	}

	startIdx := endIdx - windowSize + 1
	window := data[:endIdx+1]

	// Rug detection
	for i, candle := range window {
		dropPct := (candle.Close - candle.Open) / candle.Open
		if dropPct <= -rugDropPct {
			log.Printf(
				"Index %d (window idx %d): Rug detected: open=%.8f, close=%.8f, drop=%.2f%% ≤ -%.2f%%\n",
				endIdx, startIdx+i, candle.Open, candle.Close, dropPct*100, rugDropPct*100,
			)
			return true
		}
	}

	// Dip 80% criterion
	firstClose := window[0].Close
	lastClose := window[endIdx].Close
	dipPct := (lastClose - firstClose) / firstClose
	if dipPct > -priceDropPct {
		log.Printf(
			"Index %d: Price dip not large enough: firstClose=%.8f, lastClose=%.8f, dip=%.2f%% > -%.2f%%\n",
			endIdx, firstClose, lastClose, dipPct*100, priceDropPct*100,
		)
		return false
	}
	log.Printf(
		"Index %d: Price dipped %.2f%% over window (≥ %.2f%% drop)\n",
		endIdx, dipPct*100, priceDropPct*100,
	)

	// Volatility check
	var sumClose float64
	for _, candle := range window[(len(window) - windowSize):] {
		sumClose += candle.Close
	}
	meanClose := sumClose / float64(len(window[(len(window)-windowSize):])) //float64(windowSize)
	//fmt.Println("meanClose:", window[(len(window)-windowSize):], "meanClose", meanClose)
	var sumSqDiff float64
	for _, candle := range window[(len(window) - windowSize):] {
		diff := candle.Close - meanClose
		sumSqDiff += (diff * diff)
	}
	stdDev := math.Sqrt(sumSqDiff / float64(windowSize))
	stdDevPct := stdDev / meanClose

	if stdDevPct > stdDevPctThresh {
		log.Printf(
			"Index %d: Volatility too high: stdDevPct=%.2f%% > %.2f%%\n",
			endIdx, stdDevPct*100, stdDevPctThresh*100,
		)
		return false
	}
	log.Printf(
		"Index %d: Volatility low: stdDevPct=%.2f%% ≤ %.2f%%\n",
		endIdx, stdDevPct*100, stdDevPctThresh*100,
	)

	return true
}
func SaveExcludedAddresses(excludeAddrs map[string]bool, filePath string) error {
	// Ensure parent directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	// Read existing addresses from file if it exists
	existingAddrs := make(map[string]bool)
	if data, err := os.ReadFile(filePath); err == nil {
		var existingList []string
		if err := json.Unmarshal(data, &existingList); err == nil {
			for _, addr := range existingList {
				existingAddrs[addr] = true
			}
		}
	}

	// Merge new addresses
	for addr := range excludeAddrs {
		existingAddrs[addr] = true
	}

	// Convert back to slice
	finalList := make([]string, 0, len(existingAddrs))
	for addr := range existingAddrs {
		finalList = append(finalList, addr)
	}

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(finalList, "", "  ")
	if err != nil {
		return err
	}

	// Write to file
	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return err
	}

	log.Printf("Excluded addresses saved to %s", filePath)
	return nil
}

var sortByLastChecked = true

type AddressTask struct {
	Address           Address
	AddedAt           time.Time
	LastChecked       time.Time
	UpdateAfter       time.Duration
	NextEligibleCheck time.Time
	Index             int // required by heap
}
type PriorityQueue []*AddressTask

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].NextEligibleCheck.Before(pq[j].NextEligibleCheck)
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	task := x.(*AddressTask)
	task.Index = len(*pq)
	*pq = append(*pq, task)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	task := old[n-1]
	old[n-1] = nil
	task.Index = -1
	*pq = old[:n-1]
	return task
}

var (
	pq         PriorityQueue
	pqMu       sync.Mutex
	requestCnt int
	lastReset  = time.Now()
)

func computeUpdateInterval(task *AddressTask, now time.Time) time.Duration {
	age := now.Sub(task.AddedAt)
	if age < 30*time.Minute {
		return 1 * time.Minute
	}
	return 2 * time.Minute
}
func loadAddressesFromFile() []Address {
	addrFile := filepath.Join("..", "addresses", "address.json")
	data, err := ioutil.ReadFile(addrFile)
	if err != nil {
		log.Printf("Failed to read address file: %v", err)
		return nil
	}
	var addrs []Address
	if err := json.Unmarshal(data, &addrs); err != nil {
		log.Printf("Failed to parse address file: %v", err)
		return nil
	}
	return addrs
}
func LoadExcludedAddresses(filePath string) error {
	// Check if file exists
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, nothing to load
			return nil
		}
		return err
	}

	// Unmarshal JSON data into a slice
	var addrList []string
	if err := json.Unmarshal(data, &addrList); err != nil {
		return err
	}
	//fmt.Println("addrList", addrList)
	// Fill the map
	for _, addr := range addrList {
		ExcludeAddrs[addr] = true
	}

	log.Printf("Loaded %d excluded addresses from %s", len(ExcludeAddrs), filePath)
	return nil
}
func refreshAddressListLoop() {
	for {
		newAddrs := loadAddressesFromFile()
		pqMu.Lock()
		existing := make(map[string]*AddressTask)
		for _, t := range pq {
			existing[t.Address.Address] = t
		}
		now := time.Now()
		for _, addr := range newAddrs {
			if ExcludeAddrs[addr.Address] {
				continue
			}
			if _, ok := existing[addr.Address]; !ok {

				task := &AddressTask{
					Address:           addr,
					AddedAt:           now,
					LastChecked:       time.Time{},
					UpdateAfter:       0,           //1 * time.Minute,
					NextEligibleCheck: time.Time{}, //now.Add(1 * time.Minute),
				}
				heap.Push(&pq, task)
			}
		}
		pqMu.Unlock()
		time.Sleep(2 * time.Second)
	}
}

// Load and parse JSON address file
func removeExcludedTasks() {
	pqMu.Lock()
	defer pqMu.Unlock()

	for i := 0; i < len(pq); {
		task := pq[i]
		if ExcludeAddrs[task.Address.Address] {
			heap.Remove(&pq, task.Index)
			// do not increment i since items shift
		} else {
			i++
		}
	}
}
func loadConfig(path string) (notify.NotifierConfig, error) {
	var config notify.NotifierConfig

	configFile, err := ioutil.ReadFile(path)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(configFile, &config)
	return config, err
}

// --- ADD THIS ENTIRE NEW FUNCTION ---

// worker is the background process that runs the expensive data fetching.
func worker(id int) {
	log.Printf("Starting holder data worker %d", id)
	for job := range jobQueue {
		log.Printf("Worker %d: processing job for address %s", id, job.Address)

		// We are now calling your existing function inside this controlled loop.
		fetchHoldersForAllAddresses(job)

		log.Printf("Worker %d: finished job for address %s", id, job.Address)
		// Suggest a garbage collection to free up memory before the next job.
		runtime.GC()
	}
}
func startRateLimitedAddressUpdater() {
	go func() {
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for range ticker.C {
			removeExcludedTasks()
			pqMu.Lock()
			if time.Since(lastReset) >= time.Minute {
				requestCnt = 0
				lastReset = time.Now()
			}

			maxPerTick := int(math.Max(1, float64(apiLimitPerMinute)/float64(time.Minute/tickInterval)))
			updatesDone := 0
			now := time.Now()

			for pq.Len() > 0 && updatesDone < maxPerTick && requestCnt < apiLimitPerMinute {
				task := pq[0]
				if now.Before(task.NextEligibleCheck) {
					break
				}
				heap.Pop(&pq)
				//go fetchHoldersForAllAddresses(task.Address)
				// --- REPLACEMENT LOGIC ---
				// Instead of starting an uncontrolled goroutine, send a job to the queue.
				select {
				case jobQueue <- task.Address:
					// Successfully sent job to a worker.
				default:
					log.Printf("Warning: Holder update job queue is full. Dropping task for %s", task.Address.Address)
				}
				// --- END OF REPLACEMENT ---
				//var config notify.NotifierConfig
				task.LastChecked = now
				task.UpdateAfter = computeUpdateInterval(task, now)
				task.NextEligibleCheck = now.Add(task.UpdateAfter)
				heap.Push(&pq, task)
				requestCnt++
				updatesDone++
			}
			pqMu.Unlock()
		}
	}()
}

func fetchHoldersForAllAddresses(a Address) {

	//fmt.Println("==========   Getting Holder Info for", a.Address, "last", a.LastActivity)
	if ExcludeAddrs[a.Address] {
		log.Printf("[holders] skipping excluded address: %s", a.Address)
		return
	}
	fmt.Println("===Getting Holder Info for", a.Address, "last", a.LastActivity, "Excluded Address")
	client := holders.NewBitqueryClient()
	// get transfers for this mint/address
	//fmt.Println("Getting Holder Info for", a.Address)
	geckoTerminalRequestLock() // <<< ADD THIS (Token 1)
	geckoTerminalRequestLock()
	output, err := client.UpdateAndGetTransfers(RedisClient, a.Address)
	if err != nil {
		log.Printf("[holders] error fetching for %s: %v", a.Address, err)
		return
	}
	// optionally unmarshal and log length
	var transfers []Transfer
	if err := json.Unmarshal([]byte(output), &transfers); err != nil {
		log.Printf("[holders] invalid JSON for %s: %v", a.Address, err)
	} else {
		log.Printf("[holders] fetched %d transfers for %s", len(transfers), a.Address)
	}
	// --- START OF FIX ---
	// If there are no new transfers, there's nothing to update. Exit early.
	if len(transfers) == 0 {
		log.Printf("[holders] No new transfers found for %s. Skipping update.", a.Address)
		return
	}
	// --- END OF FIX ---
	// store raw JSON in Redis under "holders:<mint>"

	key := fmt.Sprintf("holderhistory:%s", a.Address)

	// Check if the key exists
	exists, err := RedisClient.Exists(key).Result()
	if err != nil {
		log.Printf("[redis] error checking existence of key %s: %v", key, err)
		return
	}

	var tokenData TokenData
	if exists == 0 {
		// Key does not exist, create new TokenData
		tokenData = TokenData{}
	} else {
		// Key exists, retrieve existing data
		res_, err := RedisClient.Get(key).Result() //rh.JSONGet(key, ".")
		if err != nil {
			log.Printf("[redis] error getting JSON for key %s: %v", key, err)
			return
		} /*
			res, _ := decompress([]byte(res_))
			if err := json.Unmarshal((res), &tokenData); err != nil {
				log.Printf("[json] error unmarshaling data for key %s: %v", key, err)
				return
			}*/
		res, err := decompress([]byte(res_))
		if err != nil {
			log.Printf("[decompress] error decompressing key %s: %v", key, err)
			return
		}

		// Use a streamed decoder to avoid high memory usage
		var holdersWrapper struct {
			Holders []json.RawMessage `json:"holders"`
		}
		if err := json.Unmarshal(res, &holdersWrapper); err != nil {
			log.Printf("[json] error unmarshaling holders: %v", err)
			return
		}

		for _, raw := range holdersWrapper.Holders {
			var h HolderData
			if err := json.Unmarshal(raw, &h); err != nil {
				log.Printf("[json] invalid holder: %v", err)
				continue
			}
			tokenData.Holders = append(tokenData.Holders, h)
		}
	}

	for _, t := range transfers {
		// Find if holder already exists
		found := false
		for i, holder := range tokenData.Holders {
			if holder.Address == t.Address {
				// Append new amount and time
				tokenData.Holders[i].Amount = append(tokenData.Holders[i].Amount, t.Amount)
				tokenData.Holders[i].Time = append(tokenData.Holders[i].Time, time.Now().Format(time.RFC3339))
				found = true
				break
			}
		}
		if !found {
			// Add new holder
			newHolder := HolderData{
				Address: t.Address,
				Price:   t.Price,
				Amount:  []float64{t.Amount},
				Time:    []string{time.Now().Format(time.RFC3339)},
			}
			tokenData.Holders = append(tokenData.Holders, newHolder)
		}
	}

	// Set the updated TokenData back to Redis
	jsonData, err := json.Marshal(tokenData)
	if err != nil {
		log.Printf("failed to marshal snapshots to JSON: %v", err)
	}

	fmt.Println("=== Savedm  ====", a.Address, "key", key)
	compressedData, _ := compress(jsonData)
	if err := RedisClient.Set(key, compressedData, 0).Err(); err != nil {
		log.Printf("failed to store snapshots in Redis: %v", err)
	}
	//time.Sleep(6 * time.Second)
	//}
}

// periodicHolderUpdater starts a ticker that fires every interval d.
func periodicHolderUpdater(d time.Duration) {
	fmt.Println("======== Getting Holders ============")
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	LoadExcludedAddresses("../addresses/excluded.json")
	go watchForNewAddressesAndQueueOhlcv()
	FetchAndSaveOhlcv()
	// run once immediately
	go refreshAddressListLoop() // Reload addresses every 2 mins
	go startRateLimitedAddressUpdater()
	//fetchHoldersForAllAddresses()
	fmt.Println("============ RERD ========")
	//FetchAndSaveOhlcv()
	//refreshAddressListLoop()
	for i := 1; i <= MAX_CONCURRENT_FETCHES; i++ {
		go worker(i)
	}
	for range ticker.C {
		//fetchHoldersForAllAddresses()
		FetchAndSaveOhlcv()
		//refreshAddressListLoop()
	}
}

type HolderSnapshot struct {
	Holders int    `json:"holders"`
	Time    string `json:"time"`
}

// getHolderSnapshotsHandler handles HTTP requests to retrieve holder snapshots.
func getHolderSnapshotsHandler(w http.ResponseWriter, r *http.Request) {
	// Extract the token address from query parameters
	tokenAddress := r.URL.Query().Get("address")
	if tokenAddress == "" {
		http.Error(w, "Missing 'address' query parameter", http.StatusBadRequest)
		return
	}

	// Construct the Redis key
	redisKey := fmt.Sprintf("token:%s:holdersplot", tokenAddress)

	// Retrieve the JSON array from Redis
	val_, err := RedisClient.Get(redisKey).Result()
	if err == redis.Nil {
		http.Error(w, "No data found for the given address", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, fmt.Sprintf("Error retrieving data from Redis: %v", err), http.StatusInternalServerError)
		return
	}
	val, _ := decompress([]byte(val_))
	// Unmarshal the JSON array into a slice of HolderSnapshot
	var snapshots []HolderSnapshot
	if err := json.Unmarshal((val), &snapshots); err != nil {
		http.Error(w, fmt.Sprintf("Error unmarshaling JSON: %v", err), http.StatusInternalServerError)
		return
	}

	// Set response headers and write the JSON response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(snapshots); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding JSON response: %v", err), http.StatusInternalServerError)
		return
	}
}

func getSavedOhlcvHandler(w http.ResponseWriter, r *http.Request) {
	// Allow only GET method
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameter
	poolId := r.URL.Query().Get("poolId")
	if poolId == "" {
		http.Error(w, "Missing poolId parameter", http.StatusBadRequest)
		return
	}

	// Redis key format
	redisKey := fmt.Sprintf("ohlcv:%s", poolId)

	// Fetch from Redis
	val_, err := RedisClient.Get(redisKey).Result()
	if err == redis.Nil {
		http.Error(w, "No data found for this poolId", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Failed to retrieve data from Redis", http.StatusInternalServerError)
		return
	}
	val, _ := decompress([]byte(val_))
	// Set JSON header
	w.Header().Set("Content-Type", "application/json")
	w.Write((val))
}

// --- NEW CODE: CENTRALIZED GECKOTERMINAL RATE LIMITER ---

// geckoTerminalRateLimiter is our token bucket. Any function wanting to make a
// GeckoTerminal API call must take a token from this channel first.
var geckoTerminalRateLimiter chan struct{}

// startGeckoTerminalRateLimiter starts a background goroutine to fill the token bucket
// at the specified rate. It should be called once from main().
func startGeckoTerminalRateLimiter() {
	// Create a buffered channel to act as the bucket.
	geckoTerminalRateLimiter = make(chan struct{}, GeckoTerminalRateLimit)

	// Initially fill the bucket so we can make requests immediately.
	for i := 0; i < GeckoTerminalRateLimit; i++ {
		geckoTerminalRateLimiter <- struct{}{}
	}

	// Calculate the interval at which we should add a new token.
	// e.g., 60 seconds / 30 requests = 2 seconds per request.
	fillInterval := GeckoTerminalRateLimitPeriod / time.Duration(GeckoTerminalRateLimit)
	ticker := time.NewTicker(fillInterval)

	go func() {
		for range ticker.C {
			// Add a token to the bucket. If the bucket is full, this will
			// block until a token is consumed, which is the desired behavior.
			// To avoid blocking the ticker forever if nothing is consumed,
			// we use a non-blocking send.
			select {
			case geckoTerminalRateLimiter <- struct{}{}:
				// Token added
			default:
				// Bucket is full, do nothing.
			}
		}
	}()

	log.Printf("Centralized GeckoTerminal rate limiter started. Rate: %d req/min. Interval: %v", GeckoTerminalRateLimit, fillInterval)
}

// geckoTerminalRequestLock blocks until a request token is available from the bucket.
// Call this function BEFORE every single GeckoTerminal API call.
func geckoTerminalRequestLock() {
	// This will block until a token is available in the channel.
	<-geckoTerminalRateLimiter
	log.Println("[RATE_LIMITER] Token acquired. Proceeding with API request.")
}

// --- END OF NEW CODE ---
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			// Client does not accept gzip
			next.ServeHTTP(w, r)
			return
		}

		// Set the gzip header
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()

		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		next.ServeHTTP(gzw, r)
	})
}

type gzipResponseWriter struct {
	http.ResponseWriter
	Writer *gzip.Writer
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func getHolderHistoryHandler(w http.ResponseWriter, r *http.Request) {
	tokenAddress := r.URL.Query().Get("address")
	if tokenAddress == "" {
		http.Error(w, "Missing 'address' query parameter", http.StatusBadRequest)
		return
	}

	redisKey := fmt.Sprintf("holderhistory:%s", tokenAddress)
	val_, err := RedisClient.Get(redisKey).Result()
	if err == redis.Nil {
		http.Error(w, "No holder history found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Redis error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	val, _ := decompress([]byte(val_))
	// Stream JSON response
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Start streaming manually
	w.Write([]byte(`{"holders":[`))

	// Decode manually to avoid loading entire data into memory
	decoder := json.NewDecoder(strings.NewReader(string(val)))
	var raw map[string][]map[string]interface{}
	if err := decoder.Decode(&raw); err != nil {
		http.Error(w, "JSON decoding failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	holders := raw["holders"]
	for i, holder := range holders {
		if i != 0 {
			w.Write([]byte(",")) // comma between items
		}
		enc := json.NewEncoder(w)
		if err := enc.Encode(holder); err != nil {
			log.Printf("error encoding holder: %v", err)
			break
		}
		// Flush buffer to client
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}

	w.Write([]byte(`]}`)) // close JSON
}
func notiFy() {
	log.Println("Starting notification service...")

	// 1. Load configuration from the file
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("FATAL: Could not load configuration from config.json: %v", err)
	}

	// 2. Create a new Notifier instance with the loaded config
	notifier := notify.NewNotifier(config)

	// 3. Load the addresses to process
	addrFile := filepath.Join("..", "addresses", "address.json") // Corrected path
	data, err := ioutil.ReadFile(addrFile)
	if err != nil {
		log.Fatalf("[main] could not read addresses file: %v", err)
	}

	var addrs []notify.Address
	if err := json.Unmarshal(data, &addrs); err != nil {
		log.Fatalf("[main] invalid JSON in addresses file: %v", err)
	}
	excaddr := filepath.Join("..", "addresses", "excluded.json") // Corrected path
	notifier.LoadExcludedAddresses(excaddr)
	// 4. Run the processing logic
	ticker := time.NewTicker(1 * time.Minute)
	// For continuous monitoring, you could wrap this in a time.Ticker loop.
	defer ticker.Stop()
	for range ticker.C {
		notifier.LoadExcludedAddresses(excaddr)
		notifier.ProcessAddresses(addrs)

	}
}

func memoryProfilerMiddleware(next http.HandlerFunc, label string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var mBefore, mAfter runtime.MemStats
		runtime.GC() // Clean up before measuring
		runtime.ReadMemStats(&mBefore)

		peak := mBefore.Alloc
		stop := make(chan struct{})

		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					if m.Alloc > peak {
						peak = m.Alloc
					}
				case <-stop:
					return
				}
			}
		}()

		start := time.Now()
		next(w, r)
		elapsed := time.Since(start)

		close(stop)
		runtime.ReadMemStats(&mAfter)

		log.Printf("[MEM] %s | RAM Before: %.2f MB | After: %.2f MB | Peak: %.2f MB | Duration: %s",
			label,
			float64(mBefore.Alloc)/1024/1024,
			float64(mAfter.Alloc)/1024/1024,
			float64(peak)/1024/1024,
			elapsed,
		)
	}
}
func profileMemoryTimed(fn func(), label string, duration time.Duration) {
	var mBefore, mAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mBefore)

	peak := mBefore.Alloc
	stop := make(chan struct{})

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.Alloc > peak {
					peak = m.Alloc
				}
			case <-stop:
				return
			}
		}
	}()

	go fn() // Start the long-running function in background
	time.Sleep(duration)
	close(stop)
	runtime.ReadMemStats(&mAfter)

	log.Printf("[RAM] %s | Before: %.2f MB | After: %.2f MB | Peak: %.2f MB | Elapsed: %s",
		label,
		float64(mBefore.Alloc)/1024/1024,
		float64(mAfter.Alloc)/1024/1024,
		float64(peak)/1024/1024,
		duration,
	)
}

// ----------------------
// Main Function
// ----------------------

func main() {
	// Start migration subscription in a goroutine.
	/*go subscribeMigration()
	go periodicHolderUpdater(2 * time.Minute)
	go notiFy()*/
	startGeckoTerminalRateLimiter()
	profileMemoryTimed(subscribeMigration, "subscribeMigration", 10*time.Second)
	profileMemoryTimed(func() { periodicHolderUpdater(5 * time.Minute) }, "periodicHolderUpdater", 10*time.Second)
	profileMemoryTimed(notiFy, "notiFy", 10*time.Second)

	go func() {
		// Create a new ServeMux (router) specifically for pprof.
		pprofMux := http.NewServeMux()

		// The import `_ "net/http/pprof"` registers handlers on the DefaultServeMux.
		// To make them available on our new mux, we need to explicitly register them.
		// The simplest way is to register the main pprof handler.
		pprofMux.HandleFunc("/debug/pprof/", http.DefaultServeMux.ServeHTTP)
		pprofMux.HandleFunc("/debug/pprof/cmdline", http.DefaultServeMux.ServeHTTP)
		pprofMux.HandleFunc("/debug/pprof/profile", http.DefaultServeMux.ServeHTTP)
		pprofMux.HandleFunc("/debug/pprof/symbol", http.DefaultServeMux.ServeHTTP)
		pprofMux.HandleFunc("/debug/pprof/trace", http.DefaultServeMux.ServeHTTP)

		log.Println("Starting pprof server on :6060")
		// Pass our new pprofMux to the server instead of `nil`.
		// Also, bind to ":6060" to allow external access.
		if err := http.ListenAndServe(":6060", pprofMux); err != nil {
			log.Fatalf("pprof server failed to start: %v", err)
		}
	}()

	holder_flow := flow.NewFlowAnalyticsService(RedisClient)
	//holder_flow.StartWorkers()
	holder_snapshot := snapshot.NewHolderSnapshotService(RedisClient)
	holder_srs := srs.NewSrsService(RedisClient)
	bqClient := holders.NewBitqueryClient()
	aggregateSvc := aggregate.NewAggregateService(RedisClient, bqClient)
	holderhistory := holderhistory.NewHandler(RedisClient) //NewHolderHistoryService(RedisClient, aggregateSvc)
	//Api links
	http.HandleFunc("/api/flow-analytics", memoryProfilerMiddleware(withCORS(holder_flow.HandleFlowAnalytics), "FlowAnalytics"))
	http.HandleFunc("/api/holder_snapshots", memoryProfilerMiddleware(withCORS(holder_snapshot.HandleGetHolderSnapshots), "HolderSnapshots"))
	http.HandleFunc("/api/holder_srs", memoryProfilerMiddleware(withCORS(holder_srs.HandleSrsRequest), "HolderSRS"))
	http.HandleFunc("/api/holder-aggregated", memoryProfilerMiddleware(withCORS(aggregateSvc.HandleAggregateRequest), "HolderAggregated"))
	http.HandleFunc("/api/holder_history", memoryProfilerMiddleware(withCORS(holderhistory.GetHistory), "HolderHistory"))

	http.HandleFunc("/saveData", withCORS(saveDataHandler))
	http.HandleFunc("/tweet", withCORS(tweetHandler))
	http.HandleFunc("/tweeturl", withCORS(tweetURLHandler))
	http.HandleFunc("/api/token-metadata", memoryProfilerMiddleware(withCORS(getTokenMetadata), "GetTokenMetadata"))
	http.HandleFunc("/fetch-data", memoryProfilerMiddleware(withCORS(fetchKeysAndDataHandler), "FetchKeysAndData"))
	http.HandleFunc("/fetch-holders", memoryProfilerMiddleware(withCORS(fetchHolderKeysAndDataHandler), "FetchHolderKeysAndData"))
	http.HandleFunc("/add-proxies", withCORS(addProxiesHandler))
	http.HandleFunc("/add-cookies", withCORS(addCookiesHandler))
	http.HandleFunc("/add-address", withCORS(addAddressHandler))
	http.HandleFunc("/session-status", withCORS(readSessionStatusHandler))
	//http.HandleFunc("/api/holder-history", withCORS(getHolderHistoryHandler))
	http.Handle("/api/holder-history", gzipMiddleware(http.HandlerFunc(withCORS(getHolderHistoryHandler))))

	http.HandleFunc("/api/holder-snapshots", withCORS(getHolderSnapshotsHandler))
	http.HandleFunc("/get-ohlcv", memoryProfilerMiddleware(withCORS(getSavedOhlcvHandler), "GetSavedOHLCV"))
	staticPath := filepath.Join("..", "static")
	fs := http.FileServer(http.Dir(staticPath))
	http.Handle("/static/", withCORSStat(http.StripPrefix("/static/", fs)))

	addressPath := filepath.Join("..", "addresses")
	fs_addr := http.FileServer(http.Dir(addressPath))
	http.Handle("/addresses/", withCORSStat(http.StripPrefix("/addresses/", fs_addr)))

	slptokenPath := filepath.Join("..", "spltoken")
	fs_slptkn := http.FileServer(http.Dir(slptokenPath))
	http.Handle("/spltoken/", withCORSStat(http.StripPrefix("/spltoken/", fs_slptkn)))

	fmt.Println("Server running on http://localhost:3300")
	http.ListenAndServe(":3300", nil)
}
