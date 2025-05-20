package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blocto/solana-go-sdk/common"
	"github.com/blocto/solana-go-sdk/program/metaplex/token_metadata"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/go-redis/redis"
	holders "github.com/thankgod20/scraperServer/Holders"
)

const (
	filePath_      = "extractedData.json"
	staticFolder   = "/static"
	tokenFolder    = "/spltoken"
	contentTypeKey = "Content-Type"
	contentTypeVal = "application/json"
)

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

// ----------------------
// Structures for new endpoints
// ----------------------

type Address struct {
	Address string `json:"address"`
	Name    string `json:"name"`
	Symbol  string `json:"symbol"`
	Index   int64  `json:"index"`
}
type Cookies struct {
	Address string `json:"cookies"`
}
type Proxy struct {
	Address string `json:"proxies"`
}
type TokenMetadata struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
	URI    string `json:"uri"`
}

var RedisClient *redis.Client

//var ctx = context.Background()

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

// Handler to fetch keys and data
func fetchHolderKeysAndDataHandler(w http.ResponseWriter, r *http.Request) {
	searchWord := r.URL.Query().Get("search") //"4x77NhFuVzWWDGEMUyB17e3nhvVdkV7HT2AZNmz6pump"
	pattern := fmt.Sprintf("%s", searchWord)
	fmt.Println("Partern", pattern)

	// Create a new BitqueryClient.
	client := holders.NewBitqueryClient()

	// Fetch transfer data, update the JSON file, and get its content.
	output, err := client.UpdateAndGetTransfers(pattern)
	if err != nil {
		log.Fatalf("Error updating and getting transfers: %v", err)
	}
	var transfers []Transfer
	if err := json.Unmarshal([]byte(output), &transfers); err != nil {
		log.Fatalf("Error decoding transfers: %v", err)
	}
	// If no values were found, return an appropriate response
	if len(transfers) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "No matching keys found"})
		return
	}

	// Send the response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(transfers); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// Handler to fetch keys and data
func fetchKeysAndDataHandler(w http.ResponseWriter, r *http.Request) {
	searchWord := r.URL.Query().Get("search") //"4x77NhFuVzWWDGEMUyB17e3nhvVdkV7HT2AZNmz6pump"
	pattern := fmt.Sprintf("*spltoken:%s*", searchWord)

	// Get all keys matching the pattern
	keys, err := RedisClient.Keys(pattern).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to retrieve keys: %v", err), http.StatusInternalServerError)
		return
	}

	// Retrieve the values for each key
	var values []interface{}
	for _, key := range keys {
		value, err := RedisClient.Get(key).Result()
		if err != nil {
			log.Printf("Error retrieving value for key %s: %v", key, err)
			continue
		}
		var rawValue interface{}
		if err := json.Unmarshal([]byte(value), &rawValue); err == nil {
			// If JSON decoding succeeds, use the decoded value
			values = append(values, rawValue)
		} else {
			// Otherwise, use the raw value
			cleanedValue := strings.ReplaceAll(value, "\\\"", "\"")

			// Append the cleaned value
			values = append(values, cleanedValue)
		}

	}

	// If no values were found, return an appropriate response
	if len(values) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "No matching keys found"})
		return
	}

	// Send the response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(values); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}
func main() {
	// Initialize server routes with CORS handling

	http.HandleFunc("/saveData", withCORS(saveDataHandler))
	http.HandleFunc("/tweet", withCORS(tweetHandler))
	http.HandleFunc("/tweeturl", withCORS(tweetURLHandler))
	http.HandleFunc("/api/token-metadata", withCORS(getTokenMetadata))

	http.HandleFunc("/fetch-data", withCORS(fetchKeysAndDataHandler))
	http.HandleFunc("/fetch-holders", withCORS(fetchHolderKeysAndDataHandler))

	// New endpoints:
	http.HandleFunc("/add-proxies", withCORS(addProxiesHandler))
	http.HandleFunc("/add-cookies", withCORS(addCookiesHandler))
	http.HandleFunc("/add-address", withCORS(addAddressHandler))
	http.HandleFunc("/session-status", withCORS(readSessionStatusHandler))

	staticPath := filepath.Join("..", "static")
	fs := http.FileServer(http.Dir(staticPath))
	http.Handle("/static/", withCORSStat(http.StripPrefix("/static/", fs)))

	//AddressPath
	addressPath := filepath.Join("..", "addresses")
	fs_addr := http.FileServer(http.Dir(addressPath))
	http.Handle("/addresses/", withCORSStat(http.StripPrefix("/addresses/", fs_addr)))
	//SplToken
	slptokenPath := filepath.Join("..", "spltoken")
	fs_slptkn := http.FileServer(http.Dir(slptokenPath))
	http.Handle("/spltoken/", withCORSStat(http.StripPrefix("/spltoken/", fs_slptkn)))
	// Start the server
	fmt.Println("Server running on http://localhost:3300")
	http.ListenAndServe(":3300", nil)
}

// Middleware to set headers
func setHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set(contentTypeKey, contentTypeVal)
}

// withCORS adds CORS headers and handles preflight requests
func withCORS(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the original handler
		handler(w, r)
	}
}
func withCORSStat(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Serve the request
		handler.ServeHTTP(w, r)
	})
}
func getTokenMetadata(w http.ResponseWriter, r *http.Request) {
	// Extract the mint address from the URL query parameters
	mintAddress := r.URL.Query().Get("mint")
	if mintAddress == "" {
		http.Error(w, "Missing 'mint' query parameter", http.StatusBadRequest)
		return
	}

	// Convert the mint address string to a Solana PublicKey
	mintPubKey, err := solana.PublicKeyFromBase58(mintAddress)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid mint address: %v", err), http.StatusBadRequest)
		return
	}

	// Initialize the Solana RPC client
	client := rpc.New(rpc.MainNetBeta_RPC)
	commonMintPubKey := common.PublicKeyFromBytes(mintPubKey.Bytes())
	// Derive the metadata account public key associated with the mint address
	metadataPubKey, err := token_metadata.GetTokenMetaPubkey(commonMintPubKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to derive metadata public key: %v", err), http.StatusInternalServerError)
		return
	}

	// Fetch the metadata account information
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
	// Deserialize the metadata account data
	metadata, err := token_metadata.MetadataDeserialize(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to deserialize metadata: %v", err), http.StatusInternalServerError)
		return
	}

	// Prepare the response with the desired metadata fields
	response := NFTMetadata{
		Name:   metadata.Data.Name,
		Symbol: metadata.Data.Symbol,
		URI:    metadata.Data.Uri,
	}

	// Set the response header and write the JSON response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// saveDataHandler saves or updates token data
func saveDataHandler(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)

	if r.Method != http.MethodPost {
		fmt.Println("Invalid request method")
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var newData map[string]map[string]interface{} //map[string]map[string][]string

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Unable to read request body")
		http.Error(w, "Unable to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	fmt.Println("Raw body:", string(body))
	if err := json.Unmarshal(body, &newData); err != nil {
		fmt.Println("Invalid JSON")
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	rootDir := ensureFolder(staticFolder)
	existingData := make(map[string]map[string]interface{}) //make(map[string]map[string][]string)
	filePath := filepath.Join(rootDir, filePath_)
	// Load existing data
	if _, err := os.Stat(filePath); err == nil {
		data, err := ioutil.ReadFile(filePath)
		if err == nil {
			json.Unmarshal(data, &existingData)
		}
	}

	// Merge new data with existing data
	for tokenAddress, tokenData := range newData {
		fmt.Println("tokenAddress", tokenAddress, "tokenData", tokenData)
		if _, exists := existingData[tokenAddress]; !exists {
			existingData[tokenAddress] = tokenData
		}
	}

	// Write updated data to file
	data, _ := json.MarshalIndent(existingData, "", "  ")
	if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
		http.Error(w, "Error saving data", http.StatusInternalServerError)
		return
	}

	w.Write([]byte("Data saved successfully"))
}

// tweetHandler processes incoming tweet data
func tweetHandler(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)

	if r.Method != http.MethodPost {
		fmt.Println("Invalid request method")
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Unable to read request body")
		http.Error(w, "Unable to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	fmt.Println("Recieveing", string(body))
	var requestData map[string]map[string]interface{}
	if err := json.Unmarshal(body, &requestData); err != nil {
		fmt.Println("Invalid JSON")
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
		// Construct and save the JSON in the desired format
		tweetContent, ok := data["tweet"].(string)
		if !ok || tweetContent == "" {
			fmt.Println("Missing or invalid tweet content")
			continue
		}

		if _, exists := fileData[tweetURL]; !exists {
			//fileData[tweetURL] = data["tweet"]
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

// tweetURLHandler processes tweet URL data
func tweetURLHandler(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)

	if r.Method != http.MethodPost {
		fmt.Println("Invalid request method")
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)

		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Unable to read request body")
		http.Error(w, "Unable to read request body", http.StatusInternalServerError)

		return
	}
	defer r.Body.Close()

	if strings.TrimSpace(string(body)) == "" {
		fmt.Println("Request body is empty")
		http.Error(w, "Request body is empty", http.StatusBadRequest)

		return
	}

	w.Write([]byte(`{"message": "Data saved successfully"}`))
}

// ensureFolder ensures the folder exists, creates if not
func ensureFolder(folder string) string {
	// Construct the path to the root directory relative to the current file
	rootDir := filepath.Join("..", folder) // ".." moves up one level from the current folder

	// Create the folder in the root directory
	if err := os.MkdirAll(rootDir, os.ModePerm); err != nil {
		fmt.Println(err)

	}

	//fmt.Println("Folder created at:", rootDir)
	/*
		if _, err := os.Stat(folder); os.IsNotExist(err) {
			os.MkdirAll(folder, 0755)
		}*/
	return rootDir
}

// contains checks if a slice contains a string
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
	// Call your token metadata endpoint (adjust the URL if needed)
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
// New E
// ----------------------
// New Endpoints: Add Proxies, Cookies, and Addresses
// ----------------------

// /add-proxies expects: {"proxies": ["IP:PORT", "IP:PORT", ...]}

// addProxiesHandler serves as a single-page interface to manage proxies.
func addProxiesHandler(w http.ResponseWriter, r *http.Request) {
	outputPath := filepath.Join("..", "datacenter", "proxies.json")
	var proxies []Proxy
	// Read current proxies if the file exists.
	if data, err := ioutil.ReadFile(outputPath); err == nil {
		json.Unmarshal(data, &proxies)
	}

	if r.Method == http.MethodPost {
		// Process form submission based on the "action" field.
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}
		action := r.FormValue("action")
		switch action {
		case "add":
			// Add new proxies from the textarea.
			newProxiesText := r.FormValue("newProxies")
			lines := strings.Split(newProxiesText, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					proxies = append(proxies, Proxy{Address: line})
				}
			}
		case "edit":
			// Edit a proxy at a specific index.
			indexStr := r.FormValue("index")
			newProxyValue := strings.TrimSpace(r.FormValue("proxy"))
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(proxies) && newProxyValue != "" {
				proxies[index].Address = newProxyValue
			}
		case "delete":
			// Delete a proxy at a specific index.
			indexStr := r.FormValue("index")
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(proxies) {
				proxies = append(proxies[:index], proxies[index+1:]...)
			}
		default:
			http.Error(w, "Unknown action", http.StatusBadRequest)
			return
		}
		// Write the updated proxies list back to the JSON file.
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

	// For GET requests, render the HTML page with the add/edit/delete interface.
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
	// Dynamically generate a table row for each proxy.
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
	// Load existing cookies from file (if exists).
	if data, err := ioutil.ReadFile(outputPath); err == nil {
		json.Unmarshal(data, &cookies)
	}

	if r.Method == http.MethodPost {
		// Process form submission.
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}
		action := r.FormValue("action")
		switch action {
		case "add":
			// Add new cookies from textarea.
			newCookiesText := r.FormValue("newCookies")
			lines := strings.Split(newCookiesText, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					cookies = append(cookies, Cookies{Address: line})
				}
			}
		case "edit":
			// Edit a cookie at a given index.
			indexStr := r.FormValue("index")
			newCookie := strings.TrimSpace(r.FormValue("cookie"))
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(cookies) && newCookie != "" {
				cookies[index].Address = newCookie
			}
		case "delete":
			// Delete a cookie at the given index.
			indexStr := r.FormValue("index")
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(cookies) {
				cookies = append(cookies[:index], cookies[index+1:]...)
			}
		default:
			http.Error(w, "Unknown action", http.StatusBadRequest)
			return
		}
		// Write the updated cookies list back to the JSON file.
		os.MkdirAll(filepath.Dir(outputPath), os.ModePerm)
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
		http.Redirect(w, r, "/add-cookies", http.StatusSeeOther)
		return
	}

	// For GET: Render the HTML page.
	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Manage Cookies</title>
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
  <h1>Manage Cookies</h1>
  <h2>Add New Cookies</h2>
  <form method="POST" action="/add-cookies">
    <textarea name="newCookies" placeholder="Enter cookies, one per line"></textarea><br>
    <input type="hidden" name="action" value="add">
    <button type="submit">Add Cookies</button>
  </form>
  <h2>Current Cookies</h2>
  <table>
    <tr>
      <th>#</th>
      <th>Cookie</th>
      <th>Actions</th>
    </tr>`
	for i, cookie := range cookies {
		html += fmt.Sprintf(`<tr>
      <td>%d</td>
      <td>
        <form method="POST" action="/add-cookies" class="inline">
          <input type="text" name="cookie" value="%s" required>
          <input type="hidden" name="index" value="%d">
          <input type="hidden" name="action" value="edit">
          <button type="submit">Update</button>
        </form>
      </td>
      <td>
        <form method="POST" action="/add-cookies" class="inline">
          <input type="hidden" name="index" value="%d">
          <input type="hidden" name="action" value="delete">
          <button type="submit" onclick="return confirm('Delete this cookie?');">Delete</button>
        </form>
      </td>
    </tr>`, i, cookie.Address, i, i)
	}
	html += `</table>
</body>
</html>`
	w.Write([]byte(html))
}

// /add-address expects: {"addresses": [{"address": "mintAddress", "index": 1}, ...]}
func addAddressHandler(w http.ResponseWriter, r *http.Request) {
	outputPath := filepath.Join("..", "addresses", "address.json")
	var addresses []Address
	// Load existing addresses from file (if exists).
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
			// Add new address: fetch metadata and append.
			newAddress := strings.TrimSpace(r.FormValue("newAddress"))
			newIndexStr := strings.TrimSpace(r.FormValue("newIndex"))
			if newAddress != "" && newIndexStr != "" {
				newIndex, err := strconv.ParseInt(newIndexStr, 10, 64)
				if err == nil {
					metadata, err := getTokenMetadataFromAPI(newAddress)
					if err != nil {
						// In case of error, you can choose to set empty metadata.
						metadata = &TokenMetadata{Name: "", Symbol: "", URI: ""}
					}
					addresses = append(addresses, Address{
						Address: newAddress,
						Name:    metadata.Name,
						Symbol:  metadata.Symbol,
						Index:   newIndex,
					})
				}
			}
		case "edit":
			// Edit an existing address: update the mint address and index and re-fetch metadata.
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
					addresses[row] = Address{
						Address: newAddress,
						Name:    metadata.Name,
						Symbol:  metadata.Symbol,
						Index:   newIndex,
					}
				}
			}
		case "delete":
			// Delete an address at the specified row.
			rowStr := r.FormValue("row")
			row, err := strconv.Atoi(rowStr)
			if err == nil && row >= 0 && row < len(addresses) {
				addresses = append(addresses[:row], addresses[row+1:]...)
			}
		default:
			http.Error(w, "Unknown action", http.StatusBadRequest)
			return
		}
		// Write updated addresses back to file.
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

	// For GET: Render the HTML page.
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

// SessionStatus represents the structure for a session.
type SessionStatus struct {
	ID      int    `json:"id"`
	Port    int    `json:"port"`
	Status  string `json:"status"`
	Updated string `json:"updated"`
}

// readSessionStatusHandler reads the active session data from the file
// and displays it in an HTML table.
func readSessionStatusHandler(w http.ResponseWriter, r *http.Request) {
	filePath := "../datacenter/activesession.json"
	var sessions []SessionStatus

	// Check if the file exists and read its contents.
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
		// If file doesn't exist, we'll just show an empty list.
		sessions = []SessionStatus{}
	}

	// Build the HTML page.
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
