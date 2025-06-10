package holders

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/jsonrpc"
	"github.com/go-redis/redis"
)

// HolderInfo is the final output structure.
type HolderInfo struct {
	Holder string  `json:"address"`
	Amount float64 `json:"amount"`
	Time   string  `json:"time"`
	Price  float64 `json:"price"`
}

// BitqueryResponse defines the structure for the Bitquery API response.
type BitqueryResponse struct {
	Data struct {
		Solana struct {
			Transfers []struct {
				Block struct {
					Time string `json:"Time"`
				} `json:"Block"`
				Transfer struct {
					Amount   string `json:"Amount"`
					Receiver struct {
						Address string `json:"Address"`
					} `json:"Receiver"`
				} `json:"Transfer"`
			} `json:"Transfers"`
		} `json:"Solana"`
	} `json:"data"`
}

// BitqueryClient is a client for interacting with the Bitquery API.
type BitqueryClient struct {
	APIURL    string
	AuthToken string
}

// NewBitqueryClient creates a new BitqueryClient with default settings.
func NewBitqueryClient() *BitqueryClient {
	return &BitqueryClient{
		APIURL:    "https://streaming.bitquery.io/eap",
		AuthToken: "Bearer ory_at_FGsip4ADdVEwxvhQ9SeoNt7-1sczNLfC6Sn6yPRlyEs.dcjiWyByWco1HdvK5fxAkCmz2yqsg40047H-3QULCUU", // Replace if needed.
	}
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

// HolderInfo represents information about a holder/trade participant.

// GTTradeAttributes reflects the structure of the attributes for each trade from GeckoTerminal.
type GTTradeAttributes struct {
	BlockNumber              int    `json:"block_number"`
	TxHash                   string `json:"tx_hash"`
	TxFromAddress            string `json:"tx_from_address"`
	FromTokenAmount          string `json:"from_token_amount"`
	ToTokenAmount            string `json:"to_token_amount"`
	PriceFromInCurrencyToken string `json:"price_from_in_currency_token"`
	PriceToInCurrencyToken   string `json:"price_to_in_currency_token"`
	PriceFromInUSD           string `json:"price_from_in_usd"`
	PriceToInUSD             string `json:"price_to_in_usd"`
	BlockTimestamp           string `json:"block_timestamp"`
	Kind                     string `json:"kind"`
	VolumeInUSD              string `json:"volume_in_usd"`
	FromTokenAddress         string `json:"from_token_address"`
	ToTokenAddress           string `json:"to_token_address"`
}

// GTTrade is the structure for an individual trade record.
type GTTrade struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Attributes GTTradeAttributes `json:"attributes"`
}

// GTTradesResponse defines the full response from GeckoTerminal.
type GTTradesResponse struct {
	Data []GTTrade `json:"data"`
}

// BitqueryClient is repurposed here as an API client that now holds the GeckoTerminal endpoint URL.
func getPoolID(address string) (string, error) {
	poolURL := "https://api.geckoterminal.com/api/v2/networks/solana/tokens/" + address + "/pools"
	resp, err := http.Get(poolURL)
	if err != nil {
		return "", fmt.Errorf("failed to get pool data: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read pool response body: %v", err)
	}

	// Define a struct to parse the returned pool data.
	var poolResponse struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &poolResponse); err != nil {
		return "", fmt.Errorf("failed to unmarshal pool response: %v", err)
	}

	if poolResponse.Data == nil || len(poolResponse.Data) == 0 {
		return "", fmt.Errorf("no pool data found for the provided address")
	}

	// Use the first pool's id.
	rawPoolID := poolResponse.Data[0].ID

	// Remove the "solana_" prefix if present.
	poolID := rawPoolID
	if strings.HasPrefix(rawPoolID, "solana_") {
		poolID = strings.TrimPrefix(rawPoolID, "solana_")
	}

	return poolID, nil
}

// UpdateAndGetTransfers now fetches trade data from GeckoTerminal and then updates holder amounts.
/*
func (c *BitqueryClient) UpdateAndGetTransfers(rdb *redis.Client, tokenAddress string) (string, error) {
	// Set up Redis client.
	// Use a Redis key for storage.
	redisKey := "holders:" + tokenAddress
	unionMap := make(map[string]HolderInfo)
	// Use a default limit if you want to restrict caching amounts.
	//fileLimit := 1000

	// Phase 1: Retrieve any existing data from Redis.
	val_, err := rdb.Get(redisKey).Result()
	if err == nil  {
		val, _ := decompress([]byte(val_))
		// If data exists, we reduce our fetch limit.
		//fileLimit = 100
		var savedHolders []HolderInfo
		if err := json.Unmarshal(val, &savedHolders); err != nil {
			log.Printf("warning: could not unmarshal existing redis data, proceeding with empty list: %v", err)
		} else {
			for _, rec := range savedHolders {
				unionMap[rec.Holder] = rec
			}
		}
	} else if err != nil && err != redis.Nil {
		log.Printf("error reading from redis: %v", err)
	}
	poolID, _ := getPoolID(tokenAddress)
	fmt.Println("Pool ID", poolID)
	// Build the GeckoTerminal API URL.
	gtURL := "https://api.geckoterminal.com/api/v2/networks/solana/pools/" + poolID + "/trades?trade_volume_in_usd_greater_than=100"
	// You could append additional query parameters like fileLimit if the API supports it.

	req, err := http.NewRequest("GET", gtURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create GeckoTerminal request: %v", err)
	}
	// Add headers if required by the API. For example:
	req.Header.Add("Content-Type", "application/json")
	if c.AuthToken != "" {
		req.Header.Add("Authorization", c.AuthToken)
	}

	clientHTTP := &http.Client{Timeout: 10 * time.Second}
	resp, err := clientHTTP.Do(req)
	if err != nil {
		log.Printf("failed to execute GeckoTerminal request: %v. Falling back to cached data...", err)
		// Try to return the cached value from Redis.
		cached_, redisErr := rdb.Get(redisKey).Result()
		if redisErr != nil {
			return "", fmt.Errorf("GeckoTerminal error: %v; and redis fallback error: %v", err, redisErr)
		}
		cached, _ := decompress([]byte(cached_))
		return string(cached), nil
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read GeckoTerminal response body: %v", err)
	}

	var gtResponse GTTradesResponse
	if err := json.Unmarshal(body, &gtResponse); err != nil {
		return "", fmt.Errorf("failed to unmarshal GeckoTerminal response: %v", err)
	}

	// Append new GeckoTerminal trade data to the union map.
	for _, trade := range gtResponse.Data {
		// Use the tx_from_address as the unique key.
		addr := trade.Attributes.TxFromAddress
		if addr == "" {
			// Optionally skip trades with no source address.
			continue
		}
		// Only add if the record does not already exist.
		if _, exists := unionMap[addr]; !exists {
			// Parse the amount from the string returned by the API.
			if trade.Attributes.Kind == "buy" {
				amount, err := strconv.ParseFloat(trade.Attributes.ToTokenAmount, 64)
				if err != nil {
					log.Printf("failed to parse trade amount %s for address %s: %v", trade.Attributes.FromTokenAmount, addr, err)
					amount = 0
				}
				price, err := strconv.ParseFloat(trade.Attributes.PriceToInUSD, 64)
				if err != nil {
					log.Printf("failed to parse trade price %s for address %s: %v", trade.Attributes.PriceToInUSD, addr, err)
					price = 0
				}
				// Record the trade timestamp and amount.
				unionMap[addr] = HolderInfo{
					Holder: addr,
					Time:   trade.Attributes.BlockTimestamp,
					Amount: amount,
					Price:  price,
				}
			}
		}
	}

	// Write the merged GeckoTerminal data to Redis.
	var mergedHolders []HolderInfo
	for _, rec := range unionMap {
		mergedHolders = append(mergedHolders, rec)
	}
	jsonOutput, err := json.MarshalIndent(mergedHolders, "", "  ")
	if err != nil {
		return "", fmt.Errorf("error marshalling merged data: %v", err)
	}
	compressedData_, _ := compress(jsonOutput)
	if err := rdb.Set(redisKey, compressedData_, 0).Err(); err != nil {
		return "", fmt.Errorf("error saving merged data to redis: %v", err)
	}

	holderBalances := GetCurrentHolders(tokenAddress, rdb)
	//fmt.Println("Holder Details", holderBalances)
	// Update amounts in unionMap using on-chain values
	recentPrice := 0.000
	i := 0 // loop counter

	for addr, amount := range holderBalances {
		if rec, exists := unionMap[addr]; exists {
			if recentPrice == 0 {
				recentPrice = rec.Price
			}
			rec.Amount = amount
			unionMap[addr] = rec
		} else {
			// Add new holders not seen in trade history
			if recentPrice != 0 {
				// inverse exponential decay function: e.g., 1 / (1 + e^(i))
				decayFactor := 1.0 / (1 + math.Exp(float64(i)))

				// Subtract up to 10 minutes back in time, scaling with decay
				pastTime := time.Now().Add(-time.Duration(decayFactor*10) * time.Minute)

				// Reduce price by decay factor times recentPrice
				newPrice := recentPrice * (1 - decayFactor)

				unionMap[addr] = HolderInfo{
					Holder: addr,
					Amount: amount,
					Time:   pastTime.Format(time.RFC3339),
					Price:  newPrice,
				}
				i++
			}
		}
	}
	// Update amounts in unionMap using on‑chain values.


	// Write the final updated data to Redis.
	var finalHolders []HolderInfo
	for _, rec := range unionMap {
		finalHolders = append(finalHolders, rec)
	}
	finalJSON, err := json.MarshalIndent(finalHolders, "", "  ")
	if err != nil {
		return "", fmt.Errorf("error marshalling final output: %v", err)
	}
	compressedData, _ := compress(finalJSON)
	if err := rdb.Set(redisKey, compressedData, 0).Err(); err != nil {
		return "", fmt.Errorf("error saving final data to redis: %v", err)
	}

	// Retrieve and return the final data from Redis.
	val_, err = rdb.Get(redisKey).Result()
	if err != nil {
		return "", fmt.Errorf("error reading final data from redis: %v", err)
	}
	val, err := decompress([]byte(val_))
	if err != nil {
		fmt.Printf("error decompressing final data from redis: %v", err)
	}
	return string(val), nil
}
*/
// fetchCachedHolders safely fetches and decodes holder data from Redis using a stream.
func (c *BitqueryClient) fetchCachedHolders(rdb *redis.Client, redisKey string) (map[string]HolderInfo, error) {
	unionMap := make(map[string]HolderInfo)
	valStr, err := rdb.Get(redisKey).Result()
	if err == redis.Nil {
		return unionMap, nil // Key doesn't exist, return empty map
	}
	if err != nil {
		return nil, fmt.Errorf("error reading from redis: %w", err)
	}

	valBytes, err := decompress([]byte(valStr))
	if err != nil {
		return nil, fmt.Errorf("could not decompress existing redis data: %w", err)
	}

	// Use a streaming decoder for memory efficiency
	decoder := json.NewDecoder(bytes.NewReader(valBytes))
	// Expect an array `[`
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("invalid json format in cache (expected array start): %w", err)
	}

	for decoder.More() {
		var holder HolderInfo
		if err := decoder.Decode(&holder); err != nil {
			log.Printf("warning: could not decode a holder from redis cache, skipping: %v", err)
			continue
		}
		unionMap[holder.Holder] = holder
	}

	return unionMap, nil
}
func (c *BitqueryClient) fetchAndMergeGeckoTerminalTrades(poolID string, unionMap map[string]HolderInfo) error {
	gtURL := "https://api.geckoterminal.com/api/v2/networks/solana/pools/" + poolID + "/trades?trade_volume_in_usd_greater_than=100"
	req, err := http.NewRequest("GET", gtURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create GeckoTerminal request: %w", err)
	}

	clientHTTP := &http.Client{Timeout: 15 * time.Second}
	resp, err := clientHTTP.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute GeckoTerminal request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GeckoTerminal API returned non-200 status: %s", resp.Status)
	}

	// Use a streaming decoder on the response body
	decoder := json.NewDecoder(resp.Body)
	// Expect an object `{`
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("invalid json response from GT (expected object start): %w", err)
	}

	// Find the "data" key
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if key, ok := token.(string); ok && key == "data" {
			break // Found it, the next token will be the array start `[`
		}
	}

	// Expect an array `[`
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("invalid json response from GT (expected data array start): %w", err)
	}

	for decoder.More() {
		var trade GTTrade
		if err := decoder.Decode(&trade); err != nil {
			log.Printf("failed to decode a trade from GeckoTerminal, skipping: %v", err)
			continue
		}

		addr := trade.Attributes.TxFromAddress
		if addr == "" {
			continue
		}

		// Only add new holders not seen before
		if _, exists := unionMap[addr]; !exists && trade.Attributes.Kind == "buy" {
			amount, _ := strconv.ParseFloat(trade.Attributes.ToTokenAmount, 64)
			price, _ := strconv.ParseFloat(trade.Attributes.PriceToInUSD, 64)

			unionMap[addr] = HolderInfo{
				Holder: addr,
				Time:   trade.Attributes.BlockTimestamp,
				Amount: amount,
				Price:  price,
			}
		}
	}

	return nil
}
func (c *BitqueryClient) synthesizeNewHolder(addr string, amount float64, recentPrice float64, index int) HolderInfo {
	// Inverse exponential decay function to simulate a past time and price
	decayFactor := 1.0 / (1 + math.Exp(float64(index)*0.1)) // Added a coefficient to make decay more gradual
	pastTime := time.Now().Add(-time.Duration(decayFactor*10) * time.Minute)
	newPrice := recentPrice * (1 - decayFactor)

	return HolderInfo{
		Holder: addr,
		Amount: amount,
		Time:   pastTime.Format(time.RFC3339),
		Price:  newPrice,
	}
}

func (c *BitqueryClient) UpdateAndGetTransfers(rdb *redis.Client, tokenAddress string) ([]byte, error) {
	redisKey := "holders:" + tokenAddress

	// Step 1: Fetch cached data efficiently.
	unionMap, err := c.fetchCachedHolders(rdb, redisKey)
	if err != nil {
		return nil, fmt.Errorf("could not fetch cached holders: %w", err)
	}

	// Step 2: Fetch and merge new trade data from GeckoTerminal.
	poolID, _ := getPoolID(tokenAddress)
	if err := c.fetchAndMergeGeckoTerminalTrades(poolID, unionMap); err != nil {
		// This is a non-fatal error; we can proceed with cached + on-chain data.
		log.Printf("warning: could not fetch from GeckoTerminal, proceeding with existing data: %v", err)
	}

	// Step 3: Fetch current on-chain balances.
	holderBalances := GetCurrentHolders(tokenAddress, rdb)

	// Step 4: Update amounts and synthesize new holders in a single pass.
	var recentPrice float64
	// Find a recent price from the existing trade data to use as a baseline.
	for _, rec := range unionMap {
		if rec.Price > 0 {
			recentPrice = rec.Price
			break
		}
	}

	newHolderCounter := 0
	for addr, amount := range holderBalances {
		if rec, exists := unionMap[addr]; exists {
			rec.Amount = amount
			unionMap[addr] = rec
		} else if recentPrice > 0 { // Only synthesize if we have a price baseline
			unionMap[addr] = c.synthesizeNewHolder(addr, amount, recentPrice, newHolderCounter)
			newHolderCounter++
		}
	}

	// Step 5: Convert final map to slice and marshal ONCE.
	finalHolders := make([]HolderInfo, 0, len(unionMap))
	for _, rec := range unionMap {
		finalHolders = append(finalHolders, rec)
	}

	// Use standard Marshal for performance; Indent is for debugging.
	finalJSON, err := json.Marshal(finalHolders)
	if err != nil {
		return nil, fmt.Errorf("error marshalling final output: %w", err)
	}

	// Step 6: Compress and write to Redis ONCE.
	compressedData, err := compress(finalJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to compress final data: %w", err)
	}

	if err := rdb.Set(redisKey, compressedData, 0).Err(); err != nil {
		// Log the error but still return the data, as the computation was successful.
		log.Printf("warning: error saving final data to redis for key %s: %v", redisKey, err)
	}

	// Step 7: Return the in-memory data directly. No final read needed.
	return finalJSON, nil
}

// decodeAmount decodes an 8-byte little-endian uint64 value.
func decodeAmount(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

// CurrentHolder returns the current on‑chain token holders for the given token.
func CurrentHolder(token string, rds *redis.Client) rpc.GetProgramAccountsResult {
	quickNodeEndpoint := "https://sleek-wandering-tab.solana-mainnet.quiknode.pro/e5f4c26cc15290eda8ae67162a31a0070cf192d6"

	jsonrpcClient := jsonrpc.NewClient(quickNodeEndpoint)
	client := rpc.NewWithCustomRPCClient(jsonrpcClient)

	tokenMint := solana.MustPublicKeyFromBase58(token)
	rawMintStr := string(tokenMint.Bytes())

	filters := []rpc.RPCFilter{
		{
			Memcmp: &rpc.RPCFilterMemcmp{
				Offset: 0,
				Bytes:  solana.Base58(rawMintStr),
			},
		},
		{
			DataSize: 165, // Standard SPL token account size.
		},
	}

	accounts, err := client.GetProgramAccountsWithOpts(
		context.Background(),
		solana.TokenProgramID,
		&rpc.GetProgramAccountsOpts{
			Filters: filters,
		},
	)
	if err != nil {
		log.Fatalf("failed to get program accounts: %v", err)
	}
	fmt.Printf("Found %d token accounts for mint %s\n", len(accounts), tokenMint)
	return accounts
}

type HolderSnapshot struct {
	Holders int    `json:"holders"`
	Time    string `json:"time"`
}

// GetCurrentHolders returns a map of wallet addresses to token balances for the given token
func GetCurrentHolders(tokenAddress string, rds *redis.Client) map[string]float64 {
	quickNodeEndpoint := "https://light-ancient-pallet.solana-mainnet.quiknode.pro/6bb8bc30eb2438f8ee50d8ae8fb8425ab2dcdb97/"

	jsonrpcClient := jsonrpc.NewClient(quickNodeEndpoint)
	client := rpc.NewWithCustomRPCClient(jsonrpcClient)

	tokenMint := solana.MustPublicKeyFromBase58(tokenAddress)

	// Get token decimals to properly convert raw amounts
	/*tokenInfo, err := getTokenInfo(client, tokenMint)
	if err != nil {
		log.Printf("failed to get token info: %v", err)
		//return map[string]float64{} // Return empty map on error
	}
	//decimals := tokenInfo.Decimals
	//fmt.Printf("==== Token %s has %d decimals\n====", tokenMint, decimals)*/
	// Find all token accounts for this mint
	filters := []rpc.RPCFilter{
		{
			Memcmp: &rpc.RPCFilterMemcmp{
				Offset: 0,
				Bytes:  solana.Base58(tokenMint.Bytes()),
			},
		},
		{
			DataSize: 165, // Standard SPL token account size
		},
	}

	accounts, err := client.GetProgramAccountsWithOpts(
		context.Background(),
		solana.TokenProgramID,
		&rpc.GetProgramAccountsOpts{
			Filters: filters,
		},
	)
	if err != nil {
		log.Printf("failed to get program accounts: %v", err)
		return map[string]float64{} // Return empty map on error
	}

	fmt.Printf("Found %d token accounts for mint %s\n", len(accounts), tokenMint)

	// Map to store wallet addresses and their token balances
	holderBalances := make(map[string]float64)

	for _, acct := range accounts {
		// Parse the token account data
		tokenAccountData := parseTokenAccountData(acct.Account.Data.GetBinary())
		if tokenAccountData == nil {
			continue
		}

		// Skip accounts with zero balance

		// Get the owner (wallet address) of this token account
		owner := tokenAccountData.Owner.String()

		// Convert raw amount to decimal representation
		amount := float64(tokenAccountData.Amount) / math.Pow(10, float64(uint8(6)))

		// Add to our holders map, combining amounts if wallet has multiple token accounts
		holderBalances[owner] += amount
	}
	holderCount := len(holderBalances)

	// Get current UTC time in RFC3339 format
	currentTime := time.Now().UTC().Format(time.RFC3339)

	// Create the snapshot
	snapshot := HolderSnapshot{
		Holders: holderCount,
		Time:    currentTime,
	}

	// Define the Redis key
	redisKey := fmt.Sprintf("token:%s:holdersplot", tokenAddress)

	// Check if the key exists in Redis.
	exists, err := rds.Exists(redisKey).Result()
	if err != nil {
		log.Printf("failed to check if key exists in Redis: %v", err)
	}

	var snapshots []HolderSnapshot

	if exists == 1 {
		// Key exists; retrieve existing snapshots.
		val_, err := rds.Get(redisKey).Result()
		if err != nil {
			log.Printf("failed to get existing snapshots from Redis: %v", err)
			return nil
		}
		val, err := decompress([]byte(val_))
		if err != nil {
			log.Printf("failed to decompress existing snapshots: %v", err)

		}
		// Unmarshal the existing JSON array into the snapshots slice.
		if err := json.Unmarshal((val), &snapshots); err != nil {
			log.Printf("failed to unmarshal existing snapshots: %v", err)
		}
	}

	// Append the new snapshot to the slice.
	snapshots = append(snapshots, snapshot)

	// Marshal the updated snapshots slice to JSON.
	jsonData, err := json.Marshal(snapshots)
	if err != nil {
		log.Printf("failed to marshal snapshots to JSON: %v", err)
	}
	compressedData, err := compress(jsonData)
	if err != nil {
		log.Printf("failed to compress JSON data: %v", err)
		//return holderBalances // Return current balances even if compression fails
	}
	// Store the updated JSON array back to Redis.
	if err := rds.Set(redisKey, compressedData, 0).Err(); err != nil {
		log.Printf("failed to store snapshots in Redis: %v", err)
	}
	return holderBalances
}

// TokenAccountData represents the parsed data of a token account
type TokenAccountData struct {
	Mint   solana.PublicKey
	Owner  solana.PublicKey
	Amount uint64
}

// parseTokenAccountData parses the binary data of a token account
func parseTokenAccountData(data []byte) *TokenAccountData {
	if len(data) < 165 {
		return nil // Not enough data for a token account
	}

	var mint solana.PublicKey
	copy(mint[:], data[0:32])

	var owner solana.PublicKey
	copy(owner[:], data[32:64])

	amount := binary.LittleEndian.Uint64(data[64:72])

	return &TokenAccountData{
		Mint:   mint,
		Owner:  owner,
		Amount: amount,
	}
}

// TokenInfo represents the data of a token mint account
type TokenInfo struct {
	Decimals uint8
}

// getTokenInfo fetches the token mint info to get decimals
func getTokenInfo(client *rpc.Client, mint solana.PublicKey) (*TokenInfo, error) {
	accountResult, err := client.GetAccountInfo(context.Background(), mint)
	if err != nil {
		return nil, fmt.Errorf("failed to get mint account info: %v", err)
	}

	// For GetAccountInfo, we need to use Value.Data instead of Data directly
	if accountResult == nil || accountResult.Value == nil {
		return nil, fmt.Errorf("no account info returned")
	}

	// Get the binary data from the account info result
	data := accountResult.Value.Data.GetBinary()

	if len(data) < 82 {
		return nil, fmt.Errorf("invalid mint account data length")
	}

	// Decimals is at offset 44 in the mint data structure
	decimals := data[44]

	return &TokenInfo{
		Decimals: decimals,
	}, nil
}
