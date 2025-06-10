package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"

	// "os" // No longer needed for DATA_HOST
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const (
	snapshotCacheTTL = 5 * time.Minute // Cache TTL for the *processed* snapshot data
	// defaultDataHost    = "localhost"     // No longer needed
	// externalAPITimeout = 10 * time.Second  // No longer needed
	defaultSnapshotPage  = 1
	defaultSnapshotLimit = 50
)

// SnapshotEntryInput represents a single entry from the raw data source (now Redis).
// Example: { "holders": "123", "time": "2023-01-01T12:34:56Z" }

// Current struct definition:
// SnapshotEntryInput represents a single entry from the raw data source (now Redis).
type SnapshotEntryInput struct {
	Holders int    `json:"holders"` // Changed from string to int
	Time    string `json:"time"`
}

// Actual JSON data example:
// {"holders":974,"time":"2025-06-01T17:46:28Z"} // << `holders` is a number
// SnapshotData represents an aggregated data point for holder counts at a specific minute.
type SnapshotData struct {
	Holders int    `json:"holders"`
	Time    string `json:"time"`
}

// PaginatedSnapshotsResponse is the structure for the paginated API response.
type PaginatedSnapshotsResponse struct {
	Snapshot   []SnapshotData `json:"snapshot"`
	Page       int            `json:"page"`
	TotalPages int            `json:"totalPages"`
	TotalItems int            `json:"totalItems"`
}

// HolderSnapshotService handles fetching, caching, and serving of holder snapshot data.
type HolderSnapshotService struct {
	redisClient *redis.Client
	// httpClient  *http.Client // No longer needed for fetching raw data
	// dataHost    string       // No longer needed

	refreshing     map[string]bool
	refreshingLock sync.Mutex
}

// NewHolderSnapshotService creates a new instance of HolderSnapshotService.
func NewHolderSnapshotService(redisClient *redis.Client) *HolderSnapshotService {
	// DATA_HOST and httpClient logic removed as raw data is fetched from Redis.
	return &HolderSnapshotService{
		redisClient: redisClient,
		refreshing:  make(map[string]bool),
	}
}

// fetchAndAggregateData fetches raw snapshot data from a specific Redis key,
// processes, and aggregates it.
func (s *HolderSnapshotService) fetchAndAggregateData(ctx context.Context, address string) ([]SnapshotData, error) {
	// The Redis key for fetching the raw holder plot data.
	rawDataSourceRedisKey := fmt.Sprintf("token:%s:holdersplot", address)
	log.Printf("Fetching raw data from Redis key: %s", rawDataSourceRedisKey)

	// Get raw data from Redis
	rawDataJSON_, err := s.redisClient.Get(rawDataSourceRedisKey).Result()
	//fmt.Println("=======rawDataJSON_:", rawDataJSON_)
	if err == redis.Nil {
		log.Printf("Raw data not found in Redis at key %s for address %s. Returning empty dataset.", rawDataSourceRedisKey, address)
		return []SnapshotData{}, nil // No data found is not necessarily an error for aggregation
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get raw data from Redis key %s: %w", rawDataSourceRedisKey, err)
	}
	rawDataJSON, err := decompress([]byte(rawDataJSON_))
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data from Redis key %s: %w", rawDataSourceRedisKey, err)
	}
	var rawEntries []SnapshotEntryInput
	if err := json.Unmarshal((rawDataJSON), &rawEntries); err != nil {
		// Attempt to parse as a different structure if the primary one fails, similar to the guide example.
		// For instance, if the guide showed parsing `{"holders": [...]}` vs `[...]` directly.
		// Here, we'll stick to the primary `[]SnapshotEntryInput` expectation for now.
		// If the data under `token:%s:holdersplot` has a wrapper, this part would need adjustment.
		return nil, fmt.Errorf("failed to unmarshal raw data from Redis key %s: %w. Data: %s", rawDataSourceRedisKey, err, rawDataJSON)
	}

	if len(rawEntries) == 0 {
		log.Printf("No entries found after parsing raw data from Redis key %s for address %s.", rawDataSourceRedisKey, address)
		return []SnapshotData{}, nil
	}

	// Aggregate data by minute (same logic as before)
	holderDataMap := make(map[string]SnapshotData)
	for _, entry := range rawEntries {
		//holdersCount, convErr := strconv.Atoi(entry.Holders)
		/*
			if convErr != nil {
				log.Printf("Warning: failed to parse holders count '%s' from Redis key %s, time %s: %v. Skipping entry.", entry.Holders, rawDataSourceRedisKey, entry.Time, convErr)
				continue
			}*/
		holdersCount := entry.Holders
		parsedTime, timeErr := time.Parse(time.RFC3339Nano, entry.Time)
		if timeErr != nil {
			parsedTime, timeErr = time.Parse(time.RFC3339, entry.Time)
			if timeErr != nil {
				log.Printf("Warning: failed to parse time '%s' from Redis key %s: %v. Skipping entry.", entry.Time, rawDataSourceRedisKey, timeErr)
				continue
			}
		}

		truncatedTime := parsedTime.Truncate(time.Minute)
		formattedTimeKey := truncatedTime.Format(time.RFC3339)

		holderDataMap[formattedTimeKey] = SnapshotData{
			Holders: holdersCount,
			Time:    formattedTimeKey,
		}
	}

	aggregatedData := make([]SnapshotData, 0, len(holderDataMap))
	for _, dataPoint := range holderDataMap {
		aggregatedData = append(aggregatedData, dataPoint)
	}

	sort.Slice(aggregatedData, func(i, j int) bool {
		return aggregatedData[i].Time > aggregatedData[j].Time
	})

	return aggregatedData, nil
}

// triggerBackgroundRefresh starts a goroutine to refresh the *processed data cache*.
// The raw data source is `token:${address}:holdersplot`. The processed data cache is `snapshot:${address}`.
func (s *HolderSnapshotService) triggerBackgroundRefresh(initiatingCtx context.Context, address string, processedDataRedisKey string) {
	s.refreshingLock.Lock()
	if s.refreshing[address] {
		s.refreshingLock.Unlock()
		log.Printf("Background refresh for processed snapshot %s (key: %s) is already in progress.", address, processedDataRedisKey)
		return
	}
	s.refreshing[address] = true
	s.refreshingLock.Unlock()

	log.Printf("Triggering background refresh for processed snapshot %s (key: %s)", address, processedDataRedisKey)

	go func() {
		defer func() {
			s.refreshingLock.Lock()
			delete(s.refreshing, address)
			s.refreshingLock.Unlock()
			log.Printf("Background refresh goroutine for processed snapshot %s (key: %s) has finished.", address, processedDataRedisKey)
		}()

		bgCtx := context.Background() // Use a new context for the background task

		// Step 1: Fetch raw data (from token:...:holdersplot) and aggregate it
		data, err := s.fetchAndAggregateData(bgCtx, address) // This now fetches from Redis key `token:${address}:holdersplot`
		if err != nil {
			log.Printf("Error during background refresh (fetchAndAggregateData) for address %s: %v", address, err)
			return
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Printf("Error marshalling data for background refresh cache (address %s, processed key: %s): %v", address, processedDataRedisKey, err)
			return
		}

		// Step 2: Store the processed data into the designated processed data cache key (snapshot:${address})
		compressedData, _ := compress(jsonData)
		err = s.redisClient.Set(processedDataRedisKey, compressedData, snapshotCacheTTL).Err()
		if err != nil {
			log.Printf("Error setting Redis cache for processed data during background refresh (address %s, key: %s): %v", address, processedDataRedisKey, err)
			return
		}
		log.Printf("Processed data cache refreshed in background for address %s (key: %s)", address, processedDataRedisKey)
	}()
}

// paginateSnapshotData remains the same
func paginateSnapshotData(allData []SnapshotData, page, limit int) PaginatedSnapshotsResponse {
	totalItems := len(allData)
	if limit <= 0 {
		limit = defaultSnapshotLimit
	}
	totalPages := 0
	if totalItems > 0 {
		totalPages = int(math.Ceil(float64(totalItems) / float64(limit)))
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages && totalPages > 0 {
		page = totalPages
	}
	startIndex := (page - 1) * limit
	endIndex := startIndex + limit
	var paginatedSlice []SnapshotData
	if startIndex < totalItems {
		if endIndex > totalItems {
			endIndex = totalItems
		}
		paginatedSlice = allData[startIndex:endIndex]
	} else {
		paginatedSlice = []SnapshotData{}
	}
	return PaginatedSnapshotsResponse{
		Snapshot:   paginatedSlice,
		Page:       page,
		TotalPages: totalPages,
		TotalItems: totalItems,
	}
}

// HandleGetHolderSnapshots is the HTTP handler.
// It uses `snapshot:${address}` as the cache key for *processed* data.
func (s *HolderSnapshotService) HandleGetHolderSnapshots(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	address := r.URL.Query().Get("address")
	if address == "" {
		http.Error(w, `{"error": "Missing address"}`, http.StatusBadRequest)
		return
	}

	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = defaultSnapshotPage
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = defaultSnapshotLimit
	}

	// This is the Redis key for caching the *processed and aggregated* snapshot data.
	processedDataCacheKey := fmt.Sprintf("snapshot:%s", address)
	log.Printf("Request for holder snapshots. Address: %s, Page: %d, Limit: %d. Processed data cache key: %s", address, page, limit, processedDataCacheKey)

	cachedData_, redisErr := s.redisClient.Get(processedDataCacheKey).Result()

	if redisErr == nil { // Cache hit for processed data
		log.Printf("Cache hit for processed data %s. Serving stale data and triggering background refresh.", processedDataCacheKey)
		s.triggerBackgroundRefresh(ctx, address, processedDataCacheKey)
		cachedData, _ := decompress([]byte(cachedData_))
		var snapshots []SnapshotData
		if umErr := json.Unmarshal((cachedData), &snapshots); umErr != nil {
			log.Printf("Error unmarshalling cached processed data for %s: %v. Will fetch and process fresh.", processedDataCacheKey, umErr)
		} else {
			paginatedResult := paginateSnapshotData(snapshots, page, limit)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(paginatedResult); err != nil {
				log.Printf("Error encoding cached response for %s: %v", address, err)
			}
			return
		}
	}

	if redisErr != redis.Nil {
		log.Printf("Redis error (excluding Nil) for processed data key %s: %v. Attempting to fetch and process fresh.", processedDataCacheKey, redisErr)
	} else {
		log.Printf("Cache miss for processed data key %s.", processedDataCacheKey)
	}

	// Fetch fresh raw data (from token:...:holdersplot), aggregate it.
	log.Printf("Fetching and processing new data for address %s.", address)
	freshProcessedData, fetchErr := s.fetchAndAggregateData(ctx, address)
	if fetchErr != nil {
		log.Printf("Error fetching and aggregating data for %s: %v", address, fetchErr)
		errResp := map[string]string{"error": "Internal Server Error during data processing"}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errResp)
		return
	}

	// Cache the fresh *processed* data
	jsonData, marshalErr := json.Marshal(freshProcessedData)
	if marshalErr != nil {
		log.Printf("Error marshalling fresh processed data for caching %s: %v.", address, marshalErr)
	} else {
		compressData, _ := compress(jsonData)
		setErr := s.redisClient.Set(processedDataCacheKey, compressData, snapshotCacheTTL).Err()
		if setErr != nil {
			log.Printf("Error setting Redis cache for processed data key %s: %v", processedDataCacheKey, setErr)
		} else {
			log.Printf("Successfully fetched, processed, and cached data into key %s.", processedDataCacheKey)
		}
	}

	paginatedResult := paginateSnapshotData(freshProcessedData, page, limit)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(paginatedResult); err != nil {
		log.Printf("Error encoding fresh response for %s: %v", address, err)
	}
}

/*
// Example main function to demonstrate how to run this service.
func main() {
	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Successfully connected to Redis.")

	snapshotService := NewHolderSnapshotService(redisClient)

	http.HandleFunc("/api/holder-snapshots", snapshotService.HandleGetHolderSnapshots)

	port := "8080"
	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
*/
