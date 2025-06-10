package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	holders "github.com/thankgod20/scraperServer/Holders"
	"golang.org/x/sync/singleflight" // Import the singleflight package
)

const (
	aggregateCacheTTL       = 5 * time.Minute
	defaultAggregatePage    = 1
	defaultAggregateLimit   = 50
	redisAggregateKeyPrefix = "aggregate:"
)

// ----- Type Definitions (Unchanged) -----

type NullTime struct {
	time.Time
}

func (nt *NullTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "null" || s == "" {
		nt.Time = time.Time{}
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
	}
	nt.Time = t
	return nil
}

type Transfer struct {
	Address string   `json:"address"`
	Amount  float64  `json:"amount"`
	Time    NullTime `json:"time"`
	Price   float64  `json:"price"`
}

type AggregatedEntry struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
	Time   string  `json:"time"`
}

type PaginatedAggregatedResponse struct {
	Holders    []AggregatedEntry `json:"holders"`
	Page       int               `json:"page"`
	TotalPages int               `json:"totalPages"`
	TotalItems int               `json:"totalItems"`
}

// ----- Service Definition (Refactored) -----

type AggregateService struct {
	redisClient    *redis.Client
	bitqueryClient *holders.BitqueryClient
	sfGroup        singleflight.Group
}

func NewAggregateService(redisClient *redis.Client, bqClient *holders.BitqueryClient) *AggregateService {
	return &AggregateService{
		redisClient:    redisClient,
		bitqueryClient: bqClient,
		// sfGroup is zero-value ready
	}
}

// ----- Helper and Core Logic Functions (Unchanged) -----

func roundTimeToMinuteStartAndFormat(t time.Time) (string, error) {
	if t.IsZero() {
		return "", fmt.Errorf("cannot round zero time")
	}
	return t.Truncate(time.Minute).Format(time.RFC3339), nil
}

func compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func (s *AggregateService) streamAndAggregateData(jsonData io.Reader) ([]AggregatedEntry, error) {
	aggregatedDataMap := make(map[string]AggregatedEntry)
	decoder := json.NewDecoder(jsonData)
	if _, err := decoder.Token(); err != nil {
		if err == io.EOF {
			return []AggregatedEntry{}, nil
		}
		return nil, fmt.Errorf("stream read error: %w", err)
	}
	for decoder.More() {
		var entry Transfer
		if err := decoder.Decode(&entry); err != nil {
			continue
		}
		if entry.Time.IsZero() {
			continue
		}
		formattedTime, err := roundTimeToMinuteStartAndFormat(entry.Time.Time)
		if err != nil {
			continue
		}
		key := fmt.Sprintf("%.2f_%s", entry.Price, formattedTime)
		if existing, ok := aggregatedDataMap[key]; ok {
			existing.Amount += entry.Amount
			aggregatedDataMap[key] = existing
		} else {
			aggregatedDataMap[key] = AggregatedEntry{
				Price: entry.Price, Amount: entry.Amount, Time: formattedTime,
			}
		}
	}
	resultList := make([]AggregatedEntry, 0, len(aggregatedDataMap))
	for _, data := range aggregatedDataMap {
		resultList = append(resultList, data)
	}
	sort.Slice(resultList, func(i, j int) bool { return resultList[i].Time > resultList[j].Time })
	return resultList, nil
}

func (s *AggregateService) computeAndAggregateData(ctx context.Context, address string) ([]AggregatedEntry, error) {
	rawJsonData, err := s.bitqueryClient.UpdateAndGetTransfers(s.redisClient, address)
	if err != nil {
		return nil, fmt.Errorf("BitqueryClient error: %w", err)
	}
	if len(rawJsonData) == 0 {
		return []AggregatedEntry{}, nil
	}
	jsonStream := bytes.NewReader([]byte(rawJsonData))
	result, err := s.streamAndAggregateData(jsonStream)
	if err != nil {
		return nil, fmt.Errorf("stream processing error: %w", err)
	}
	log.Printf("Successfully computed aggregate data for %s, %d entries", address, len(result))
	return result, nil
}

// ----- Concurrency-Safe HTTP Handler and Caching -----

// Helper struct for singleflight results
type aggregateComputationResult struct {
	dataBytes []byte
}

func (s *AggregateService) triggerAggregateBackgroundRefresh(ctx context.Context, redisStoreKey, address string) {
	go func() {
		// Use singleflight to prevent multiple background refreshes from piling up.
		s.sfGroup.Do(redisStoreKey+"_refresh", func() (interface{}, error) {
			log.Printf("Triggering Aggregate background refresh for %s", redisStoreKey)
			bgCtx := context.Background()
			data, err := s.computeAndAggregateData(bgCtx, address)
			if err != nil {
				log.Printf("Error during background refresh for %s: %v", redisStoreKey, err)
				return nil, err
			}
			jsonData, _ := json.Marshal(data)
			compressedData, _ := compress(jsonData)
			s.redisClient.Set(redisStoreKey, compressedData, aggregateCacheTTL)
			log.Printf("Aggregate cache refreshed in background for %s", redisStoreKey)
			return nil, nil
		})
	}()
}

func paginateAggregatedData(data []AggregatedEntry, page, limit int) PaginatedAggregatedResponse {
	totalItems := len(data)
	if limit <= 0 {
		limit = defaultAggregateLimit
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
	startIndex, endIndex := (page-1)*limit, page*limit
	var paginatedSlice []AggregatedEntry
	if startIndex < totalItems {
		if endIndex > totalItems {
			endIndex = totalItems
		}
		paginatedSlice = data[startIndex:endIndex]
	} else {
		paginatedSlice = []AggregatedEntry{}
	}
	return PaginatedAggregatedResponse{
		Holders: paginatedSlice, Page: page, TotalPages: totalPages, TotalItems: totalItems,
	}
}

// HandleAggregateRequest is the concurrency-safe HTTP handler.
func (s *AggregateService) HandleAggregateRequest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parameter Parsing
	address := r.URL.Query().Get("address")
	if address == "" {
		http.Error(w, `{"error": "Missing address"}`, http.StatusBadRequest)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = defaultAggregatePage
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = defaultAggregateLimit
	}

	redisStoreKey := fmt.Sprintf("%s%s", redisAggregateKeyPrefix, address)
	log.Printf("New Aggregate request - redis key: %s", redisStoreKey)

	// Step 1: Check Cache
	cacheEntryStr, redisErr := s.redisClient.Get(redisStoreKey).Result()
	if redisErr == nil {
		log.Printf("Aggregate cache hit for %s. Serving stale and refreshing.", redisStoreKey)
		s.triggerAggregateBackgroundRefresh(ctx, redisStoreKey, address)
		cacheEntry, err := decompress([]byte(cacheEntryStr))
		if err == nil {
			var cachedData []AggregatedEntry
			if json.Unmarshal(cacheEntry, &cachedData) == nil {
				result := paginateAggregatedData(cachedData, page, limit)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(result)
				return
			}
		}
	} else if redisErr != redis.Nil {
		log.Printf("Redis error (non-Nil) for key %s: %v", redisStoreKey, redisErr)
	}

	// Step 2: Cache Miss - Use singleflight to compute safely
	log.Printf("Aggregate cache miss for %s. Using singleflight.", redisStoreKey)
	v, err, _ := s.sfGroup.Do(redisStoreKey, func() (interface{}, error) {
		log.Printf("Executing expensive Aggregate computation for %s", redisStoreKey)
		computedData, err := s.computeAndAggregateData(ctx, address)
		if err != nil {
			return nil, err
		}
		dataJSON, err := json.Marshal(computedData)
		if err != nil {
			return nil, err
		}
		compressedData, err := compress(dataJSON)
		if err == nil {
			s.redisClient.Set(redisStoreKey, compressedData, aggregateCacheTTL)
		}
		return &aggregateComputationResult{dataBytes: dataJSON}, nil
	})
	s.sfGroup.Forget(redisStoreKey)

	if err != nil {
		log.Printf("Error during singleflight Aggregate computation for %s: %v", redisStoreKey, err)
		http.Error(w, `{"error": "Internal Server Error during data computation"}`, http.StatusInternalServerError)
		return
	}

	// Step 3: Process the result from singleflight
	resultData := v.(*aggregateComputationResult)
	var data []AggregatedEntry
	if err := json.Unmarshal(resultData.dataBytes, &data); err != nil {
		log.Printf("Error unmarshalling result from singleflight for %s: %v", redisStoreKey, err)
		http.Error(w, `{"error": "Internal Server Error after computation"}`, http.StatusInternalServerError)
		return
	}

	result := paginateAggregatedData(data, page, limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
