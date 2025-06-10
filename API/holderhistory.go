package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"golang.org/x/sync/singleflight" // Import the singleflight package
)

// NOTE: compress and decompress functions are assumed to be defined elsewhere in your project.

const (
	CacheTTL               = 5 * time.Minute
	RawDataRedisKeyPattern = "holderhistory:%s" // For fetching raw data
	CacheKeyPattern        = "history:%s"       // For caching processed data
)

// ----- Type Definitions (Unchanged) -----
type UpstreamHolderData struct {
	Amount []float64 `json:"amount"`
	Time   []string  `json:"time"`
}

type AggregatedEntry_ struct {
	Time    string  `json:"time"`
	Holders float64 `json:"holders"`
}

type PaginatedResponse struct {
	History    []AggregatedEntry_ `json:"history"`
	Page       int                `json:"page"`
	TotalPages int                `json:"totalPages"`
	TotalItems int                `json:"totalItems"`
}

// ----- Handler (Refactored for Concurrency Safety) -----

// Handler now includes a singleflight.Group to prevent thundering herds.
type Handler struct {
	redisClient *redis.Client
	sfGroup     singleflight.Group
}

// NewHandler creates a new Handler instance.
func NewHandler(redisClient *redis.Client) *Handler {
	return &Handler{
		redisClient: redisClient,
		// sfGroup is zero-value ready
	}
}

// ----- Core Logic (Unchanged and Memory-Efficient) -----

func (h *Handler) streamAndProcess(ctx context.Context, dataStream io.Reader, address string) ([]AggregatedEntry_, error) {
	holderDataAggregated := make(map[string]float64)
	decoder := json.NewDecoder(dataStream)

	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("read open brace: %w", err)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("read 'holders' key: %w", err)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("read open bracket: %w", err)
	}

	for decoder.More() {
		var holder UpstreamHolderData
		if err := decoder.Decode(&holder); err != nil {
			log.Printf("Warning: Could not decode entry for %s: %v", address, err)
			continue
		}
		if len(holder.Amount) != len(holder.Time) {
			continue
		}
		for i, tsStr := range holder.Time {
			parsedTime, err := time.Parse(time.RFC3339Nano, tsStr)
			if err != nil {
				parsedTime, err = time.Parse("2006-01-02T15:04:05Z07:00", tsStr)
				if err != nil {
					continue
				}
			}
			roundedTime := parsedTime.Truncate(time.Minute)
			formattedTime := roundedTime.UTC().Format(time.RFC3339)
			holderDataAggregated[formattedTime] += holder.Amount[i]
		}
	}

	result := make([]AggregatedEntry_, 0, len(holderDataAggregated))
	for t, totalAmount := range holderDataAggregated {
		result = append(result, AggregatedEntry_{Time: t, Holders: totalAmount})
	}

	sort.Slice(result, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, result[i].Time)
		tj, _ := time.Parse(time.RFC3339, result[j].Time)
		return ti.After(tj)
	})
	return result, nil
}

func (h *Handler) fetchAndProcessRawData(ctx context.Context, address string) ([]AggregatedEntry_, error) {
	rawRedisKey := fmt.Sprintf(RawDataRedisKeyPattern, address)
	rawJSONDataStr, err := h.redisClient.Get(rawRedisKey).Result()
	if err == redis.Nil {
		return []AggregatedEntry_{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fetch raw data: %w", err)
	}

	rawJSONData, err := decompress([]byte(rawJSONDataStr))
	if err != nil {
		return nil, fmt.Errorf("decompress raw data: %w", err)
	}

	dataStream := bytes.NewReader(rawJSONData)
	return h.streamAndProcess(ctx, dataStream, address)
}

// ----- HTTP Handler and Helpers (Refactored for Concurrency Safety) -----

// Helper struct for singleflight results
type historyComputationResult struct {
	dataBytes []byte
}

// GetHistory is the concurrency-safe HTTP handler function.
func (h *Handler) GetHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parameter Parsing
	address := r.URL.Query().Get("address")
	if address == "" {
		http.Error(w, `{"error": "Missing address"}`, http.StatusBadRequest)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 {
		limit = 50
	}

	cacheKey := fmt.Sprintf(CacheKeyPattern, address)

	// Step 1: Check Cache
	cachedDataStr, err := h.redisClient.Get(cacheKey).Result()
	if err == nil {
		log.Printf("Cache hit for %s", cacheKey)
		h.triggerBackgroundRefresh(ctx, address, cacheKey)
		decompressedData, err := decompress([]byte(cachedDataStr))
		if err == nil {
			var aggregatedData []AggregatedEntry_
			if json.Unmarshal(decompressedData, &aggregatedData) == nil {
				h.respondWithJSON(w, http.StatusOK, paginate(aggregatedData, page, limit))
				return
			}
		}
	} else if err != redis.Nil {
		log.Printf("Redis error getting cache for %s: %v", cacheKey, err)
	}

	// Step 2: Cache Miss - Use singleflight to compute safely
	log.Printf("Cache miss for %s, using singleflight to compute.", cacheKey)
	v, err, _ := h.sfGroup.Do(cacheKey, func() (interface{}, error) {
		log.Printf("Executing expensive computation for %s", cacheKey)
		data, err := h.fetchAndProcessRawData(ctx, address)
		if err != nil {
			return nil, err
		}
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		compressedData, err := compress(jsonData)
		if err == nil {
			h.redisClient.Set(cacheKey, compressedData, CacheTTL)
		}
		return &historyComputationResult{dataBytes: jsonData}, nil
	})
	h.sfGroup.Forget(cacheKey)

	if err != nil {
		log.Printf("Error during singleflight computation for %s: %v", cacheKey, err)
		http.Error(w, `{"error": "Internal Server Error during data processing"}`, http.StatusInternalServerError)
		return
	}

	// Step 3: Process the result from singleflight
	resultData := v.(*historyComputationResult)
	var aggregatedData []AggregatedEntry_
	if err := json.Unmarshal(resultData.dataBytes, &aggregatedData); err != nil {
		log.Printf("Error unmarshalling result from singleflight for %s: %v", cacheKey, err)
		http.Error(w, `{"error": "Internal Server Error after computation"}`, http.StatusInternalServerError)
		return
	}

	h.respondWithJSON(w, http.StatusOK, paginate(aggregatedData, page, limit))
}

func (h *Handler) triggerBackgroundRefresh(ctx context.Context, address, cacheKey string) {
	go func() {
		// Use singleflight here as well to prevent multiple background refreshes from piling up.
		h.sfGroup.Do(cacheKey+"_refresh", func() (interface{}, error) {
			log.Printf("Triggering background refresh for %s", cacheKey)
			bgCtx := context.Background()
			data, err := h.fetchAndProcessRawData(bgCtx, address)
			if err != nil {
				log.Printf("Background refresh failed for %s: %v", cacheKey, err)
				return nil, err
			}
			jsonData, _ := json.Marshal(data)
			compressedData, _ := compress(jsonData)
			h.redisClient.Set(cacheKey, compressedData, CacheTTL)
			log.Printf("Cache refreshed in background for %s", cacheKey)
			return nil, nil
		})
	}()
}

func paginate(data []AggregatedEntry_, page, limit int) PaginatedResponse {
	totalItems := len(data)
	totalPages := 0
	if totalItems > 0 && limit > 0 {
		totalPages = int(math.Ceil(float64(totalItems) / float64(limit)))
	}
	start, end := (page-1)*limit, page*limit
	if start > totalItems {
		start, end = totalItems, totalItems
	} else if end > totalItems {
		end = totalItems
	}
	var paginatedData []AggregatedEntry_
	if start < end {
		paginatedData = data[start:end]
	} else {
		paginatedData = []AggregatedEntry_{}
	}
	return PaginatedResponse{
		History: paginatedData, Page: page, TotalPages: totalPages, TotalItems: totalItems,
	}
}

func (h *Handler) respondWithJSON(w http.ResponseWriter, statusCode int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshalling JSON response: %v", err)
		http.Error(w, `{"error":"Failed to marshal response"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if _, err := w.Write(response); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}
