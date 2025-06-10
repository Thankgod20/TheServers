// /api/dataprovider.go
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"golang.org/x/sync/singleflight"
)

const (
	pivotedHistoryCacheTTL = 5 * time.Minute
	pivotedHistoryPrefix   = "pivotedhistory:"
	// rawHolderHistoryPrefix is defined in srs_analytics.go/flow_analytics.go, but used here.
	defaultPivotingInterval = 1 // 1 minute, as it's the shared interval.
)

// PivotedHistory is the shared, pre-computed data structure.
// It's the result of the expensive computation that other services will consume.
type PivotedHistory struct {
	TimeSeries map[string]map[string]float64 `json:"time_series"`
	AddressMax map[string]float64            `json:"address_max"`
}

// DataProvider centralizes the expensive data processing.
type DataProvider struct {
	redisClient *redis.Client
	sfGroup     singleflight.Group
}

// NewDataProvider creates a new instance of the DataProvider.
func NewDataProvider(redisClient *redis.Client) *DataProvider {
	return &DataProvider{
		redisClient: redisClient,
	}
}

// GetPivotedHistory is the main entry point for other services.
// It handles caching and uses singleflight to ensure the computation runs only once.
func (dp *DataProvider) GetPivotedHistory(ctx context.Context, address string) (*PivotedHistory, error) {
	cacheKey := fmt.Sprintf("%s%s", pivotedHistoryPrefix, address)

	// 1. Check cache for the already-pivoted data.
	cachedDataStr, err := dp.redisClient.Get(cacheKey).Result()
	if err == nil {
		var data PivotedHistory
		decompressedData, err := decompress([]byte(cachedDataStr))
		if err == nil && json.Unmarshal(decompressedData, &data) == nil {
			return &data, nil
		}
	}

	// 2. On cache miss, use singleflight to compute. This prevents thundering herds from all services.
	v, err, _ := dp.sfGroup.Do(cacheKey, func() (interface{}, error) {
		log.Printf("DataProvider: Cache miss for %s. Computing new pivoted history.", cacheKey)

		pivotedData, err := dp.computePivotedHistory(ctx, address)
		if err != nil {
			return nil, err
		}

		// Cache the result of this expensive operation.
		jsonData, err := json.Marshal(pivotedData)
		if err == nil {
			compressedData, err := compress(jsonData)
			if err == nil {
				dp.redisClient.Set(cacheKey, compressedData, pivotedHistoryCacheTTL)
			}
		}
		return pivotedData, nil
	})
	dp.sfGroup.Forget(cacheKey) // Allow re-computation after the single flight is done.

	if err != nil {
		return nil, err
	}
	return v.(*PivotedHistory), nil
}

// computePivotedHistory contains the memory-efficient streaming logic to build the shared data.
func (dp *DataProvider) computePivotedHistory(ctx context.Context, address string) (*PivotedHistory, error) {
	historyReader, err := dp.fetchHolderHistoryReader(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("could not fetch holder history reader: %w", err)
	}

	rawTimeSeries, addressMax := make(map[string]map[string]float64), make(map[string]float64)

	decoder := json.NewDecoder(historyReader)
	if _, err := decoder.Token(); err != nil {
		return nil, err
	} // {
	if _, err := decoder.Token(); err != nil {
		return nil, err
	} // "holders"
	if _, err := decoder.Token(); err != nil {
		return nil, err
	} // [

	for decoder.More() {
		var holder HolderHistoryEntry
		if err := decoder.Decode(&holder); err != nil {
			continue
		}
		currentAddressMax := addressMax[holder.Address]
		for i := 0; i < len(holder.Time); i++ {
			roundedTimeStr, err := roundTimeToInterval(holder.Time[i], defaultPivotingInterval)
			if err != nil {
				continue
			}
			amount := holder.Amount[i]
			if _, ok := rawTimeSeries[roundedTimeStr]; !ok {
				rawTimeSeries[roundedTimeStr] = make(map[string]float64)
			}
			rawTimeSeries[roundedTimeStr][holder.Address] = amount
			if amount > currentAddressMax {
				currentAddressMax = amount
			}
		}
		addressMax[holder.Address] = currentAddressMax
	}
	return &PivotedHistory{TimeSeries: rawTimeSeries, AddressMax: addressMax}, nil
}

// fetchHolderHistoryReader gets the raw data stream from Redis.
func (dp *DataProvider) fetchHolderHistoryReader(ctx context.Context, address string) (io.Reader, error) {
	redisKey := fmt.Sprintf("%s%s", holderHistoryRedisPrefix, address)
	jsonDataStr, err := dp.redisClient.Get(redisKey).Result()
	if err == redis.Nil {
		return strings.NewReader(`{"holders":[]}`), nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get error: %w", err)
	}
	jsonData, err := decompress([]byte(jsonDataStr))
	if err != nil {
		return nil, fmt.Errorf("decompress error: %w", err)
	}
	if bytes.HasPrefix(jsonData, []byte(`[`)) {
		var wrappedData bytes.Buffer
		wrappedData.WriteString(`{"holders":`)
		wrappedData.Write(jsonData)
		wrappedData.WriteString(`}`)
		return &wrappedData, nil
	}
	return bytes.NewReader(jsonData), nil
}
