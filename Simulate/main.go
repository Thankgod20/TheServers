package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"sort"
	"time"
)

// OHLCV represents one candle of OHLCV data.
type OHLCV struct {
	Timestamp int64 // Unix timestamp (seconds)
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

// parseOHLCVList converts raw [][]interface{} (from JSON) into []OHLCV.
func parseOHLCVList(raw [][]interface{}) ([]OHLCV, error) {
	log.Printf("Stage: Parsing raw OHLCV list with %d rows\n", len(raw))
	out := make([]OHLCV, 0, len(raw))
	for idx, row := range raw {
		if len(row) != 6 {
			return nil, fmt.Errorf("row %d: expected 6 elements, got %d", idx, len(row))
		}

		tFloat, ok0 := row[0].(float64)
		o, ok1 := row[1].(float64)
		h, ok2 := row[2].(float64)
		l, ok3 := row[3].(float64)
		c, ok4 := row[4].(float64)
		v, ok5 := row[5].(float64)
		if !(ok0 && ok1 && ok2 && ok3 && ok4 && ok5) {
			return nil, fmt.Errorf("row %d: malformed numeric value", idx)
		}

		out = append(out, OHLCV{
			Timestamp: int64(tFloat),
			Open:      o,
			High:      h,
			Low:       l,
			Close:     c,
			Volume:    v,
		})
	}
	log.Printf("Stage: Parsed OHLCV list into %d candles\n", len(out))
	return out, nil
}

// isDeadSlice checks if, for a slice ending at index `endIdx`, the coin is "dead" over the last windowSize candles.
//  1. "Dip 80% below": last close <= 20% of first close in the 10-candle window.
//  2. Low volatility: standard deviation of close prices over the window, divided by mean, <= stdDevPctThresh.
//  3. Rug detection: if any single candle in the window has (close-open)/open <= -0.30, return true immediately.
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

func main() {
	log.Println("Stage: Starting dead-coin detection program")

	// Read JSON file
	log.Println("Stage: Reading file price.json")
	bytes, err := ioutil.ReadFile("price.json")
	if err != nil {
		log.Fatalf("Error: failed to read price.json: %v", err)
	}
	log.Printf("Stage: Read %d bytes from price.json\n", len(bytes))

	// Unmarshal JSON
	log.Println("Stage: Unmarshaling JSON")
	var raw struct {
		Data struct {
			Attributes struct {
				OHLCVList [][]interface{} `json:"ohlcv_list"`
			} `json:"attributes"`
		} `json:"data"`
	}
	if err := json.Unmarshal(bytes, &raw); err != nil {
		log.Fatalf("Error: failed to unmarshal JSON: %v", err)
	}
	log.Printf("Stage: JSON unmarshaled; found %d raw OHLCV rows\n", len(raw.Data.Attributes.OHLCVList))

	// Parse into []OHLCV
	ohlcvSlice, err := parseOHLCVList(raw.Data.Attributes.OHLCVList)
	if err != nil {
		log.Fatalf("Error: parsing OHLCV list: %v", err)
	}

	// Sort by timestamp ascending
	log.Println("Stage: Sorting OHLCV by timestamp")
	sort.Slice(ohlcvSlice, func(i, j int) bool {
		return ohlcvSlice[i].Timestamp < ohlcvSlice[j].Timestamp
	})
	log.Println("Stage: Sorting complete")
	//fmt.Println("→candles loaded from price.json\n", ohlcvSlice)
	// Detection parameters
	windowSize := 10
	priceDropPct := 0.80
	stdDevPctThresh := 0.02
	rugDropPct := 0.70
	log.Printf(
		"Stage: Detection params → windowSize=%d, priceDropPct=%.2f%%, stdDevPctThresh=%.2f%%, rugDropPct=%.2f%%\n",
		windowSize, priceDropPct*100, stdDevPctThresh*100, rugDropPct*100,
	)

	// Detection loop
	log.Println("Stage: Starting dead-coin detection loop")
	deadTimestamp := int64(0)
	for idx := range ohlcvSlice {
		log.Printf("Stage: Checking index %d (timestamp %d)\n", idx, ohlcvSlice[idx].Timestamp)
		if isDeadSlice(ohlcvSlice, idx, windowSize, priceDropPct, stdDevPctThresh, rugDropPct) {
			deadTimestamp = ohlcvSlice[idx].Timestamp
			log.Printf("Stage: Dead coin detected at index %d (timestamp %d)\n", idx, deadTimestamp)
			break
		}
	}
	log.Println("Stage: Detection loop finished")

	if deadTimestamp == 0 {
		log.Println("→ No “dead” point found in the given data (under the chosen criteria).")
		return
	}

	// Convert and print timestamp
	loc, err := time.LoadLocation("Africa/Lagos")
	if err != nil {
		log.Printf("Warning: failed to load Africa/Lagos timezone: %v. Falling back to UTC.\n", err)
		loc = time.UTC
	}
	t := time.Unix(deadTimestamp, 0).In(loc)
	log.Println("Stage: Converting timestamp to human-readable format")
	fmt.Printf("→ Coin considered dead at Unix timestamp %d → %s (Africa/Lagos)\n",
		deadTimestamp, t.Format("2006-01-02 15:04:05 MST"))
}
