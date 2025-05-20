package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gorilla/websocket"
)

// WSMessage represents a generic JSON-RPC message from the WebSocket.
type WSMessage struct {
	Jsonrpc string          `json:"jsonrpc"`
	Id      int             `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// LogSubscriptionParams wraps the "result" field in a logs subscription message.
type LogSubscriptionParams struct {
	Result LogResult `json:"result"`
}

// LogResult contains logs and the transaction signature.
type LogResult struct {
	Logs      []string `json:"logs"`
	Signature string   `json:"signature"`
}

// getTokenPrice simulates a token price lookup. Replace with a real API call.
func getTokenPrice(tokenMint string) (float64, error) {
	// For demonstration, assume each token is priced at $0.05.
	return 0.05, nil
}

// pollTokenMarketCap continuously checks a token's supply (via RPC) and computes market cap.
// It polls every pollInterval seconds for up to maxDuration.
// When market cap >= threshold, it logs the token details.
func pollTokenMarketCap(tokenMint string, rpcClient *rpc.Client, threshold float64, pollInterval time.Duration, maxDuration time.Duration) {
	timeout := time.After(maxDuration)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			log.Printf("Token %s did not hit $%.2f market cap within the allotted time", tokenMint, threshold)
			return
		case <-ticker.C:
			supplyResp, err := rpcClient.GetTokenSupply(
				context.Background(),
				solana.MustPublicKeyFromBase58(tokenMint),
				rpc.CommitmentFinalized,
			)
			if err != nil {
				log.Printf("Error getting token supply for %s: %v", tokenMint, err)
				continue
			}
			supplyStr := supplyResp.Value.Amount
			var supply float64
			if _, err := fmt.Sscanf(supplyStr, "%f", &supply); err != nil {
				log.Printf("Error parsing supply for %s: %v", tokenMint, err)
				continue
			}

			price, err := getTokenPrice(tokenMint)
			if err != nil {
				log.Printf("Error getting price for %s: %v", tokenMint, err)
				continue
			}

			marketCap := supply * price
			log.Printf("Token %s: Supply=%f, Price=$%f, Market Cap=$%f", tokenMint, supply, price, marketCap)
			if marketCap >= threshold {
				log.Printf(">>> Token %s has hit the market cap threshold! (Market Cap: $%f)", tokenMint, marketCap)
				// Add additional actions here (e.g., notifications, database updates, etc.)
				return
			}
		}
	}
}

func main() {
	// Use free public Solana endpoints.
	wsURL := "wss://api.mainnet-beta.solana.com"
	httpURL := "https://api.mainnet-beta.solana.com"

	// Connect to the WebSocket endpoint.
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatal("WebSocket dial error:", err)
	}
	defer conn.Close()
	log.Println("Connected to Solana RPC WebSocket")

	// Subscribe to logs that mention the SPL Token program.
	subscribeReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "logsSubscribe",
		"params": []interface{}{
			map[string]interface{}{
				"mentions": []string{"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
			},
			map[string]interface{}{
				"commitment": "confirmed",
				"encoding":   "jsonParsed",
			},
		},
	}
	if err := conn.WriteJSON(subscribeReq); err != nil {
		log.Fatal("Error sending subscribe request:", err)
	}
	log.Println("Subscribed to logs for SPL Token program events")

	// Create an HTTP RPC client.
	rpcClient := rpc.New(httpURL)

	// Continuously read messages from the WebSocket.
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		var wsResp WSMessage
		if err := json.Unmarshal(message, &wsResp); err != nil {
			log.Println("Error unmarshaling message:", err)
			continue
		}

		// Ensure the message has params.
		if len(wsResp.Params) == 0 {
			continue
		}

		var params LogSubscriptionParams
		if err := json.Unmarshal(wsResp.Params, &params); err != nil {
			log.Println("Error unmarshaling params:", err)
			continue
		}

		// Look for logs that indicate a new token mint.
		for _, logLine := range params.Result.Logs {
			if strings.Contains(logLine, "InitializeMint") {
				// Extract token mint address from the log.
				tokenMint := extractTokenMint(logLine)
				if tokenMint == "" {
					continue
				}
				log.Printf("Detected new token mint event for token: %s", tokenMint)
				// Start a goroutine to poll this token's market cap.
				go pollTokenMarketCap(tokenMint, rpcClient, 100000.0, 10*time.Second, 5*time.Minute)
			}
		}
	}
}

// extractTokenMint is a dummy parser that assumes the log is formatted as:
// "Program log: InitializeMint: <tokenMint>"
// Adjust this function based on the actual log format.
func extractTokenMint(logLine string) string {
	parts := strings.Split(logLine, ":")
	if len(parts) < 3 {
		return ""
	}
	tokenMint := strings.TrimSpace(parts[2])
	// A typical Solana public key is 44 characters in base58.
	if len(tokenMint) == 44 {
		return tokenMint
	}
	return ""
}
