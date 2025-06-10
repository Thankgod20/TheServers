package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/smtp"
	"os"
	"time"
)

// Address defines the structure for a token address to be monitored.
// It's exported so the main package can create a slice of this type.
type Address struct {
	Address string `json:"address"`
	Name    string `json:"name"`
}

// --- Internal Data Structures ---

type ohlcvResponse struct {
	Data struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		Attributes any    `json:"attributes"` // Can be refined if needed
	} `json:"data"`
	Meta struct {
		PoolID string `json:"poolID"`
		// You can add other fields here like Base, Quote if needed
	} `json:"meta"`
}

type flowAnalyticsResponse struct {
	Inflow     flowData `json:"inflow"`
	Outflow    flowData `json:"outflow"`
	Netflow    flowData `json:"netflow"`
	TotalItems int      `json:"totalItems"`
}

type flowData struct {
	Retail []float64 `json:"retail"`
	Shark  []float64 `json:"shark"`
	Whale  []float64 `json:"whale"`
}

// NotifierConfig holds all the necessary configuration for the Notifier.
type NotifierConfig struct {
	APIbaseURL         string
	SMTPHost           string
	SMTPPort           int
	SMTPUser           string
	SMTPPassword       string
	EmailSenderName    string
	EmailRecipient     string
	WhaleFlowThreshold float64
	ExcludeAddrs       map[string]bool
}

// Notifier is our "class" that handles the entire notification process.
type Notifier struct {
	config     NotifierConfig
	httpClient *http.Client
}

// NewNotifier is the constructor for our Notifier class.
func NewNotifier(config NotifierConfig) *Notifier {
	return &Notifier{
		config: config,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}
func (n *Notifier) LoadExcludedAddresses(filePath string) error {
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
		n.config.ExcludeAddrs[addr] = true
	}

	log.Printf("Loaded %d excluded addresses from %s", len(n.config.ExcludeAddrs), filePath)
	return nil
}

// ProcessAddresses is the main entry point method. It iterates through addresses,
// performs analysis, and sends notifications.
func (n *Notifier) ProcessAddresses(addrs []Address) {
	log.Println("--- Starting notification processing cycle ---")
	for i := len(addrs) - 1; i >= 0; i-- {

		a := addrs[i]
		if n.config.ExcludeAddrs[a.Address] {
			log.Printf("[holders] skipping excluded address: %s", a.Address)
			continue
		}

		log.Printf("--- Processing %s (%s) ---", a.Name, a.Address)

		poolID, err := n.getPoolID(a.Address)
		if err != nil {
			log.Printf("[error] could not get pool ID for %s: %v", a.Address, err)
			continue
		}
		log.Printf("[info] Found Pool ID for %s: %s", a.Name, poolID)

		analytics, err := n.getFlowAnalytics(a.Address, poolID)
		if err != nil {
			log.Printf("[error] could not get flow analytics for %s: %v", a.Address, err)
			continue
		}

		if analytics.TotalItems == 0 {
			log.Printf("[info] No flow data available for %s", a.Name)
			continue
		}

		n.checkForRawDataAlerts(a, analytics)
		n.checkForMacdAlerts(a, analytics.Netflow.Whale)

		time.Sleep(2 * time.Second)
	}
	log.Println("--- Notification cycle complete ---")
}

// --- Internal Methods (Unexported) ---

func (n *Notifier) getPoolID(tokenAddress string) (string, error) {
	url := fmt.Sprintf("%s/get-ohlcv?poolId=%s", n.config.APIbaseURL, tokenAddress)
	resp, err := n.httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to call OHLCV API: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read OHLCV API response body: %w", err)
	}

	var ohlcvResp ohlcvResponse
	if err := json.Unmarshal(body, &ohlcvResp); err != nil {
		return "", fmt.Errorf("failed to unmarshal OHLCV JSON: %w", err)
	}
	//fmt.Println("ohlcvResp", string(body), "url", url)
	if ohlcvResp.Meta.PoolID == "" {
		return "", fmt.Errorf("poolID not found for address %s", tokenAddress)
	}
	return ohlcvResp.Meta.PoolID, nil
}

func (n *Notifier) getFlowAnalytics(address, lps string) (*flowAnalyticsResponse, error) {
	url := fmt.Sprintf("%s/api/flow-analytics?address=%s&lps=%s&page=1&limit=50", n.config.APIbaseURL, address, lps)
	resp, err := n.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to call flow analytics API: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read flow analytics API response body: %w", err)
	}

	var analyticsResp flowAnalyticsResponse
	if err := json.Unmarshal(body, &analyticsResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal flow analytics JSON: %w", err)
	}
	return &analyticsResp, nil
}

func (n *Notifier) sendEmail(subject, body string) {
	auth := smtp.PlainAuth("", n.config.SMTPUser, n.config.SMTPPassword, n.config.SMTPHost)

	from := fmt.Sprintf("%s <%s>", n.config.EmailSenderName, n.config.SMTPUser)
	to := []string{n.config.EmailRecipient}

	var msg bytes.Buffer
	msg.WriteString("From: " + from + "\r\n")
	msg.WriteString("To: " + n.config.EmailRecipient + "\r\n")
	msg.WriteString("Subject: " + subject + "\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(body)

	addr := fmt.Sprintf("%s:%d", n.config.SMTPHost, n.config.SMTPPort)
	err := smtp.SendMail(addr, auth, n.config.SMTPUser, to, msg.Bytes())
	if err != nil {
		log.Printf("[email] Failed to send email: %v", err)
	} else {
		log.Printf("[email] Notification sent successfully: %s", subject)
	}
}

func (n *Notifier) checkForRawDataAlerts(addr Address, data *flowAnalyticsResponse) {
	inflow := data.Inflow.Whale
	outflow := data.Outflow.Whale

	if len(inflow) < 3 || len(outflow) < 3 {
		return
	}

	lastIn := inflow[len(inflow)-1]
	prevIn := inflow[len(inflow)-2]
	prevvIn := inflow[len(inflow)-3]

	lastOut := outflow[len(outflow)-1]
	prevOut := outflow[len(outflow)-2]
	prevvOut := outflow[len(outflow)-3]

	if prevIn > n.config.WhaleFlowThreshold && lastIn > n.config.WhaleFlowThreshold && prevvIn < n.config.WhaleFlowThreshold {
		subject := fmt.Sprintf("🚨 Sustained Whale Inflow Alert for %s", addr.Name)
		body := fmt.Sprintf("Whale inflow above threshold for 2 periods.\n\n%s (%s)\nPrevious: %.2f\nCurrent:  %.2f",
			addr.Name, addr.Address, prevIn, lastIn)
		n.sendEmail(subject, body)
	}

	if prevOut > n.config.WhaleFlowThreshold && lastOut > n.config.WhaleFlowThreshold && prevvOut < n.config.WhaleFlowThreshold {
		subject := fmt.Sprintf("🚨 Sustained Whale Outflow Alert for %s", addr.Name)
		body := fmt.Sprintf("Whale outflow above threshold for 2 periods.\n\n%s (%s)\nPrevious: %.2f\nCurrent:  %.2f",
			addr.Name, addr.Address, prevOut, lastOut)
		n.sendEmail(subject, body)
	}
}

func (n *Notifier) checkForMacdAlerts(addr Address, netflow []float64) {
	macdLine, signalLine := calculateMACD(netflow)
	if len(macdLine) < 4 || len(signalLine) < 4 {
		log.Printf("[macd] Not enough data for MACD crossover analysis for %s", addr.Name)
		return
	}

	// Align lengths
	offset := len(macdLine) - len(signalLine)
	macdLine = macdLine[offset:]

	// Use the last 3 data points for crossover confirmation
	macdPrev2, macdPrev1, macdLast := macdLine[len(macdLine)-3], macdLine[len(macdLine)-2], macdLine[len(macdLine)-1]
	sigPrev2, sigPrev1, sigLast := signalLine[len(signalLine)-3], signalLine[len(signalLine)-2], signalLine[len(signalLine)-1]

	// Signal crossover: Confirmed 2-step bullish crossover
	if macdPrev2 < sigPrev2 && macdPrev1 > sigPrev1 && macdLast > sigLast {
		subject := fmt.Sprintf("📈 Confirmed Bullish MACD Crossover for %s", addr.Name)
		body := fmt.Sprintf("Token %s (%s): Confirmed bullish MACD crossover.\n\nMACD: %.4f → %.4f → %.4f\nSignal: %.4f → %.4f → %.4f", addr.Name, addr.Address, macdPrev2, macdPrev1, macdLast, sigPrev2, sigPrev1, sigLast)
		n.sendEmail(subject, body)
	}

	// Signal crossover: Confirmed 2-step bearish crossover
	if macdPrev2 > sigPrev2 && macdPrev1 < sigPrev1 && macdLast < sigLast {
		subject := fmt.Sprintf("📉 Confirmed Bearish MACD Crossover for %s", addr.Name)
		body := fmt.Sprintf("Token %s (%s): Confirmed bearish MACD crossover.\n\nMACD: %.4f → %.4f → %.4f\nSignal: %.4f → %.4f → %.4f", addr.Name, addr.Address, macdPrev2, macdPrev1, macdLast, sigPrev2, sigPrev1, sigLast)
		n.sendEmail(subject, body)
	}

	// Zero-line crossover alerts
	if macdPrev2 < 0 && macdPrev1 > 0 && macdLast > 0 {
		subject := fmt.Sprintf("📊 MACD Crossed Above Zero for %s", addr.Name)
		body := fmt.Sprintf("Token %s (%s): MACD line crossed above 0 and held.\n\nMACD: %.4f → %.4f → %.4f", addr.Name, addr.Address, macdPrev2, macdPrev1, macdLast)
		n.sendEmail(subject, body)
	}

	if macdPrev2 > 0 && macdPrev1 < 0 && macdLast < 0 {
		subject := fmt.Sprintf("📊 MACD Crossed Below Zero for %s", addr.Name)
		body := fmt.Sprintf("Token %s (%s): MACD line crossed below 0 and held.\n\nMACD: %.4f → %.4f → %.4f", addr.Name, addr.Address, macdPrev2, macdPrev1, macdLast)
		n.sendEmail(subject, body)
	}
}

// --- Pure Utility Functions (Unexported) ---

func calculateEMA(data []float64, period int) []float64 {
	if len(data) < period {
		return nil
	}
	ema := make([]float64, len(data)-period+1)
	multiplier := 2.0 / (float64(period) + 1.0)
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += data[i]
	}
	ema[0] = sum / float64(period)
	for i := period; i < len(data); i++ {
		ema[i-period+1] = (data[i]-ema[i-period])*multiplier + ema[i-period]
	}
	return ema
}

func calculateMACD(data []float64) (macdLine, signalLine []float64) {
	shortPeriod, longPeriod, signalPeriod := 12, 26, 9
	if len(data) < longPeriod {
		return nil, nil
	}
	ema12 := calculateEMA(data, shortPeriod)
	ema26 := calculateEMA(data, longPeriod)
	offset := len(ema12) - len(ema26)
	macdLine = make([]float64, len(ema26))
	for i := 0; i < len(ema26); i++ {
		macdLine[i] = ema12[i+offset] - ema26[i]
	}
	if len(macdLine) < signalPeriod {
		return macdLine, nil
	}
	signalLine = calculateEMA(macdLine, signalPeriod)
	return macdLine, signalLine
}
