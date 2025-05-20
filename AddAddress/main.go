package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
)

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

func AddProxies() {
	var proxies []Proxy
	reader := bufio.NewReader(os.Stdin)

	// Define the file path
	outputPath := "../datacenter/proxies.json"

	// Check if the file exists
	if _, err := os.Stat(outputPath); err == nil {
		// Read existing data
		fileData, err := ioutil.ReadFile(outputPath)
		if err != nil {
			fmt.Println("Error reading existing file:", err)
			return
		}
		if err := json.Unmarshal(fileData, &proxies); err != nil {
			fmt.Println("Error parsing existing JSON:", err)
			return
		}
	}

	fmt.Println("Enter proxies one by one (type 'done' to finish):")

	for {
		fmt.Print("Enter Proxies (IP:PORT): ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			return
		}

		input = strings.TrimSpace(input) // Remove newline and extra spaces

		if input == "done" {
			break
		}

		if input != "" {
			proxies = append(proxies, Proxy{Address: input})
		}
	}

	if len(proxies) == 0 {
		fmt.Println("No proxies entered. Exiting.")
		return
	}

	// Create directories if they don't exist
	if err := os.MkdirAll("../datacenter", os.ModePerm); err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}

	// Open the JSON file for writing
	file, err := os.Create(outputPath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Encode the addresses to JSON and write to the file
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty-print JSON
	if err := encoder.Encode(proxies); err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

	fmt.Println("Proxies saved to", outputPath)
}
func AddCookies() {
	var cookies []Cookies
	reader := bufio.NewReader(os.Stdin)

	// Define the file path
	outputPath := "../datacenter/cookies.json"

	// Check if the file exists
	if _, err := os.Stat(outputPath); err == nil {
		// Read existing data
		fileData, err := ioutil.ReadFile(outputPath)
		if err != nil {
			fmt.Println("Error reading existing file:", err)
			return
		}
		if err := json.Unmarshal(fileData, &cookies); err != nil {
			fmt.Println("Error parsing existing JSON:", err)
			return
		}
	}

	fmt.Println("Enter Cookies one by one (type 'done' to finish):")

	for {
		fmt.Print("Enter Cookies: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			return
		}

		input = strings.TrimSpace(input) // Remove newline and extra spaces

		if input == "done" {
			break
		}

		if input != "" {
			cookies = append(cookies, Cookies{Address: input})
		}
	}

	if len(cookies) == 0 {
		fmt.Println("No Cookies entered. Exiting.")
		return
	}

	// Create directories if they don't exist
	if err := os.MkdirAll("../datacenter", os.ModePerm); err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}

	// Open the JSON file for writing
	file, err := os.Create(outputPath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Encode the addresses to JSON and write to the file
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty-print JSON
	if err := encoder.Encode(cookies); err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

	fmt.Println("Cookies saved to", outputPath)
}

func AddAddress() {
	var addresses []Address
	reader := bufio.NewReader(os.Stdin)

	// Define the file path
	outputPath := "../addresses/address.json"

	// Check if the file exists
	if _, err := os.Stat(outputPath); err == nil {
		// Read existing data
		fileData, err := ioutil.ReadFile(outputPath)
		if err != nil {
			fmt.Println("Error reading existing file:", err)
			return
		}
		if err := json.Unmarshal(fileData, &addresses); err != nil {
			fmt.Println("Error parsing existing JSON:", err)
			return
		}
	}

	fmt.Println("Enter addresses one by one (type 'done' to finish):")

	for {
		fmt.Print("Enter address: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			return
		}

		input = strings.TrimSpace(input) // Remove newline and extra spaces
		if input == "done" {
			break
		}
		fmt.Print("Enter Index Number: ")

		indexInput, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			return
		}

		indexInput = strings.TrimSpace(indexInput)

		index, err := strconv.ParseInt(indexInput, 10, 64)
		if err != nil {
			fmt.Println("Error reading input:", err)
			return
		}
		metaData := getTokenMetadata(input, index)
		if input != "" {
			addresses = append(addresses, Address{Address: input, Name: metaData.Name, Symbol: metaData.Symbol, Index: index})
		}
	}

	if len(addresses) == 0 {
		fmt.Println("No addresses entered. Exiting.")
		return
	}

	// Create directories if they don't exist
	if err := os.MkdirAll("../addresses", os.ModePerm); err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}

	// Open the JSON file for writing
	file, err := os.Create(outputPath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Encode the addresses to JSON and write to the file
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty-print JSON
	if err := encoder.Encode(addresses); err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

	fmt.Println("Addresses saved to", outputPath)
}
func getTokenMetadata(token string, index int64) *TokenMetadata {
	url := fmt.Sprintf("http://localhost:3300/api/token-metadata?mint=%s", token)

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error fetching data:", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Error reading response body:", err)
			return nil
		}

		var metadata TokenMetadata
		if err := json.Unmarshal(body, &metadata); err != nil {
			fmt.Println("Error unmarshaling JSON:", err)
			return nil
		}

		// Print the response
		fmt.Printf("Name: %s\nSymbol: %s\nURI: %s\nIndex: %d\n", metadata.Name, metadata.Symbol, metadata.URI, index)
		return &metadata
	} else {
		fmt.Printf("Failed to fetch data. Status code: %d\n", resp.StatusCode)
		return nil
	}
}
func main() {
	fmt.Println(`
Enter the Number for the Specific Purpose
1. Add Proxies
2. Add Cookies
3. Add Address
	`)
	fmt.Print("Enter Number: ")
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input:", err)
		return
	}
	input = strings.TrimSpace(input)
	numval, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	switch numval {
	case 1:
		AddProxies()
	case 2:
		AddCookies()
	case 3:
		AddAddress()
	default:
		fmt.Println("[][]Invalid Inputs:-", numval)
	}
}
