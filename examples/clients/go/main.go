// Command go reads a datafeed feed: two pages, then a stream that survives dropped connections.
package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

var (
	server = cmp.Or(os.Getenv("DATAFEED_SERVER"), "http://127.0.0.1:8080")
	token  = os.Getenv("DATAFEED_TOKEN")
)

func get(feed, path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, server+"/api/v1/feeds/"+feed+"/"+path, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func id(line string) string {
	var record struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		log.Fatal(err)
	}
	return record.ID
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: go run ./examples/clients/go <feed-id>")
	}
	feed := os.Args[1]

	// Pages: carry on from X-Next-Offset, which is absent at the end.
	offset := "0"
	for i := 0; i < 2 && offset != ""; i++ {
		resp, err := get(feed, "records?limit=3&offset="+offset)
		if err != nil {
			log.Fatal(err)
		}
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			fmt.Println("page:", id(scanner.Text()))
		}
		resp.Body.Close()
		offset = resp.Header.Get("X-Next-Offset")
	}

	// Stream: the position is the start plus the records received, so a dropped connection
	// picks up exactly where it stopped. Stops here after 5 records.
	position := 0
	for position < 5 {
		resp, err := get(feed, fmt.Sprintf("stream?from=%d&rate=10", position))
		if err != nil {
			log.Println("reconnecting after:", err)
			continue
		}
		scanner := bufio.NewScanner(resp.Body)
		for position < 5 && scanner.Scan() {
			fmt.Println("stream:", position, id(scanner.Text()))
			position++
		}
		resp.Body.Close()
	}
}
