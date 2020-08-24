package es

import (
	"bytes"
	"cl-tools/reporter"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"log"
	"os"
	"sync"
)

type ES interface {
	CheckHeights(start int64, end int64, workers int)
	BalanceDiff(a string, b string)
}

type es struct {
	reporter reporter.Reporter
	client   *elasticsearch.Client
}

func InitES(url string, rep reporter.Reporter) ES {
	cfg := elasticsearch.Config{
		Addresses: []string{url},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		exitErrorf("Unable to open an elasticsearch connection, %v", err)
	}

	return &es{
		reporter: rep,
		client:   client,
	}
}

func (e *es) worker(jobChan <-chan int64, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobChan {
		e.checkHeight(job)
	}
}

func (e *es) CheckHeights(start int64, end int64, workers int) {
	var wg sync.WaitGroup

	jobChan := make(chan int64)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go e.worker(jobChan, &wg)
	}

	for i := start; i <= end; i = i + 2 {
		jobChan <- i
	}

	close(jobChan)
	wg.Wait()

	fmt.Printf("\n ** Checked heights: %d (%d - %d)\n", (end-start)/2+1, start, end)
}

func (e *es) checkHeight(height int64) (int64, bool) {
	fmt.Printf("Check %d\n", height)
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"height": height,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		exitErrorf("Error encoding query: %v", err)
	}

	res, err := e.client.Search(
		e.client.Search.WithContext(context.Background()),
		e.client.Search.WithIndex("snapshots"),
		e.client.Search.WithBody(&buf),
		e.client.Search.WithTrackTotalHits(true),
		e.client.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		e.reporter.ReportESGap(height)
		return height, false
	}

	var (
		r map[string]interface{}
	)

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		e.reporter.ReportESGap(height)
		return height, false
	}

	hits := int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))

	if hits == 1 {
		return height, true
	}

	e.reporter.ReportESGap(height)
	return height, false
}

func (e *es) BalanceDiff(a string, b string) {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"_id": a,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		exitErrorf("Error encoding query: %v", err)
	}

	res, err := e.client.Search(
		e.client.Search.WithContext(context.Background()),
		e.client.Search.WithIndex("balances"),
		e.client.Search.WithBody(&buf),
		e.client.Search.WithTrackTotalHits(true),
		e.client.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		exitErrorf("Response error")
	}

	var (
		r map[string]interface{}
	)

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		exitErrorf("Decode error, %v", err)
	}

	//hits := int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
	//
	//for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
	//	doc := hit.(map[string]interface{})["_source"].(map[string]interface{})
	//	for address, value := range doc {
	//		balance := value.(map[string]float32)
	//		fmt.Printf("Address: %s, Balance: %f, Rewards: %f", address, balance["balance"], balance["rewards"])
	//	}
	//}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
