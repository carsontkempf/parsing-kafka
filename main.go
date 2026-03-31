package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Topic struct {
	Name       string `json:"name"`
	Partitions []struct {
		Partition int `json:"partition"`
	} `json:"partitions"`
}

type Result struct {
	Name            string
	PartitionsCount int
	LastProduced    string
}

func main() {
	inputFile := flag.String("input", "C:/Users/p3293326/OneDrive - Charter Communications/Documents/Apps/Notepad++Portable/Notes/kafka.txt", "Input TXT file")
	outputFile := flag.String("output", "output.csv", "Output CSV file")
	clusterID := flag.String("cluster", "", "Kafka Cluster ID")
	flag.Parse()

	if *clusterID == "" {
		log.Fatal("Usage: program -cluster <cluster_id> [-output <out.csv>]")
	}

	log.Printf("Opening input file: %s", *inputFile)
	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	log.Printf("Creating output file: %s", *outputFile)
	out, err := os.Create(*outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	writer := csv.NewWriter(out)
	writer.Write([]string{"Topic Name", "Partitions", "Last Produced"})

	dec := json.NewDecoder(file)
	_, err = dec.Token()
	if err != nil {
		log.Fatal("Error reading JSON array start:", err)
	}

	jobs := make(chan Topic, 1000)
	results := make(chan Result, 1000)
	var wg sync.WaitGroup

	client := &http.Client{Timeout: 10 * time.Second}

	log.Println("Starting 50 concurrent HTTP workers...")
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for t := range jobs {
				log.Printf("[Worker %d] Fetching lastProduceTime for topic: %s", workerID, t.Name)
				req, _ := http.NewRequest("GET", fmt.Sprintf("http://198.19.144.5:9021/2.0/kafka/%s/topics/%s/lastProduceTime", *clusterID, t.Name), nil)
				req.Header.Set("Authorization", "Basic QVBPU0NVU0VSOm4wV1F0QWlyQVk=")
				req.Header.Set("Cookie", "JSESSIONID=node01jtj7kg5pz9anjpy90cjz8htk9099.node0")

				resp, err := client.Do(req)
				lastProduced := ""
				if err != nil {
					log.Printf("[Worker %d] Error fetching topic %s: %v", workerID, t.Name, err)
				} else {
					body, _ := io.ReadAll(resp.Body)
					val := string(body)
					if val != "-1" && val != "" {
						lastProduced = val
					} else {
						log.Printf("[Worker %d] Topic %s returned blank or -1", workerID, t.Name)
					}
					resp.Body.Close()
				}

				results <- Result{
					Name:            t.Name,
					PartitionsCount: len(t.Partitions),
					LastProduced:    lastProduced,
				}
			}
		}(i)
	}

	go func() {
		topicCount := 0
		for dec.More() {
			var t Topic
			err := dec.Decode(&t)
			if err != nil {
				log.Printf("Error decoding topic object: %v", err)
				continue
			}
			jobs <- t
			topicCount++
			if topicCount%1000 == 0 {
				log.Printf("Queued %d topics for processing...", topicCount)
			}
		}
		log.Printf("Finished queuing all %d topics. Waiting for workers...", topicCount)
		close(jobs)
		wg.Wait()
		log.Println("All workers finished. Closing results channel...")
		close(results)
	}()

	resultCount := 0
	for r := range results {
		writer.Write([]string{r.Name, strconv.Itoa(r.PartitionsCount), r.LastProduced})
		resultCount++
	}
	writer.Flush()
	log.Printf("Successfully wrote %d records to %s", resultCount, *outputFile)
}
