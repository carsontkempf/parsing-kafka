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
	"path/filepath"
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

type Cluster struct {
	ClusterId   string `json:"clusterId"`
	DisplayName string `json:"displayName"`
}

type ClusterResponse struct {
	Clusters []Cluster `json:"clusters"`
}

type Result struct {
	ClusterName     string
	Name            string
	PartitionsCount int
	LastProduced    string
}

func main() {
	exePath, _ := os.Executable()
	exeDir := filepath.Dir(exePath)

	logFile, err := os.OpenFile(filepath.Join(exeDir, "execution.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.SetOutput(logFile)
	}
	defer logFile.Close()

	defaultOutput := filepath.Join(exeDir, "output.csv")

	inputFile := flag.String("input", "C:/Users/p3293326/OneDrive - Charter Communications/Documents/Apps/Notepad++Portable/Notes/kafka.txt", "Input TXT file")
	outputFile := flag.String("output", defaultOutput, "Output CSV file")
	flag.Parse()

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
	writer.Write([]string{"Cluster Name", "Topic Name", "Partitions", "Last Produced"})

	client := &http.Client{Timeout: 10 * time.Second}

	log.Println("Fetching cluster IDs...")
	req, _ := http.NewRequest("GET", "http://198.19.144.5:9021/2.0/clusters/kafka/display/CLUSTER_MANAGEMENT", nil)
	req.Header.Set("Authorization", "Basic QVBPU0NVU0VSOm4wV1F0QWlyQVk=")
	req.Header.Set("Cookie", "JSESSIONID=node01jtj7kg5pz9anjpy90cjz8htk9099.node0")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Failed to fetch clusters:", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var clusterResp ClusterResponse
	if err := json.Unmarshal(body, &clusterResp); err != nil {
		log.Fatal("Failed to parse clusters:", err)
	}
	log.Printf("Found %d clusters", len(clusterResp.Clusters))

	dec := json.NewDecoder(file)
	_, err = dec.Token()
	if err != nil {
		log.Fatal("Error reading JSON array start:", err)
	}

	type Job struct {
		Topic   Topic
		Cluster Cluster
	}

	jobs := make(chan Job, 1000)
	results := make(chan Result, 1000)
	var wg sync.WaitGroup

	log.Println("Starting 50 concurrent HTTP workers...")
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range jobs {
				log.Printf("[Worker %d] Fetching lastProduceTime for topic: %s on cluster: %s", workerID, j.Topic.Name, j.Cluster.DisplayName)
				req, _ := http.NewRequest("GET", fmt.Sprintf("http://198.19.144.5:9021/2.0/kafka/%s/topics/%s/lastProduceTime", j.Cluster.ClusterId, j.Topic.Name), nil)
				req.Header.Set("Authorization", "Basic QVBPU0NVU0VSOm4wV1F0QWlyQVk=")
				req.Header.Set("Cookie", "JSESSIONID=node01jtj7kg5pz9anjpy90cjz8htk9099.node0")

				resp, err := client.Do(req)
				lastProduced := ""
				if err != nil {
					log.Printf("[Worker %d] Error fetching topic %s on cluster %s: %v", workerID, j.Topic.Name, j.Cluster.DisplayName, err)
				} else {
					body, _ := io.ReadAll(resp.Body)
					val := string(body)
					if val != "-1" && val != "" {
						lastProduced = val
					} else {
						log.Printf("[Worker %d] Topic %s on %s returned blank or -1", workerID, j.Topic.Name, j.Cluster.DisplayName)
					}
					resp.Body.Close()
				}

				results <- Result{
					ClusterName:     j.Cluster.DisplayName,
					Name:            j.Topic.Name,
					PartitionsCount: len(j.Topic.Partitions),
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
			for _, c := range clusterResp.Clusters {
				jobs <- Job{Topic: t, Cluster: c}
			}
			topicCount++
			if topicCount%1000 == 0 {
				log.Printf("Queued %d topics across %d clusters for processing...", topicCount, len(clusterResp.Clusters))
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
		writer.Write([]string{r.ClusterName, r.Name, strconv.Itoa(r.PartitionsCount), r.LastProduced})
		resultCount++
	}
	writer.Flush()
	log.Printf("Successfully wrote %d records to %s", resultCount, *outputFile)
}
