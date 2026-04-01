package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
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

type Cluster struct {
	ClusterId   string `json:"clusterId"`
	DisplayName string `json:"displayName"`
}

type ClusterResponse struct {
	Clusters []Cluster `json:"clusters"`
}

type Topic struct {
	Name       string `json:"name"`
	Partitions []struct {
		Partition int `json:"partition"`
	} `json:"partitions"`
}

type Job struct {
	Topic   Topic
	Cluster Cluster
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

	configs, err := LoadConfig(filepath.Join(exeDir, "config.json"))
	if err != nil {
		fmt.Printf("\nFATAL ERROR: Could not load config.json: %v\nPress Enter to exit...", err)
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}

	selectedEnv, err := PromptEnvironmentSelection(os.Stdin, configs)
	if err != nil {
		fmt.Printf("\nFATAL ERROR: %v\nPress Enter to exit...", err)
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}

	logFile, err := os.OpenFile(filepath.Join(exeDir, fmt.Sprintf("%s_execution.log", selectedEnv.Name)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.SetOutput(logFile)
	}
	defer logFile.Close()

	outputFile := filepath.Join(exeDir, fmt.Sprintf("%sdata.csv", selectedEnv.Name))
	log.Printf("Creating output file: %s", outputFile)

	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("\nFATAL ERROR: Could not create output file: %v\nPress Enter to exit...", err)
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}
	defer out.Close()

	writer := csv.NewWriter(out)
	writer.Write([]string{"Cluster Name", "Topic Name", "Partitions", "Last Produced"})

	client := &http.Client{Timeout: 15 * time.Second}

	log.Println("Fetching cluster IDs...")
	reqUrl := fmt.Sprintf("%s/2.0/clusters/kafka/display/CLUSTER_MANAGEMENT", selectedEnv.BaseUrl)
	req, _ := http.NewRequest("GET", reqUrl, nil)
	req.Header.Set("Authorization", selectedEnv.AuthHeader)
	req.Header.Set("Cookie", selectedEnv.CookieHeader)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("\nFATAL ERROR: Failed to fetch clusters: %v\nPress Enter to exit...", err)
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var clusterResp ClusterResponse
	if err := json.Unmarshal(body, &clusterResp); err != nil {
		fmt.Printf("\nFATAL ERROR: Failed to parse clusters: %v\nPress Enter to exit...", err)
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
	}
	log.Printf("Found %d clusters", len(clusterResp.Clusters))

	jobs := make(chan Job, 10000)
	results := make(chan Result, 10000)
	var wg sync.WaitGroup

	log.Println("Starting 50 concurrent HTTP workers...")
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range jobs {
				log.Printf("[Worker %d] Fetching lastProduceTime for topic: %s on cluster: %s", workerID, j.Topic.Name, j.Cluster.DisplayName)
				reqUrl := fmt.Sprintf("%s/2.0/kafka/%s/topics/%s/lastProduceTime", selectedEnv.BaseUrl, j.Cluster.ClusterId, j.Topic.Name)
				req, _ := http.NewRequest("GET", reqUrl, nil)
				req.Header.Set("Authorization", selectedEnv.AuthHeader)
				req.Header.Set("Cookie", selectedEnv.CookieHeader)

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
		totalTopics := 0
		for _, c := range clusterResp.Clusters {
			log.Printf("Fetching topics for cluster: %s (%s)", c.DisplayName, c.ClusterId)
			reqUrl := fmt.Sprintf("%s/2.0/kafka/%s/topics?includeAuthorizedOperations=false", selectedEnv.BaseUrl, c.ClusterId)
			req, _ := http.NewRequest("GET", reqUrl, nil)
			req.Header.Set("Authorization", selectedEnv.AuthHeader)
			req.Header.Set("Cookie", selectedEnv.CookieHeader)

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error fetching topics for cluster %s: %v", c.DisplayName, err)
				continue
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var topics []Topic
			if err := json.Unmarshal(body, &topics); err != nil {
				log.Printf("Error parsing topics for cluster %s: %v", c.DisplayName, err)
				continue
			}

			for _, t := range topics {
				jobs <- Job{Topic: t, Cluster: c}
				totalTopics++
			}
			log.Printf("Queued %d topics for cluster %s", len(topics), c.DisplayName)
		}
		log.Printf("Finished queuing all %d topics across %d clusters. Waiting for workers...", totalTopics, len(clusterResp.Clusters))
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

	log.Printf("Successfully wrote %d records to %s", resultCount, outputFile)
	fmt.Printf("\nDone! Wrote %d records to output.csv.\nPress Enter to exit...", resultCount)
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}
