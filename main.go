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
	"strings"
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

	outputFile := filepath.Join(exeDir, "output.csv")

	cwd, _ := os.Getwd()
	possiblePaths := []string{
		"kafka.txt",
		filepath.Join(exeDir, "kafka.txt"),
		filepath.Join(cwd, "kafka.txt"),
	}

	var file *os.File
	var foundPath string
	for _, p := range possiblePaths {
		info, err := os.Stat(p)
		if err == nil && !info.IsDir() {
			f, err := os.Open(p)
			if err == nil {
				file = f
				foundPath = p
				break
			}
		}
	}

	if file == nil {
		fmt.Printf("Could not automatically find kafka.txt.\nPlease paste the full, exact path to kafka.txt (must be the file, not a folder):\n> ")
		reader := bufio.NewReader(os.Stdin)
		inputPath, _ := reader.ReadString('\n')
		inputPath = strings.TrimSpace(inputPath)
		inputPath = strings.Trim(inputPath, "\"")
		inputPath = strings.Trim(inputPath, "'")

		info, err := os.Stat(inputPath)
		if err != nil || info.IsDir() {
			log.Printf("Failed to open path %s or it is a directory: %v", inputPath, err)
			fmt.Printf("\nError: The path provided does not exist or is a folder, not a file.\nPress Enter to exit...")
			bufio.NewReader(os.Stdin).ReadBytes('\n')
			os.Exit(1)
		}

		f, err := os.Open(inputPath)
		if err != nil {
			log.Printf("Failed to open custom path %s: %v", inputPath, err)
			fmt.Printf("Error: %v\nPress Enter to exit...", err)
			bufio.NewReader(os.Stdin).ReadBytes('\n')
			os.Exit(1)
		}
		file = f
		foundPath = inputPath
	}
	defer file.Close()

	log.Printf("Successfully opened input file: %s", foundPath)

	log.Printf("Creating output file: %s", outputFile)
	out, err := os.Create(outputFile)
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

	dec := json.NewDecoder(file)
	_, err = dec.Token()
	if err != nil {
		fmt.Printf("\nFATAL ERROR: Error reading JSON file: %v\nCheck if the file is a valid JSON array.\nPress Enter to exit...", err)
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		os.Exit(1)
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
	log.Printf("Successfully wrote %d records to %s", resultCount, outputFile)
	fmt.Printf("\nDone! Wrote %d records to output.csv.\nPress Enter to exit...", resultCount)
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}
