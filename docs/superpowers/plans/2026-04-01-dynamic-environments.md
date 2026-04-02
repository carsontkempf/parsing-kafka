# Extensible Environment Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the Go application to read a `config.json` file, display an interactive menu, and dynamically fetch Kafka topics based on the selected environment's credentials and URL.

**Architecture:** A `Config` struct will model the `config.json` schema. The application will initialize by parsing this file. A prompt function, decoupled from `os.Stdin` for testability, will request user input. The selected configuration will dictate the `BaseUrl`, `AuthHeader`, `CookieHeader`, and output filenames.

**Tech Stack:** Go (Standard Library only)

---

### Task 1: Define Configuration Schema and Parsing Logic

**Files:**
- Create: `config.go`
- Create: `config_test.go`
- Create: `config.json`

- [ ] **Step 1: Write the failing test for parsing config**

```go
// config_test.go
package main

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "config-*.json")
	defer os.Remove(tmpFile.Name())
	
	jsonContent := `[{"name":"testenv", "display":"Test Environment", "baseUrl":"http://localhost:9021", "authHeader":"Basic auth", "cookieHeader":"Cookie test"}]`
	os.WriteFile(tmpFile.Name(), []byte(jsonContent), 0644)

	configs, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(configs))
	}

	if configs[0].Name != "testenv" || configs[0].BaseUrl != "http://localhost:9021" {
		t.Errorf("config parsed incorrectly: %+v", configs[0])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v config_test.go config.go` (will fail as files/funcs don't exist)
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```go
// config.go
package main

import (
	"encoding/json"
	"os"
)

type EnvironmentConfig struct {
	Name         string `json:"name"`
	Display      string `json:"display"`
	BaseUrl      string `json:"baseUrl"`
	AuthHeader   string `json:"authHeader"`
	CookieHeader string `json:"cookieHeader"`
}

func LoadConfig(filepath string) ([]EnvironmentConfig, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	var configs []EnvironmentConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, err
	}
	return configs, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v config_test.go config.go`
Expected: PASS

- [ ] **Step 5: Create a default `config.json`**

```json
[
  {
    "name": "ncw",
    "display": "NCW Environment",
    "baseUrl": "http://198.19.144.5:9021",
    "authHeader": "Basic QVBPU0NVU0VSOm4wV1F0QWlyQVk=",
    "cookieHeader": "JSESSIONID=node01jtj7kg5pz9anjpy90cjz8htk9099.node0"
  },
  {
    "name": "nce",
    "display": "NCE Environment",
    "baseUrl": "http://YOUR_NCE_IP:9021",
    "authHeader": "Basic YOUR_NCE_AUTH",
    "cookieHeader": "JSESSIONID=YOUR_NCE_COOKIE"
  }
]
```

- [ ] **Step 6: Commit**

Run:
```bash
git add config.go config_test.go config.json
git commit -m "feat: add environment configuration parsing"
```

---

### Task 2: Implement Interactive Prompt

**Files:**
- Create: `prompt.go`
- Create: `prompt_test.go`

- [ ] **Step 1: Write the failing test for the prompt**

```go
// prompt_test.go
package main

import (
	"strings"
	"testing"
)

func TestPromptEnvironmentSelection(t *testing.T) {
	configs := []EnvironmentConfig{
		{Name: "env1", Display: "Env 1"},
		{Name: "env2", Display: "Env 2"},
	}

	// Test valid input "2"
	input := "2\n"
	r := strings.NewReader(input)
	
	selected, err := PromptEnvironmentSelection(r, configs)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if selected.Name != "env2" {
		t.Errorf("expected env2, got %s", selected.Name)
	}
}

func TestPromptEnvironmentSelectionInvalid(t *testing.T) {
	configs := []EnvironmentConfig{
		{Name: "env1", Display: "Env 1"},
	}

	input := "3\n"
	r := strings.NewReader(input)
	
	_, err := PromptEnvironmentSelection(r, configs)
	if err == nil {
		t.Fatal("expected error for invalid input, got nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v prompt_test.go prompt.go config.go`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```go
// prompt.go
package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func PromptEnvironmentSelection(r io.Reader, configs []EnvironmentConfig) (*EnvironmentConfig, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("no configurations available")
	}

	fmt.Println("Select environment:")
	for i, cfg := range configs {
		fmt.Printf("%d. %s (%s)\n", i+1, cfg.Display, cfg.Name)
	}
	fmt.Print("Enter your choice: ")

	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		input := strings.TrimSpace(scanner.Text())
		choice, err := strconv.Atoi(input)
		if err != nil || choice < 1 || choice > len(configs) {
			return nil, fmt.Errorf("invalid selection")
		}
		return &configs[choice-1], nil
	}
	return nil, fmt.Errorf("failed to read input")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v prompt_test.go prompt.go config.go`
Expected: PASS

- [ ] **Step 5: Commit**

Run:
```bash
git add prompt.go prompt_test.go
git commit -m "feat: add interactive environment selection prompt"
```

---

### Task 3: Integrate Configuration into Main Application

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Update main.go initialization and configuration loading**

Replace the beginning of `main()` in `main.go` to load `config.json` and prompt the user.

```go
// In main.go (replace from start of main() up to out, err := os.Create(outputFile))

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
```

- [ ] **Step 2: Update hardcoded URLs and Headers in `main.go`**

Find all `http.NewRequest` calls in `main.go` (there are 3) and update them.

*Call 1 (Clusters):*
```go
	log.Println("Fetching cluster IDs...")
	reqUrl := fmt.Sprintf("%s/2.0/clusters/kafka/display/CLUSTER_MANAGEMENT", selectedEnv.BaseUrl)
	req, _ := http.NewRequest("GET", reqUrl, nil)
	req.Header.Set("Authorization", selectedEnv.AuthHeader)
	req.Header.Set("Cookie", selectedEnv.CookieHeader)
```

*Call 2 (Workers fetching lastProduceTime):*
```go
				reqUrl := fmt.Sprintf("%s/2.0/kafka/%s/topics/%s/lastProduceTime", selectedEnv.BaseUrl, j.Cluster.ClusterId, j.Topic.Name)
				req, _ := http.NewRequest("GET", reqUrl, nil)
				req.Header.Set("Authorization", selectedEnv.AuthHeader)
				req.Header.Set("Cookie", selectedEnv.CookieHeader)
```

*Call 3 (Topics):*
```go
			reqUrl := fmt.Sprintf("%s/2.0/kafka/%s/topics?includeAuthorizedOperations=false", selectedEnv.BaseUrl, c.ClusterId)
			req, _ := http.NewRequest("GET", reqUrl, nil)
			req.Header.Set("Authorization", selectedEnv.AuthHeader)
			req.Header.Set("Cookie", selectedEnv.CookieHeader)
```

- [ ] **Step 3: Run full build and test**

Run: `go build -o kafka_parser.exe main.go config.go prompt.go`
Run: `go test -v ./...`
Expected: Build succeeds and tests PASS.

- [ ] **Step 4: Commit**

Run:
```bash
git add main.go
git commit -m "refactor: integrate dynamic environment configuration into main execution flow"
```