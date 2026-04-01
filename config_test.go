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