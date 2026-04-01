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