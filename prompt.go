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