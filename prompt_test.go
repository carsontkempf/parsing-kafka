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