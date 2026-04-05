package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseClusterConfig_Valid(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cluster.yml")
	content := `
servers:
  node1: "localhost:10001"
  node2: "localhost:10002"
  node3: "localhost:10003"
httpServers:
  node1: "localhost:20001"
  node2: "localhost:20002"
  node3: "localhost:20003"
localServerName: node1
`
	os.WriteFile(cfgPath, []byte(content), 0o644)

	cfg, err := ParseClusterConfig(cfgPath)
	if err != nil {
		t.Fatalf("ParseClusterConfig: %v", err)
	}
	if cfg.LocalServerName != "node1" {
		t.Errorf("LocalServerName = %q, want %q", cfg.LocalServerName, "node1")
	}
	if len(cfg.Servers) != 3 {
		t.Errorf("len(Servers) = %d, want 3", len(cfg.Servers))
	}
	if cfg.Servers["node1"] != "localhost:10001" {
		t.Errorf("Servers[node1] = %q, want %q", cfg.Servers["node1"], "localhost:10001")
	}
}

func TestParseClusterConfig_MissingLocalServer(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cluster.yml")
	content := `
servers:
  node1: "localhost:10001"
  node2: "localhost:10002"
  node3: "localhost:10003"
`
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := ParseClusterConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for missing localServerName")
	}
}

func TestParseClusterConfig_EvenNodeCount(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cluster.yml")
	content := `
servers:
  node1: "localhost:10001"
  node2: "localhost:10002"
localServerName: node1
`
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := ParseClusterConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for even number of nodes")
	}
}

func TestParseClusterConfig_LocalNotInCluster(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cluster.yml")
	content := `
servers:
  node1: "localhost:10001"
  node2: "localhost:10002"
  node3: "localhost:10003"
localServerName: node99
`
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := ParseClusterConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for local server not in cluster")
	}
}

func TestParseClusterConfig_FileNotFound(t *testing.T) {
	_, err := ParseClusterConfig("/nonexistent/path/cluster.yml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestParseClusterConfig_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cluster.yml")
	os.WriteFile(cfgPath, []byte("{{invalid yaml"), 0o644)

	_, err := ParseClusterConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}
