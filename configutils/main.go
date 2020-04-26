package main

import (
	"flag"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const clusterConfigFileName = "cluster.yml"

var (
	clusterConfigPath string
	outputDir         string
	binaryPath        string
)

func init() {
	flag.StringVar(&clusterConfigPath, "cluster_config_path", "", "Cluster config file path.")
	flag.StringVar(&outputDir, "output_dir", "", "Output directory.")
	flag.StringVar(&binaryPath, "binary_path", "", "Binary path.")
	flag.Parse()

	if clusterConfigPath == "" {
		logrus.Fatalf("endpoints is unspecified.")
	}
	if outputDir == "" {
		logrus.Fatalf("output_dir is unspecified.")
	}
	if binaryPath == "" {
		logrus.Fatalf("binary_path is unspecified.")
	}
}

func main() {
	binaryBuf, err := ioutil.ReadFile(binaryPath)
	if err != nil {
		logrus.Fatalf("Failed to read binary file: %v", err)
	}

	buf, err := ioutil.ReadFile(clusterConfigPath)
	if err != nil {
		logrus.Fatalf("Failed to read cluster config file: %v", err)
	}

	cluster := &core.ClusterConfig{}
	if err := yaml.Unmarshal(buf, cluster); err != nil {
		logrus.Fatalf("Failed to unmarshal cluster config file: %v", err)
	}

	for i, endpoint := range cluster.Endpoints {
		dirPath := path.Join(outputDir, strconv.Itoa(i))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			logrus.Fatalf("Failed to create dir: %v", err)
		}

		c := &core.ClusterConfig{
			Endpoints:     cluster.Endpoints,
			LocalEndpoint: endpoint,
		}
		buf, err := yaml.Marshal(c)
		if err != nil {
			logrus.Fatalf("Failed to marshal cluster config: %v", err)
		}
		filePath := path.Join(dirPath, clusterConfigFileName)
		if err := ioutil.WriteFile(filePath, buf, 0644); err != nil {
			logrus.Fatalf("Failed to write cluster config JSON file: %v", err)
		}

		binPath := path.Join(dirPath, "konsen")
		if err := ioutil.WriteFile(binPath, binaryBuf, 0755); err != nil {
			logrus.Fatalf("Failed to copy binary: %v", err)
		}
	}
}
