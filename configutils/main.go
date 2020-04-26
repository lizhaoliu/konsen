package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	endpointsFlag string
	outputDir     string
	binaryPath    string
)

func init() {
	flag.StringVar(&endpointsFlag, "endpoints", "", "Endpoints in the cluster, separated by comma.")
	flag.StringVar(&outputDir, "output_dir", "", "Output directory.")
	flag.StringVar(&binaryPath, "binary_path", "", "Binary path.")
	flag.Parse()

	if endpointsFlag == "" {
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

	endpoints := strings.Split(endpointsFlag, ",")
	var nodes []*konsen.Node
	for _, endpoint := range endpoints {
		nodes = append(nodes, &konsen.Node{Endpoint: endpoint})

	}
	for i, node := range nodes {
		dirPath := path.Join(outputDir, strconv.Itoa(i))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			logrus.Fatalf("Failed to create dir: %v", err)
		}

		cluster := &konsen.Cluster{
			Nodes:     nodes,
			LocalNode: node,
			Name:      fmt.Sprintf("node_%d", i),
		}
		clusterJson := protojson.Format(cluster)
		filePath := path.Join(dirPath, "cluster_config.json")
		if err := ioutil.WriteFile(filePath, []byte(clusterJson), 0644); err != nil {
			logrus.Fatalf("Failed to write cluster config JSON file: %v", err)
		}

		binPath := path.Join(dirPath, "konsen")
		if err := ioutil.WriteFile(binPath, binaryBuf, 0755); err != nil {
			logrus.Fatalf("Failed to copy binary: %v", err)
		}
	}
}
