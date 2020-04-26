package main

import (
	"fmt"

	"github.com/lizhaoliu/konsen/v2/core"
	"gopkg.in/yaml.v2"
)

const s = `
endpoints:
- a
- b
- c
localEndpoint: a
`

func main() {
	cluster := core.ClusterConfig{
		Endpoints:     []string{"192.168.86.25:10001", "192.168.86.25:10002", "192.168.86.25:10003"},
		LocalEndpoint: "192.168.86.25:10001",
	}

	out, err := yaml.Marshal(cluster)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", out)

	c := core.ClusterConfig{}
	if err := yaml.Unmarshal([]byte(s), &c); err != nil {
		panic(c)
	}
	fmt.Printf("%#v", c)
}
