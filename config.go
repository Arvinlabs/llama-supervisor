package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// netAddr 组装 "host:port"
func netAddr(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// yamlUnmarshalFile 读取并解析 yaml 文件
func yamlUnmarshalFile(path string, out interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, out)
}
