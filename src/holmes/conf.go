package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
)

type HolmesConfig struct {
	RedisConfs []RedisConf
	InLogDir   string
	OutLogDir  string
}

func LoadConfig(configPath string) HolmesConfig {
	var holmesConfig HolmesConfig
	file, err := os.Open(configPath)
	if err != nil {
		log.Fatal(err)
	} else {
		configReader := bufio.NewReader(file)
		var content string
		for {
			line, err := configReader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					content = content + line
					break
				}
				log.Fatal(err)
			} else {
				content = content + line
			}
		}
		temp := []byte(content)
		err := json.Unmarshal(temp, &holmesConfig)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer file.Close()
	return holmesConfig
}
