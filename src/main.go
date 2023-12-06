package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ProcessingResult struct {
	Duration time.Duration
	Sample   map[string]interface{}
}

func processSample(url string, sample interface{}, wg *sync.WaitGroup, resultCh chan ProcessingResult) {
	defer wg.Done()
	jsonData, _ := json.Marshal(sample)
	start := time.Now()
	resp, _ := http.Post(url, "application/json", bytes.NewReader(jsonData))
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	duration := time.Since(start)
	var data map[string]interface{}
	json.Unmarshal(body, &data)
	resultCh <- ProcessingResult{Sample: data, Duration: duration}
}

func executePool(url string, pool []string, sample map[string]interface{}, version string) []ProcessingResult {
	var wg sync.WaitGroup
	resultCh := make(chan ProcessingResult, len(pool))
	defer close(resultCh)
	for _, svcName := range pool {
		wg.Add(1)
		go processSample(fmt.Sprintf("%s/%s/v%s/process/sample", url, svcName, version), sample, &wg, resultCh)
	}
	wg.Wait()

	var resExecPool []ProcessingResult
	for i := 0; i < len(pool); i++ {
		resExecPool = append(resExecPool, <-resultCh)
	}
	return resExecPool
}

func pipelineProcessor(url string, pipeline []string, sample map[string]interface{}, version string) map[string]interface{} {

	var keys []string
	var poolPipeline [][]string

	groupedSvcsMap := make(map[string][]string)

	for i, svcPriorityCallStr := range pipeline {
		svcPrioritySplit := strings.Split(svcPriorityCallStr, ".")
		if len(svcPrioritySplit) == 1 {
			svcPrioritySplit = append(svcPrioritySplit, strconv.Itoa(i))
		}
		groupedSvcsMap[svcPrioritySplit[1]] = append(groupedSvcsMap[svcPrioritySplit[1]], svcPrioritySplit[0])
	}

	for key := range groupedSvcsMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		services := groupedSvcsMap[key]
		poolPipeline = append(poolPipeline, services)
	}

	for _, pool := range poolPipeline {
		procResults := executePool(url, pool, sample, version)
		sample = procResults[0].Sample
		for i := 1; i < len(procResults); i++ {
			for i, v := range procResults[i].Sample["objects"].([]interface{}) {
				sample["objects"].([]interface{})[i] = v
			}
		}

	}

	return sample
}

func main() {
	if len(os.Args) < 5 {
		fmt.Println("Usage: go run main.go <url> <imagePath> <serviceName> <version>")
		return
	}

	url := os.Args[1]
	imagePath := os.Args[2]
	pipelineStr := os.Args[3]
	version := os.Args[4]

	imageBytes, _ := os.ReadFile(imagePath)
	pipeline := strings.Split(pipelineStr, ",")

	encodedImage := base64.StdEncoding.EncodeToString(imageBytes)
	var sample map[string]interface{}
	if version == "1" {
		sample = map[string]interface{}{
			"$image": encodedImage,
		}
	} else if version == "2" {
		sample = map[string]interface{}{
			"_image": map[string]interface{}{
				"blob":   encodedImage,
				"format": "IMAGE",
			},
		}
	}

	outSample := pipelineProcessor(url, pipeline, sample, version)
	jsonSample, _ := json.Marshal(outSample)
	fmt.Println(string(jsonSample))
}
