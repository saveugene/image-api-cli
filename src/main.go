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
	Name     string
	Sample   map[string]interface{}
}

func processSample(url string, svcName string, apiVersion string, sample interface{}, wg *sync.WaitGroup, resultCh chan ProcessingResult) {
	defer wg.Done()
	jsonData, _ := json.Marshal(sample)
	start := time.Now()
	resp, _ := http.Post(fmt.Sprintf("%s/%s/v%s/process/sample", url, svcName, apiVersion), "application/json", bytes.NewReader(jsonData))
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	duration := time.Since(start)
	var data map[string]interface{}
	json.Unmarshal(body, &data)
	resultCh <- ProcessingResult{Sample: data, Duration: duration, Name: svcName}
}

func executeProcessingPool(url string, apiVersion string, pool []string, sample map[string]interface{}) []ProcessingResult {
	var wg sync.WaitGroup
	resultCh := make(chan ProcessingResult, len(pool))
	defer close(resultCh)
	for _, svcName := range pool {
		wg.Add(1)
		go processSample(url, svcName, apiVersion, sample, &wg, resultCh)
	}
	wg.Wait()

	var resExecPool []ProcessingResult
	for i := 0; i < len(pool); i++ {
		resExecPool = append(resExecPool, <-resultCh)
	}
	return resExecPool
}

func addDurationFromProcessing(durations [][2]string, procResult ProcessingResult) [][2]string {
	return append(durations, [2]string{procResult.Name, procResult.Duration.String()})
}

func pipelineProcessor(url string, pipeline []string, sample map[string]interface{}, apiVersion string) (map[string]interface{}, [][2]string) {

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
	var durations [][2]string

	for _, pool := range poolPipeline {
		procResults := executeProcessingPool(url, apiVersion, pool, sample)
		sample = procResults[0].Sample
		durations = addDurationFromProcessing(durations, procResults[0])
		for si := 1; si < len(procResults); si++ {
			durations = addDurationFromProcessing(durations, procResults[si])
			for iObj, obj := range procResults[si].Sample["objects"].([]interface{}) {
				for objField, val := range obj.(map[string]interface{}) {
					var objects = sample["objects"].([]interface{})
					objects[iObj].(map[string]interface{})[objField] = val
				}
			}
		}
	}
	return sample, durations
}

func getValueBySelector(data interface{}, selector string) interface{} {
	keys := strings.Split(selector, ".")
	for _, key := range keys {
		if index, err := strconv.Atoi(key); err == nil {
			if list, ok := data.([]interface{}); ok && index >= 0 && index < len(list) {
				data = list[index]
			} else {
				return nil
			}
		} else {
			if obj, ok := data.(map[string]interface{}); ok {
				if value, exists := obj[key]; exists {
					data = value
				} else {
					return nil
				}
			} else {
				return nil
			}
		}
	}
	return data
}

func envToBool(key string) bool {
	val := strings.ToLower(os.Getenv(key))
	return val == "true" || val == "1"
}

func main() {
	selector := ""
	if len(os.Args) < 5 {
		fmt.Println("Usage: image-api <url> <imagePath> <pipelineStr> <apiVersion> <selector | nil>")
		return
	} else if len(os.Args) == 6 {
		selector = os.Args[5]
	}

	url := os.Args[1]
	imagePath := os.Args[2]
	pipelineStr := os.Args[3]
	apiVersion := os.Args[4]

	enableProcTime := envToBool("ENABLE_PROCESSING_DURATION_TRACER")

	imageBytes, _ := os.ReadFile(imagePath)
	pipeline := strings.Split(pipelineStr, ",")

	encodedImage := base64.StdEncoding.EncodeToString(imageBytes)
	var sample map[string]interface{}
	if apiVersion == "1" {
		sample = map[string]interface{}{
			"$image": encodedImage,
		}
	} else if apiVersion == "2" {
		sample = map[string]interface{}{
			"_image": map[string]interface{}{
				"blob":   encodedImage,
				"format": "IMAGE",
			},
		}
	}

	outSample, durations := pipelineProcessor(url, pipeline, sample, apiVersion)
	if enableProcTime {
		outSample["durations"] = durations
	}
	var jsonSample []byte
	if selector != "" {
		jsonSample, _ = json.Marshal(getValueBySelector(outSample, selector))
	} else {
		jsonSample, _ = json.Marshal(outSample)
	}

	fmt.Println(string(jsonSample))
}
