package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"strings"
	"time"
)

func processSample(url string, sample interface{}, wg *sync.WaitGroup) (map[string]interface{}, time.Duration) {
	defer wg.Done()
	jsonData, _ := json.Marshal(sample)
	start := time.Now()
	resp, _ := http.Post(url, "application/json", bytes.NewReader(jsonData))
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	duration := time.Since(start)
	var data map[string]interface{}
	json.Unmarshal(body, &data)
	return data, duration
}

type CLIOut struct {
	Timings []string
	Sample map[string]interface{}
}

func pipelineProcessor(url string, pipeline []string, file []byte) CLIOut {
	var wg sync.WaitGroup
	encodedImage := base64.StdEncoding.EncodeToString(file)
	sample := map[string]interface{}{
		"$image": encodedImage,
	}
	var t time.Duration
	var out CLIOut
	for _, svcName := range pipeline {
		wg.Add(1)
		sUrl := fmt.Sprintf("%s/%s/process/sample", url, svcName)
		sample, t = processSample(sUrl, sample, &wg)
		out.Timings = append(out.Timings, t.String())
	}
	wg.Wait()
	out.Sample = sample
	return out
	
	// for index, value := range sample["objects"].([]interface{}) {
	// 	fmt.Println(index)
	// 	fmt.Println(value.(map[string]interface{})["angles"])
	// }
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run main.go <url> <imagePath> <serviceName>")
		return
	}

	url := os.Args[1]
	imagePath := os.Args[2]
	serviceName := os.Args[3]

	imageBytes, _ := os.ReadFile(imagePath)
	pipeline := strings.Split(serviceName, ",")

	sample := pipelineProcessor(url, pipeline, imageBytes)
	jsonSample, _ := json.Marshal(sample)
	fmt.Println(string(jsonSample))
}