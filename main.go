package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/go-logfmt/logfmt"
)

type chunkResult struct {
	index int
	lines []string
	err   error
}

type lineCountResult struct {
	index     int
	lineCount int
	err       error
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: logfmt2json <logfile> <outputfile>")
		return
	}

	logFile := os.Args[1]
	outputFile := os.Args[2]

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	start := time.Now()
	totalLines, err := countLinesInParallel(logFile)
	if err != nil {
		log.Fatalf("Error counting lines in file: %v\n", err)
	}
	log.Printf("Counting lines took %v\n", time.Since(start))

	numCores := runtime.NumCPU()
	linesPerChunk := (totalLines + numCores - 1) / numCores

	log.Printf("Using %d cores\n", numCores)
	log.Printf("Total lines: %d, lines per chunk: %d\n", totalLines, linesPerChunk)

	startTime := time.Now()

	var wg sync.WaitGroup
	chunkChan := make(chan chunkResult, numCores)
	resultChan := make(chan chunkResult, numCores)

	// Create a slice to hold progress bars for each worker
	bars := make([]*pb.ProgressBar, numCores)
	for i := 0; i < numCores; i++ {
		bars[i] = pb.New(linesPerChunk).SetTemplateString(fmt.Sprintf(`{{ red "Worker %d: " }}{{counters . }} {{bar . "<" "=" ">"}} {{percent . }} {{speed . }}`, i+1)).Start()
	}

	// Read file in chunks and send to chunkChan
	go func() {
		file, err := os.Open(logFile)
		if err != nil {
			log.Fatalf("Error opening file: %v\n", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		chunk := make([]string, 0, linesPerChunk)
		chunkIndex := 0

		for scanner.Scan() {
			line := scanner.Text()
			chunk = append(chunk, line)
			if len(chunk) >= linesPerChunk {
				chunkChan <- chunkResult{index: chunkIndex, lines: chunk}
				chunk = make([]string, 0, linesPerChunk)
				chunkIndex++
			}
		}
		if len(chunk) > 0 {
			chunkChan <- chunkResult{index: chunkIndex, lines: chunk}
		}
		close(chunkChan)
	}()

	// Process each chunk in parallel
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for chunk := range chunkChan {
				lines, err := processChunk(chunk.lines, bars[workerID])
				resultChan <- chunkResult{index: chunk.index, lines: lines, err: err}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(resultChan)
		for _, bar := range bars {
			bar.Finish()
		}
	}()

	// Collect results and write them in order
	results := make(map[int][]string)
	var currentIndex int

	outFile, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Error opening output file: %v\n", err)
	}
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	log.Println("Starting to write processed data to output file")

	for res := range resultChan {
		if res.err != nil {
			log.Fatalf("Error processing chunk: %v\n", res.err)
		}
		results[res.index] = res.lines
		for {
			if lines, ok := results[currentIndex]; ok {
				log.Printf("Writing chunk %d to output file", currentIndex)
				for _, line := range lines {
					if _, err := writer.WriteString(line + "\n"); err != nil {
						log.Fatalf("Error writing to output file: %v\n", err)
					}
				}
				delete(results, currentIndex)
				currentIndex++
			} else {
				break
			}
		}
	}

	log.Printf("Conversion completed in %v\n", time.Since(startTime))
}

func processChunk(chunk []string, bar *pb.ProgressBar) ([]string, error) {
	var processedLines []string

	for _, line := range chunk {
		var data map[string]interface{}

		decoder := logfmt.NewDecoder(bytes.NewBufferString(line))
		for decoder.ScanRecord() {
			data = make(map[string]interface{})
			for decoder.ScanKeyval() {
				key := string(decoder.Key())
				value := string(decoder.Value())
				if value != "" {
					data[key] = value
				}
			}
		}

		if err := decoder.Err(); err != nil {
			return nil, fmt.Errorf("error parsing logfmt: %w", err)
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("error converting to JSON: %w", err)
		}

		processedLines = append(processedLines, string(jsonData))
		bar.Increment()
	}

	return processedLines, nil
}

func countLinesInParallel(filePath string) (int, error) {
	numCores := runtime.NumCPU()
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	fileSize := fileInfo.Size()
	chunkSize := (fileSize + int64(numCores) - 1) / int64(numCores)

	var wg sync.WaitGroup
	lineCountChan := make(chan lineCountResult, numCores)

	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(chunkIndex int) {
			defer wg.Done()
			start := int64(chunkIndex) * chunkSize
			end := start + chunkSize
			if end > fileSize {
				end = fileSize
			}

			file, err := os.Open(filePath)
			if err != nil {
				lineCountChan <- lineCountResult{index: chunkIndex, lineCount: 0, err: err}
				return
			}
			defer file.Close()

			file.Seek(start, 0)
			scanner := bufio.NewScanner(file)
			if start > 0 {
				scanner.Scan() // skip partial line
			}

			lineCount := 0
			for scanner.Scan() {
				pos, err := file.Seek(0, 1)
				if err != nil {
					lineCountChan <- lineCountResult{index: chunkIndex, lineCount: 0, err: err}
					return
				}
				if pos >= end {
					break
				}
				lineCount++
			}

			if err := scanner.Err(); err != nil {
				lineCountChan <- lineCountResult{index: chunkIndex, lineCount: 0, err: err}
				return
			}

			lineCountChan <- lineCountResult{index: chunkIndex, lineCount: lineCount, err: nil}
		}(i)
	}

	go func() {
		wg.Wait()
		close(lineCountChan)
	}()

	totalLines := 0
	for res := range lineCountChan {
		if res.err != nil {
			return 0, res.err
		}
		totalLines += res.lineCount
	}

	return totalLines, nil
}
