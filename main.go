package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-logfmt/logfmt"
	"github.com/cheggaaa/pb/v3"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: logfmt2json <logfile> <outputfile>")
		return
	}

	logFile := os.Args[1]
	outputFile := os.Args[2]

	file, err := os.Open(logFile)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outFile.Close()

	// Get the total number of lines in the log file
	totalLines, err := countLines(logFile)
	if err != nil {
		fmt.Printf("Error counting lines in file: %v\n", err)
		return
	}

	// Create and start the progress bar with colors
	bar := pb.New(totalLines).SetTemplateString(`{{ red "Processing: " }}{{counters . }} {{bar . "<" "=" ">"}} {{percent . }} {{speed . }}`).Start()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	startTime := time.Now()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
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
			fmt.Printf("Error parsing logfmt: %v\n", err)
			continue
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			fmt.Printf("Error converting to JSON: %v\n", err)
			continue
		}

		_, err = writer.WriteString(string(jsonData) + "\n")
		if err != nil {
			fmt.Printf("Error writing to output file: %v\n", err)
			continue
		}

		// Increment the progress bar
		bar.Increment()
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}

	// Finish the progress bar
	bar.Finish()

	// Print elapsed time
	fmt.Printf("Conversion completed in %v\n", time.Since(startTime))
}

// countLines counts the number of lines in a file
func countLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount, nil
}
