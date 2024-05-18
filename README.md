## logfmt to JSON Converter

This Go program converts logs from logfmt format to JSON format. It reads an input log file, processes the log lines, and writes the converted JSON logs to an output file. The program utilizes multiple CPU cores to process the log file in parallel, displaying a progress bar for each worker.

### Features
- Converts logfmt formatted logs to JSON.
- Utilizes multiple CPU cores for parallel processing.
- Skips keys with empty string values.
- Displays a progress bar for each worker.

### Requirements
- Go 1.16 or later

### Installation

1. Clone the repository:

```sh
git clone https://github.com/yourusername/logfmt-to-json.git
cd logfmt-to-json
```

2. Install the required Go packages:

```sh
go get github.com/cheggaaa/pb/v3
go get github.com/go-logfmt/logfmt
```

### Usage
To run the program, use the following command:

```sh
go run logfmt2json.go /path/to/logfile.log /path/to/outputfile.json
```

### Command-Line Arguments
- `logfile`: Path to the input logfmt file.
- `outputfile`: Path to the output JSON file.


### Example
```sh
go run logfmt2json.go example.log output.json
```

### License
This project is licensed under the MIT License. See the LICENSE file for details.

### Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

### Contact
For questions or feedback, please contact jayendra@infocusp.com.
