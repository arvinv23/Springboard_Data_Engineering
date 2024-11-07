# Airflow Log Analyzer

A Python tool for analyzing Apache Airflow log files to identify and report errors. This tool scans through Airflow log directories, parses log entries, and generates reports of any errors found.

## Features

- 🔍 Recursively scans Airflow log directories
- 📊 Parses and analyzes log entries with timestamp, file info, level, and messages
- ⚠️ Identifies and counts ERROR level log entries
- 📝 Generates detailed error reports with chronological sorting

## Installation

1. Ensure you have Python 3.6+ installed
2. Clone the repository
```bash
git clone https://github.com/username/airflow-log-analyzer
cd airflow-log-analyzer
```

## Usage
```python

Copy
from log_analyzer import LogAnalyzer
```

## Create analyzer instance
analyzer = LogAnalyzer()

## Run analysis
analyzer.analyze_logs()

## Print results
analyzer.print_report()
API Reference
LogEntry Class
python

@dataclass
class LogEntry:
    timestamp: datetime
    file_info: str
    level: str
    message: str
    
| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | `datetime` | Timestamp of the log entry |
| `file_info` | `string` | File information from the log |
| `level` | `string` | Log level (INFO/ERROR/WARNING) |
| `message` | `string` | The actual log message |

## Methods
| Method | Description |
|--------|-------------|
| `analyze_logs()` | Analyzes all log files in the configured directory |
| `print_report()` | Prints analysis results including error count and details |
| `analyze_file(file_path)` | Analyzes a single log file |
| `parse_log_line(line)` | Parses a single log line |

## Dependencies
Python 3.6+
Apache Airflow
pathlib
typing
dataclasses
datetime
re

##Contributing
Fork the project
Create your feature branch (git checkout -b feature/AmazingFeature)
Commit your changes (git commit -m 'Add some AmazingFeature')
Push to the branch (git push origin feature/AmazingFeature)
Open a Pull Request

##License
MIT


