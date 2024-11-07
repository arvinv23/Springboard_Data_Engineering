#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pathlib import Path
from typing import Tuple, List, Optional
from dataclasses import dataclass
from datetime import datetime
import re


# In[6]:


from airflow.configuration import conf
import os

# Print the base log folder location
print("Base Log Folder:", conf.get('logging', 'base_log_folder'))

# Print the Airflow home directory
print("Airflow Home:", os.environ.get('AIRFLOW_HOME', os.path.expanduser('~/airflow')))


# In[11]:


@dataclass
class LogEntry:
    """Represents a single log entry"""
    timestamp: datetime
    file_info: str
    level: str
    message: str
    
    def __str__(self) -> str:
        return f"[{self.timestamp}] {{{self.file_info}}} {self.level} - {self.message}"

class LogAnalyzer:
    """Analyzes Airflow log files"""
    
    LOG_PATTERN = r'\[(.*?)\]\s*\{(.*?)\}\s*(INFO|ERROR|WARNING)\s*-\s*(.*?)$'
    
    def __init__(self):
        self.log_dir = Path(conf.get('logging', 'base_log_folder'))
        self.error_count = 0
        self.errors = []
    
    def parse_log_line(self, line: str) -> Optional[LogEntry]:
        """Parse a single log line"""
        match = re.match(self.LOG_PATTERN, line.strip())
        if not match:
            return None
            
        timestamp_str, file_info, level, message = match.groups()
        try:
            timestamp = datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S,%f')
        except ValueError:
            return None
            
        return LogEntry(
            timestamp=timestamp,
            file_info=file_info.strip(),
            level=level.strip(),
            message=message.strip()
        )
    
    def analyze_file(self, file_path: Path) -> Tuple[int, List[LogEntry]]:
        """Analyze a single log file"""
        error_count = 0
        errors = []
        
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    entry = self.parse_log_line(line)
                    if entry and entry.level == "ERROR":
                        error_count += 1
                        errors.append(entry)
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            
        return error_count, errors
    
    def analyze_logs(self):
        """Analyze all log files"""
        log_files = list(self.log_dir.rglob('*.log'))
        
        for file_path in log_files:
            count, cur_errors = self.analyze_file(file_path)
            self.error_count += count
            self.errors.extend(cur_errors)
    
    def print_report(self):
        """Print analysis results"""
        print(f"\nTotal number of errors: {self.error_count}")
        if self.error_count > 0:
            print("\nHere are all the errors:")
            for error in sorted(self.errors, key=lambda x: x.timestamp):
                print(str(error))

def main():
    """Main function"""
    analyzer = LogAnalyzer()
    print(f"Analyzing logs in directory: {analyzer.log_dir}")
    analyzer.analyze_logs()
    analyzer.print_report()

if __name__ == "__main__":
    main()
