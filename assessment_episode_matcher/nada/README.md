# Error Classification for Outcome Measures

This project provides a Python script to classify validation errors for a CSV file containing Outcome Measures. It links errors to the original CSV file based on row numbers and groups them by error type.

## Features

- Parses validation errors from a text file
- Links errors to the original CSV data
- Handles multiple errors per input row
- Groups errors by type
- Provides detailed output with row numbers, EpisodeIDs, and AssessmentDates

## Requirements

- Python 3.10+

## Installation

1. Clone this repository or download the `error_classification.py` script.
2. Ensure you have Python 3.10 or later installed on your system.

## Usage

1. Place your input files in the same directory as the script:
   - `validation_errors.txt`: Contains validation errors from the online tool
   - `outcome_measures.csv`: The original CSV file (without header row)

2. Run the script:
   ```
   python error_classification.py
   ```

3. The script will output grouped errors to the console, showing the error type, affected rows, EpisodeID, and AssessmentDate for each error.

## Input File Formats

### validation_errors.txt

Each line should contain an error message in the following format:
```
SURVEY.txt: [Error Description] on Row [RowNumber], Column [ColumnNumber]
```

Example:
```
SURVEY.txt: Invalid EpisodeId 126067 for Agency 12QQ03062 on Row 28, Column 2
```

### outcome_measures.csv

- CSV file without a header row
- EpisodeID should be in the 2nd column (index 1)
- AssessmentDate should be in the 5th column (index 4)

Example row:
```
12QQ03022,108182,10353,0,06032024,2101,0,0,0,0,2,...
```

## Output

The script will print grouped errors to the console, organized by error type. For each error, it will show:
- Row number
- EpisodeID
- AssessmentDate

Example output:
```
Error Type: Invalid
-------------------
Row: 28, EpisodeID: 108182, AssessmentDate: 06032024
Row: 35, EpisodeID: 108190, AssessmentDate: 07032024
```

## Customization

You can modify the script to change the input file names or adjust the column indices for EpisodeID and AssessmentDate if your CSV format differs.

## Contributing

Feel free to submit issues or pull requests if you have suggestions for improvements or encounter any problems.

## License

This project is open-source and available under the MIT License.