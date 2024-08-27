"""
Error Classification Script for Outcome Measures

This script classifies validation errors for a CSV file containing Outcome Measures.
It links errors to the original CSV file based on row numbers and provides a summary
report of errors, including counts of unique assessments for each error type.

Usage:
    python error_classification.py

Requirements:
    Python 3.10+

Input files (should be in the same directory as the script):
    - validation_errors.txt: Contains validation errors from the online tool
    - outcome_measures.csv: The original CSV file (without header row)

Output:
    A dictionary containing error statistics and counts
"""

from pathlib import Path
import re
from typing import TypeAlias
from collections import defaultdict

ErrorDict: TypeAlias = dict[int, list[dict[str, str]]]
RowData: TypeAlias = tuple[str, str]
ClassifiedError: TypeAlias = dict[str, str | int]
UniqueAssessments: TypeAlias = set[tuple[str, str]]
ErrorCounts: TypeAlias = dict[str, tuple[int, str]]
ReportData: TypeAlias = dict[str, int | float | ErrorCounts]

def parse_validation_errors(validation_file: str) -> ErrorDict:
    """
    Parse the validation errors file and extract errors for each row.

    Args:
        validation_file (str): Path to the validation errors file

    Returns:
        ErrorDict: A dictionary mapping row numbers to lists of error information
    """
    errors: ErrorDict = defaultdict(list)
    pattern = r'SURVEY\.txt: (\w+) (\w+) (\d+) for Agency (\w+) on Row (\d+), Column (\d+)'
    
    with open(validation_file, 'r') as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                error_type, entity, value, agency, row, column = match.groups()
                errors[int(row)].append({
                    'type': f'{error_type} {entity}',
                    'value': value,
                    'agency': agency,
                    'column': column
                })
    
    return errors

def extract_relevant_rows(csv_file: str, error_rows: set[int]) -> dict[int, RowData]:
    """
    Extract relevant rows from the CSV file based on error row numbers.

    Args:
        csv_file (str): Path to the CSV file
        error_rows (set[int]): Set of row numbers with errors

    Returns:
        dict[int, RowData]: A dictionary mapping row numbers to (EpisodeID, AssessmentDate) tuples
    """
    import csv
    with open(csv_file, 'r') as f:
        return {
            i: (row[1], row[4])
            for i, row in enumerate(csv.reader(f), start=1)
            if i in error_rows
        }

def classify_errors(validation_file: str, csv_file: str) -> tuple[list[ClassifiedError], UniqueAssessments]:
    """
    Classify errors by linking validation errors to the original CSV data.

    Args:
        validation_file (str): Path to the validation errors file
        csv_file (str): Path to the CSV file

    Returns:
        tuple[list[ClassifiedError], UniqueAssessments]: A tuple containing a list of classified errors 
        and a set of unique assessments with errors
    """
    errors = parse_validation_errors(validation_file)
    relevant_rows = extract_relevant_rows(csv_file, set(errors.keys()))
    
    classified_errors = []
    unique_assessments = set()
    for row, data in relevant_rows.items():
        for error_info in errors[row]:
            classified_errors.append({
                'row': row,
                'episode_id': data[0],
                'assessment_date': data[1],
                'error_type': error_info['type'],
                'agency': error_info['agency'],
                'column': error_info['column']
            })
        unique_assessments.add((data[0], data[1]))
    
    return classified_errors, unique_assessments

def count_unique_assessments(grouped_errors: dict[str, list[ClassifiedError]]) -> ErrorCounts:
    """
    Count unique assessments for each error type and provide an example error.

    Args:
        grouped_errors (dict[str, list[ClassifiedError]]): Grouped classified errors

    Returns:
        ErrorCounts: A dictionary mapping error types to counts of unique assessments and an example error
    """
    unique_assessment_counts = {}
    for error_type, errors in grouped_errors.items():
        unique_count = len(set((error['episode_id'], error['assessment_date']) for error in errors))
        first_error = errors[0]
        error_message = f"{error_type} {first_error['episode_id']} for Agency {first_error['agency']} on Row {first_error['row']}, Column {first_error['column']}"
        unique_assessment_counts[error_type] = (unique_count, error_message)
    return unique_assessment_counts

def group_errors(classified_errors: list[ClassifiedError]) -> dict[str, list[ClassifiedError]]:
    """
    Group classified errors by error type.

    Args:
        classified_errors (list[ClassifiedError]): List of classified errors

    Returns:
        dict[str, list[ClassifiedError]]: A dictionary mapping error types to lists of classified errors
    """
    error_groups: dict[str, list[ClassifiedError]] = defaultdict(list)
    for error in classified_errors:
        error_groups[error['error_type']].append(error)
    return dict(error_groups)

def count_total_rows(csv_file: str) -> int:
    """
    Count the total number of rows in the CSV file.

    Args:
        csv_file (str): Path to the CSV file

    Returns:
        int: Total number of rows in the CSV file
    """
    with open(csv_file, 'r') as f:
        return sum(1 for _ in f)

def generate_report(csv_file: str, validation_file: str) -> ReportData:
    """
    Generate a comprehensive report of error statistics.

    Args:
        csv_file (str): Path to the CSV file
        validation_file (str): Path to the validation errors file

    Returns:
        ReportData: A dictionary containing error statistics and counts
    """
    classified_errors, unique_assessments = classify_errors(validation_file, csv_file)
    grouped_errors = group_errors(classified_errors)
    unique_assessment_counts = count_unique_assessments(grouped_errors)
    total_rows = count_total_rows(csv_file)

    error_percentage = (len(unique_assessments) / total_rows) * 100

    return {
        "total_rows": total_rows,
        "unique_assessments_with_errors": len(unique_assessments),
        "error_percentage": round(error_percentage, 2),
        "error_counts": unique_assessment_counts
    }

def main() -> None:
    """
    Main function to run the error classification process and generate a report.
    """
    cwd = Path.cwd()
    validation_file = cwd / "validation_errors.txt"
    csv_file = cwd / "surveytxt_20230701-20240731.csv" # 'outcome_measures.csv'

    report = generate_report(csv_file, validation_file)
    print(report)  # Or do something else with the report

if __name__ == "__main__":
    main()