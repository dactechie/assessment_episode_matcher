import os
import argparse
import re

def remove_comments_and_clean_lines(code):
    # Remove multi-line comments
    code = re.sub(r'"""[\s\S]*?"""', '', code)
    code = re.sub(r"'''[\s\S]*?'''", '', code)
    
    # Remove single-line comments that occupy the entire line
    lines = code.split('\n')
    lines = [line for line in lines if not line.strip().startswith('#')]
    
    # Reduce multiple empty lines to a maximum of two
    cleaned_lines = []
    empty_line_count = 0
    for line in lines:
        if line.strip() == '':
            empty_line_count += 1
            if empty_line_count <= 2:
                cleaned_lines.append(line)
        else:
            empty_line_count = 0
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def merge_python_files(source_dir, output_file):
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for root, dirs, files in os.walk(source_dir):
            # Skip __pycache__ and .venv directories
            dirs[:] = [d for d in dirs if d not in ("__pycache__", ".venv")]
            
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, source_dir)
                    
                    outfile.write(f"\n\n# FILE: {rel_path}\n")
                    outfile.write(f"# {'=' * 50}\n\n")
                    
                    with open(file_path, 'r', encoding='utf-8') as infile:
                        content = infile.read()
                        cleaned_content = remove_comments_and_clean_lines(content)
                        outfile.write(cleaned_content)

def main():
    parser = argparse.ArgumentParser(description="Merge Python files into a single file with metadata, removing comments and cleaning up empty lines.")
    parser.add_argument("source_dir", help="Source directory containing Python files")
    parser.add_argument("output_file", help="Output file to write merged Python code")
    args = parser.parse_args()

    source_dir = os.path.abspath(args.source_dir)
    output_file = os.path.abspath(args.output_file)

    if not os.path.exists(source_dir):
        print(f"Error: Source directory '{source_dir}' does not exist.")
        return

    merge_python_files(source_dir, output_file)
    print(f"Merged Python files (with comments removed and lines cleaned) have been written to {output_file}")

if __name__ == "__main__":
    main()