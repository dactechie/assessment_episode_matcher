import os
import shutil
import argparse


def copy_py_files(source_dir, target_dir):
    for root, dirs, files in os.walk(source_dir):
        # Remove __pycache__ and .venv directories from the walk
        dirs[:] = [d for d in dirs if d not in ("__pycache__", ".venv")]
        
        for file in files:
            if file.endswith('.py'):
                # Get the relative path from the source directory
                rel_path = os.path.relpath(root, source_dir)
                
                # Create the target directory structure
                target_path = os.path.join(target_dir, rel_path)
                os.makedirs(target_path, exist_ok=True)
                
                # Copy the .py file
                source_file = os.path.join(root, file)
                target_file = os.path.join(target_path, file)
                shutil.copy2(source_file, target_file)
                print(f"Copied: {source_file} -> {target_file}")

def main():
    parser = argparse.ArgumentParser(description="Copy .py files to a clean directory.")
    parser.add_argument("source_dir", help="Source directory containing Python files")
    parser.add_argument("target_dir", help="Target directory for clean Python files")
    args = parser.parse_args()

    source_dir = os.path.abspath(args.source_dir)
    target_dir = os.path.abspath(args.target_dir)

    if not os.path.exists(source_dir):
        print(f"Error: Source directory '{source_dir}' does not exist.")
        return

    if os.path.exists(target_dir):
        print(f"Warning: Target directory '{target_dir}' already exists. Files may be overwritten.")
    else:
        os.makedirs(target_dir)

    copy_py_files(source_dir, target_dir)
    print("Copy process completed.")

if __name__ == "__main__":
    main()