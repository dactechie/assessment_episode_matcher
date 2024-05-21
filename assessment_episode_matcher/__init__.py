from pathlib import Path

# Get the path of the current file (module.py)
package_path = Path(__file__)

# Get the package directory path
project_directory = package_path.parent.parent

# print("Project directory ", current_file_path)