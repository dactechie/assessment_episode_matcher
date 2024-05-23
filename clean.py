import shutil
import os

def remove_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Removed directory: {path}")

def main():
    remove_directory('build')
    remove_directory('dist')
    egg_info_dirs = [d for d in os.listdir() if d.endswith('.egg-info')]
    for d in egg_info_dirs:
        remove_directory(d)

if __name__ == "__main__":
    main()