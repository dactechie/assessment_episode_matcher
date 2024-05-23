from pathlib import Path
from setuptools import setup, find_packages

def get_requirements():
    req_path = Path(__file__).parent / 'requirements.txt'
    print(f"Reading requirements from {req_path}")
    with req_path.open() as f:
        return f.read().splitlines()

def get_version():
    # print("locaiton", Path(__file__).parent )
    version_path = Path(__file__).parent / 'assessment_episode_matcher' / 'version.py'
    with version_path.open() as f:
        version_line = next(line for line in f if line.startswith('__version__'))
        version = version_line.split('=')[1].strip().strip("'\"")
        return version

setup(
    name='assessment_episode_matcher',
    version=get_version(),
    packages=find_packages(),
    install_requires=get_requirements(),
    python_requires='>=3.10',
)

# if __name__ == '__main__':
#     get_version()