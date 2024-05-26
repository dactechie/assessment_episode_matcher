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
    author="Aftab Jalal", 
    author_email="mj@auditlytics.nz", 
      
    packages=find_packages(),
    install_requires=get_requirements(),
    python_requires='>=3.10',

    # long_description=long_description, 
    # long_description_content_type="text/markdown", 
    license="MIT", 
  
    # classifiers like program is suitable for python3, just leave as it is. 
    classifiers=[ 
        "Programming Language :: Python :: 3", 
        "License :: OSI Approved :: MIT License", 
        "Operating System :: OS Independent", 
    ], 
)

# if __name__ == '__main__':
#     get_version()