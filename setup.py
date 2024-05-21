from setuptools import setup, find_packages

def get_requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()

setup(
    name='assessment_episode_matcher',
    version='0.1.0',
    packages=find_packages(),
    # entry_points={
    #     'console_scripts': [
    #         'tcli=tcli.run:main',
    #     ],
    # },
    install_requires=get_requirements(),
    python_requires='>=3.10',
)