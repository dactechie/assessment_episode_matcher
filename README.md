
# README

Code Stripped out from NADAFuncTools and upgraded.

## Development (windows)

1. set up dev environment and install deps from requirements.txt
2. setup data/ in/ out/ processed/
3. start azurite: run azurite.bat

# Pusing out changes:
build locally with 
> python clean.py
> python -m build
make sure you have pip install twine  keyring artifacts-keyring
> twine upload -r atom-matching-feed dist/* --verbose

# to PyPI
> twine upload dist/* --verbose


# version
0.3.1  - removing disk writes as read-only on cloud fs
0.4.0  - remove all local writes and reads, refactor, fix errors_warning writes
