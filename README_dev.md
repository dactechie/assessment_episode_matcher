
# README

Code Stripped out from NADAFuncTools and upgraded.

## Development (windows)

1. set up dev environment and install deps from requirements.txt
2. setup data/ in/ out/ processed/
3. start azurite: run azurite.bat

(trouble with git - see GIT_HELP.md)

# Pusing out changes

build locally with :
> python clean.py
> python -m build
make sure you have pip install twine  keyring artifacts-keyring
may need to use python -m for this :
> twine upload -r atom-matching-feed dist/* --verbose

## to PyPI

> twine upload dist/* --verbose
passcode at the bttom of : 
    dev/PyPI-Recovery-Codes-amj-2024-05-24T03_53_05.368902.txt

# TESTING

- First time online : `pip install -e .`
- from the root of project folder: `pytest tests/*.py`



# Versions
- 0.6.9 - filter our ClinicalAssessments as it does not have anything Outcome Measures related.
- 0.6.8 - Program consistency check between asmts and eps was too limiting - not required as all within legit NADA programs.
- 0.6.7 - discover right dataset (ep period) matching, bugfix sdata expand, only errors for inperiod, code export tools(for AI support)
- 0.6.6 - move NADA programs config from local .py to configuration.json (blob)
- 0.6.5 - eps no parquet, handle aggrgate when missing error type
- 0.6.4 - added date_str_format for final survey.txt output. Todo: Stage# Calc needs older episodes, assessments than period
- 0.6.3 - Staff/Provider from MDS added.
- 0.6.2 - NearestMatch expected via config dict
- 0.6.1 - update requiremnts.txt for rapidfuzz library
- 0.6.0 - simplify cache-miss logic for period-based assessment loading.
- 0.5.9 - missing assessment cache => load only the requested (reporting) period.
- 0.5.8 - nearest SLK match. including tests. checking in configuration.json
- 0.5.7 - better logging (fix_incorrect_program), exceptions
- 0.5.6 - forgot to add __init for importer config module.
- 0.5.5 - Blob Config load, common interface, LogWarn: imported dataset doesn't have one or more columns of interest, error stats
- 0.5.4 - code path - no config - errors columns
- 0.5.3 - Load Blob Config for drug Mapping, etc
- 0.5.2 - Fixed Destination Paths (on blob storage)
- 0.5.1 - AOD warnings, rename reindexed file to match format prefix_date-range_suffix: prefix:forstxt_
- 0.5.0 - write redindexed to csv (instead of parq).
         - Survey.txt write to blob storage.
- 0.4.0 - remove all local writes and reads, refactor, fix errors_warning writes
- 0.3.1 - removing disk writes as read-only on cloud fs
