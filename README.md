# Assessment-Episodes Matcher

## Overview

The Assessment-Episodes Matcher is a Python package designed for alcohol and drug treatment services data processing. It performs two main functions:
1. Matching assessment data (ATOM) with episode data (MDS)
2. Generating NADA (Network of Alcohol and other Drugs Agencies) compliant survey text files (survey.txt)

This package is primarily used by Azure Functions to process and report treatment data. *Refer to the [NADA AzFunc](https://github.com/dactechie/NADATools_AzFunc) project's homepage/README for a code flow diagram.*

## Features

- Match assessments to episodes based on SLK, Program, and date ranges
- Process and transform data from various sources (Azure Table Storage, Blob Storage)
- Generate NADA-compliant survey text files (survey.txt)
- Handle complex matching scenarios with configurable slack periods
- Comprehensive error handling and audit logging
- Flexible configuration management
- Integration with Azure Functions for serverless operation


**Note**:  see [README_dev.md](https://github.com/dactechie/assessment_episode_matcher/blob/main/README_dev.md) for version details.


## Installation

To install the package, run:

```bash
pip install assessment-episodes-matcher
```

## Quick Start

```python
from assessment_episode_matcher.matching import main as match_helper
from assessment_episode_matcher.nada import generate_nada_save

# Match assessments to episodes
final_good, ew = match_helper.match_and_get_issues(e_df, a_df, 
                                                   inperiod_atomslk_notin_ep,
                                                   inperiod_epslk_notin_atom,
                                                   slack_for_matching,
                                                   config)

# Generate NADA export
nada_records, warnings_aod = generate_nada_save(reporting_start_str, 
                                                reporting_end_str,
                                                container_name,
                                                config)
```

## Configuration

The package uses a configuration file for various settings. Create a `configuration.json` file in your Azure Blob container:

```json
{
  "MATCHING_NDAYS_SLACK": 7,
  "AZURE_BLOB_CONTAINER": "your-container-name",
  "EstablishmentID_Program": {
    "12A002": "PROGRAM1",
    "12A003": "PROGRAM2"
  },
  "purpose_programs": {
    "NADA": ["PROGRAM1", "PROGRAM2"]
  },
  "drug_categories": {
    "Alcohol": ["Alcohol"],
    "Cannabis": ["Cannabis"]
  }
}
```
* The `PROGRAM1`, `PROGRAM2` are the program codes used in assessment records. For instance by the [ATOM tool](https://github.com/dactechie/ansa-surveyjs).

## Usage with Azure Functions

The package is primarily used by Azure Functions. Here's an example of how it's utilized (snippet from [NADA_AZFunc](https://github.com/dactechie/NADATools_AzFunc) ):

```python
import assessment_episode_matcher.matching_helper as ATOMEpisodeMatcher
import assessment_episode_matcher.nada_helper as NADAImportFileGenerator

def perform_mds_atom_matches(req: func.HttpRequest) -> func.HttpResponse:
    start_dt = req.params.get('start_date', "")
    end_dt = req.params.get('end_date', "")
    
    result = ATOMEpisodeMatcher.run(start_yyyymmd=start_dt,
                                    end_yyyymmd=end_dt)
    
    return func.HttpResponse(body=json.dumps(result),
                             mimetype="application/json",
                             status_code=200)

def generate_surveytxt(req: func.HttpRequest) -> func.HttpResponse:
    start_dt = req.params.get('start_date', "")
    end_dt = req.params.get('end_date', "")
    
    result = NADAImportFileGenerator.run(start_yyyymmd=start_dt,
                                         end_yyyymmd=end_dt)
    
    return func.HttpResponse(body=json.dumps(result),
                             mimetype="application/json",
                             status_code=200)
```

## Testing

Run the test suite:

```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.

## Contact

Aftab MJ - amj@eml.cc

Project Link: [https://github.com/dactechie/assessment-episodes-matcher](https://github.com/dactechie/assessment-episodes-matcher)