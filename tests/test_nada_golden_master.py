"""
GOLDEN MASTER (characterisation) suite for the NADA ``Survey.csv`` generator.

This suite exists so the generator can be PORTED into another repo and the port
proved byte-identical.  It therefore records what the code DOES TODAY, bugs and
all.  Several assertions below are deliberately assertions of WRONG behaviour;
each one is flagged with a ``CHARACTERISATION`` comment.  Do not "fix" a test
here to make it read nicer -- if the behaviour changes, the port changed.

All fixtures are SYNTHETIC.  No client data, no Azure, no network, no database.

--------------------------------------------------------------------------
1.  THE CALL SEQUENCE UNDER TEST
--------------------------------------------------------------------------
The orchestration lives in a different repo
(``NADATools_AzFunc/nada_helper.py::generate_nada_export``).  ``build_survey_df``
below reproduces it with the Azure I/O removed:

    matched["Stage"] = NADAbase.get_stage_per_episode(matched)
    filtered         = matched[AssessmentDate within [start, end]]
    prepped, warns   = data_prep.prep_nada_fields(filtered, config)
    survey_df        = NADAbase.generate_finaloutput_df(prepped)

``assessment_episode_matcher/nada.py`` is broken (it imports a non-existent
``prep_dataframe_nada``) and is deliberately not used.

--------------------------------------------------------------------------
2.  THE INPUT CONTRACT  (derived by reading the code, verified by running it)
--------------------------------------------------------------------------
Input is one row per matched assessment: the ATOM assessment joined to its MDS
episode.  In production it arrives from ``forstxt_<period>_matched.csv`` read
with ``dtype=str``, so EVERY column is a string.

Required columns
----------------
``SurveyData``      JSON **string**.  Parsed by ``clean_and_parse_json``; the
                    only failure it catches is ``JSONDecodeError`` (which yields
                    ``{}``).  A non-string value (NaN, dict, None) raises
                    ``AttributeError`` and kills the run -- see
                    ``test_characterise_non_string_surveydata_raises``.
                    A payload that parses to anything other than a dict (e.g.
                    ``"[]"``) causes the ROW TO BE SILENTLY DROPPED.
``AssessmentDate``  ``'%Y-%m-%d'`` string.  Used three times, each differently:
                      * ``get_stage_per_episode`` sorts it LEXICALLY (correct
                        only because ISO dates sort chronologically as text);
                      * the reporting-period filter parses it with
                        ``pd.to_datetime(..., dayfirst=True)``;
                      * ``generate_finaloutput_df`` reformats it with
                        ``date_str_format(infmt='%Y-%m-%d', outfmt='%d%m%Y')``,
                        which RAISES ``ValueError`` on any other layout.
``PMSEpisodeID``    Episode key.  ``get_stage_per_episode`` groups on it.
``PMSPersonID``     Emitted zero-filled to width 4.
``PDCCode``         MDS principal-drug code.  Emitted zero-filled to width 4.
``SLK``             Statistical linkage key.  ``prep_nada_fields`` sorts on
                    ``["SLK", "AssessmentDate"]``.
``RowKey``          Per-client assessment id; only used in warning records.
``ESTABLISHMENT IDENTIFIER``
                    Renamed to ``AgencyCode`` -- the first output column.
``Stage``           Added by the caller, not by ``prep_nada_fields``.

Columns that are merely carried and may be absent: ``PartitionKey``,
``Program``, ``Staff``, ``SurveyName``, ``CommencementDate``, ``EndDate``,
``GEOGRAPHICAL LOCATION``, ``PDCSubstanceOfConcern``.

Row-count requirement (undocumented, load-bearing)
--------------------------------------------------
A batch of EXACTLY TWO rows is silently corrupted, and crashes outright if its
integer index is not ``{0, 1}``.  See
``test_characterise_two_row_batch_corrupts_duplicated_day_counts``.  Any other
row count is fine at any index.

What must be inside the SurveyData JSON
---------------------------------------
It is flattened with ``pd.json_normalize(..., max_level=1)``, so one level of
nesting becomes dotted column names.  Keys the pipeline looks for:

*Drug information* -- ``create_structure_masks`` classifies each row:
  * NEW structure: both ``DrugsOfConcernDetails`` (list of dicts keyed
    ``DrugsOfConcern`` / ``MethodOfUse`` / ``DaysInLast28`` / ``Units`` /
    ``HowMuchPerOccasion`` / ``Goals``) and ``PDCSubstanceOrGambling`` (the
    name of the principal drug) are non-null.
  * OLD structure: ``PDC`` and/or ``ODC`` non-null.  ``PDC`` items are keyed
    ``PDCSubstanceOrGambling`` / ``PDCDaysInLast28`` / ``PDCHowMuchPerOccasion``
    / ``PDCUnits`` / ``PDCMethodOfUse`` / ``PDCGoals``; ``ODC`` items are keyed
    ``OtherSubstancesConcernGambling`` / ``DaysInLast28`` / ``HowMuchPerOccasion``
    / ``Units`` / ``MethodOfUse`` / ``Goals``.
  * Rows matching NEITHER get an ``AODWarning(field_name='structure')`` and are
    KEPT in the output with empty drug columns -- they are NOT dropped.
  * Rows matching BOTH poison the whole batch -- see
    ``test_characterise_row_with_both_structures_wipes_drug_data_for_batch``.

*Multi-select answers* (``data_config.mulselect_option_to_nadafield``):
  ``Past4WkAodRisks``   list of strings -> ``ATOPHomeless`` ("Homeless"),
                        ``ATOPRiskEviction`` ("At risk of eviction"),
                        ``Past4Wk_ViolentToYou`` ("Violence / Assault")
  ``PrimaryCaregiver``  list of strings -> ``PrimaryCaregiver_0-5``,
                        ``PrimaryCaregiver_5-15``
  ``Past4WkEngagedInOtheractivities``  dict -> after flattening, the columns
                        ``...​.Paid Work`` and ``....Study - college, school or
                        vocational education``; the nested ``{'Days': n}`` value
                        is read straight out, giving ``PaidWorkDays`` /
                        ``StudyDays``.  These two are NOT booleanised.

*Yes/No answers* -> '0' if exactly 'No', '1' if any other non-null value,
  ``None`` if null: ``Past4WkBeenArrested``, ``Past4WkHaveYouViolenceAbusive``.

*Numeric answers* (``data_config.data_types``) coerced with
  ``pd.to_numeric(errors='coerce')`` then truncated to int strings:
  the five ``SDS*`` questions, ``Past4WkPhysicalHealth``,
  ``Past4WkMentalHealth``, ``Past4WkQualityOfLifeScore``.

*Plain passthrough*: ``K10Q01``..``K10Q14``, ``Past4WkNumInjectingDays``,
  ``ATOPInjectedUsedEquipment``.

*Silently discarded*: any key whose name contains ``Comment``, ``Note`` or
  ``ITSP`` (case-insensitive) is dropped by ``drop_fields_by_regex``.  Any key
  named ``Program``, ``Staff``, ``AssessmentDate`` or ``SLK`` inside SurveyData
  is dropped in favour of the parent column (``data_config.keep_parent_fields``).

Config
------
``configuration.json`` from the repo root.  Only ``drug_categories`` is read by
this code path.

--------------------------------------------------------------------------
3.  REGENERATING THE SNAPSHOTS
--------------------------------------------------------------------------
Snapshots live in ``tests/golden/``.  To rewrite them after a DELIBERATE
behaviour change:

    REGENERATE_NADA_GOLDEN=1 python -m pytest tests/test_nada_golden_master.py

The run rewrites every snapshot file and still asserts, so a regeneration run
passes trivially -- always inspect ``git diff tests/golden/`` afterwards.

Three scenarios are snapshotted:
  ``nada_survey_main``            6 rows, 4 episodes: old + new drug structures
                                  in one frame, mapped and unmapped drugs,
                                  multi-selects, K10/SDS, blanks, zfill,
                                  fractional numerics, a 3-assessment episode,
                                  an unsorted input, an invalid-structure row.
  ``nada_survey_dual_structure``  a row carrying BOTH drug structures, which
                                  wipes drug data for the whole batch.
  ``nada_survey_two_row_batch``   a 2-row batch, the size at which the
                                  duplicated day-count columns are corrupted.

Each scenario is snapshotted three ways:
  ``<name>.csv``         exactly what ``DataFrame.to_csv(index=False)`` emits
                         -- this is the delivered file format, column order
                         included.
  ``<name>.typed.json``  ``{"columns": [...], "rows": [[[tag, text], ...]]}``
                         where the tag distinguishes ``str`` / ``int`` /
                         ``float`` / ``nan`` / ``none``.  The CSV cannot tell
                         ``''`` from ``NaN`` from ``None``; this can, and the
                         difference is real in the DataFrame.
  ``<name>.warnings.json``  the ``AODWarning`` list returned alongside.
"""

import json
import math
import os
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import pytest

from assessment_episode_matcher.data_prep import prep_nada_fields
from assessment_episode_matcher.exporters import NADAbase as nada_df_generator
from assessment_episode_matcher.exporters.config.NADAbase import (
    nada_final_fields,
    notanswered_defaults,
)
from assessment_episode_matcher.importers.aod import expand_drug_info
from assessment_episode_matcher.mytypes import DataKeys as dk
from assessment_episode_matcher.utils.df_ops_base import prescribe_fields
from assessment_episode_matcher.utils.fromstr import get_date_from_str

REPO_ROOT = Path(__file__).resolve().parents[1]
GOLDEN_DIR = Path(__file__).resolve().parent / "golden"
REGENERATE = os.environ.get("REGENERATE_NADA_GOLDEN", "") not in ("", "0", "false")


# --------------------------------------------------------------------------
# harness: the AzFunc call sequence, minus the Azure I/O
# --------------------------------------------------------------------------
def build_survey_df(matched_assessments, reporting_start_str, reporting_end_str, config):
    """Reproduces NADATools_AzFunc/nada_helper.py::generate_nada_export."""
    matched = matched_assessments.copy()
    matched["Stage"] = nada_df_generator.get_stage_per_episode(matched)

    reporting_start = get_date_from_str(reporting_start_str, "%Y%m%d")
    reporting_end = get_date_from_str(reporting_end_str, "%Y%m%d")
    asmtdt_field = dk.assessment_date.value
    dates = pd.to_datetime(matched[asmtdt_field], dayfirst=True)
    filtered = matched[
        (dates >= pd.Timestamp(reporting_start)) & (dates <= pd.Timestamp(reporting_end))
    ]

    prepped, warnings_aod = prep_nada_fields(filtered, config)
    return nada_df_generator.generate_finaloutput_df(prepped), warnings_aod


@pytest.fixture(scope="module")
def config():
    """The real repo-root configuration.json (drug_categories is what matters)."""
    with (REPO_ROOT / "configuration.json").open() as fh:
        return json.load(fh)


# --------------------------------------------------------------------------
# synthetic fixture builders
# --------------------------------------------------------------------------
def survey(**fields):
    """A SurveyData payload: the pipeline requires a JSON *string*."""
    return json.dumps(fields)


def _episode_defaults():
    return {
        "PartitionKey": "SYNTH",
        "Program": "EUROPATH",
        "Staff": "Synthetic Staff",
        "SurveyName": "ATOP",
        "CommencementDate": "2025-01-01",
        "EndDate": "2025-06-30",
    }


def build_matched_assessments():
    """The main golden fixture.

    Rows are deliberately NOT in SLK/date order, so the
    ``sort_values(["SLK", "AssessmentDate"])`` inside ``prep_nada_fields`` is
    exercised by the snapshot.  Every row is inside the reporting window and
    every SurveyData payload parses to a dict, because either condition failing
    makes the whole run crash (characterised separately below).
    """
    d = _episode_defaults()
    rows = [
        # -- episode 100002: NEW structure (DrugsOfConcernDetails).
        #    Blank PMSPersonID and PDCCode exercise zfill on empty strings.
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03076",
            "Program": "SAPPHIRE",
            "PMSEpisodeID": "100002",
            "PMSPersonID": "",
            "PDCCode": "",
            "SLK": "BBBCCC020219852",
            "RowKey": "rk-0002-a",
            "AssessmentDate": "2025-02-20",
            "SurveyData": survey(
                PDCSubstanceOrGambling="Ethanol",
                DrugsOfConcernDetails=[
                    {
                        "DrugsOfConcern": "Ethanol",
                        "MethodOfUse": "Ingest",
                        "DaysInLast28": "20",
                        "Units": "standard drinks",
                        "HowMuchPerOccasion": "6",
                        "Goals": "Reduce",
                    },
                    {
                        "DrugsOfConcern": "Cannabinoids",
                        "MethodOfUse": "Smoke",
                        "DaysInLast28": "5",
                        "Units": "cones / joints",
                        "HowMuchPerOccasion": "3",
                    },
                    # unmapped -> Another Drug1; range value exercises range_average
                    {
                        "DrugsOfConcern": "Kava",
                        "MethodOfUse": "Ingest",
                        "DaysInLast28": "7",
                        "Units": "bowls",
                        "HowMuchPerOccasion": "50-59",
                    },
                    # second unmapped -> Another Drug2
                    {
                        "DrugsOfConcern": "Kratom",
                        "MethodOfUse": "Ingest",
                        "DaysInLast28": "1",
                        "Units": "grams",
                        "HowMuchPerOccasion": "2",
                    },
                ],
                SDSIsAODUseOutOfControl="4",
                K10Q01="2",
                Past4WkAodRisks=[],
                PrimaryCaregiver=[],
                Past4WkEngagedInOtheractivities={},
            ),
        },
        # -- episode 100001, 2nd assessment chronologically (Stage 1)
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03062",
            "PMSEpisodeID": "100001",
            "PMSPersonID": "77",
            "PDCCode": "1201",
            "SLK": "AAABBB010119901",
            "RowKey": "rk-0001-b",
            "AssessmentDate": "2025-02-17",
            "SurveyData": survey(
                PDC=[
                    {
                        "PDCSubstanceOrGambling": "Cocaine",
                        "PDCMethodOfUse": "Snort",
                        "PDCDaysInLast28": "4",
                        "PDCUnits": "pills",
                        # no PDCHowMuchPerOccasion -> Units/TypicalQtyStr blanked
                    }
                ],
                ODC=[],
                SDSIsAODUseOutOfControl="2.7",  # fractional -> truncated
                Past4WkAodRisks=["Homeless"],
                PrimaryCaregiver=[],
                Past4WkEngagedInOtheractivities={
                    "Study - college, school or vocational education": {"Days": "9"}
                },
                Past4WkBeenArrested="No",
                Past4WkPhysicalHealth="4.9",  # fractional -> truncated
            ),
        },
        # -- episode 100001, 1st assessment chronologically (Stage 0)
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03062",
            "PMSEpisodeID": "100001",
            "PMSPersonID": "77",
            "PDCCode": "1201",
            "SLK": "AAABBB010119901",
            "RowKey": "rk-0001-a",
            "AssessmentDate": "2025-02-03",
            "SurveyData": survey(
                PDC=[
                    {
                        "PDCSubstanceOrGambling": "Heroin",
                        "PDCMethodOfUse": "Inject",
                        "PDCDaysInLast28": "14",
                        "PDCUnits": "caps",
                        "PDCHowMuchPerOccasion": "2",
                        "PDCGoals": "Reduce",
                    }
                ],
                ODC=[
                    {
                        "OtherSubstancesConcernGambling": "Diazepam",
                        "MethodOfUse": "Ingest",
                        "DaysInLast28": "9",
                        "Units": "tablets",
                        "HowMuchPerOccasion": "3",
                    }
                ],
                SDSIsAODUseOutOfControl="3",
                SDSDoesMissingFixMakeAnxious="2",
                SDSHowMuchDoYouWorryAboutAODUse="1",
                SDSDoYouWishToStop="0",
                SDSHowDifficultToStopOrGoWithout="2",
                K10Q01="1",
                K10Q02="2",
                K10Q03="3",
                K10Q04="4",
                K10Q05="5",
                K10Q06="1",
                K10Q07="2",
                K10Q08="3",
                K10Q09="4",
                K10Q10="5",
                K10Q11="1",
                K10Q12="2",
                K10Q13="3",
                K10Q14="4",
                Past4WkAodRisks=["Homeless", "Violence / Assault"],
                PrimaryCaregiver=[
                    "Yes - primary caregiver: children under 5 years old"
                ],
                Past4WkEngagedInOtheractivities={
                    "Paid Work": {"Days": "12"},
                    "Study - college, school or vocational education": {"Days": "4"},
                },
                Past4WkBeenArrested="No",
                Past4WkHaveYouViolenceAbusive="Yes (risk assessment required)",
                Past4WkMentalHealth="7",
                Past4WkPhysicalHealth="6",
                Past4WkQualityOfLifeScore="8",
                Past4WkNumInjectingDays="10",
                ATOPInjectedUsedEquipment="",  # empty -> -1 via notanswered_defaults
                # these three are dropped by drop_fields_by_regex
                PDCGoalsComment="dropped: contains 'Comment'",
                CaseNote="dropped: contains 'Note'",
                ITSPGoal="dropped: contains 'ITSP'",
                # these four lose to the parent columns (keep_parent_fields)
                Program="IGNORED-SURVEY-PROGRAM",
                Staff="IGNORED-SURVEY-STAFF",
                AssessmentDate="1999-09-09",
                SLK="IGNOREDSLK",
            ),
        },
        # -- episode 100001, 3rd assessment chronologically (Stage 2)
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03062",
            "PMSEpisodeID": "100001",
            "PMSPersonID": "77",
            "PDCCode": "1201",
            "SLK": "AAABBB010119901",
            "RowKey": "rk-0001-c",
            "AssessmentDate": "2025-03-10",
            "SurveyData": survey(
                PDC=[
                    {
                        "PDCSubstanceOrGambling": "Heroin",
                        "PDCDaysInLast28": "2",
                        "PDCUnits": "caps",
                        "PDCHowMuchPerOccasion": "1",
                    }
                ],
                ODC=[],
                SDSIsAODUseOutOfControl="1",
                K10Q01="1",
                Past4WkAodRisks=["At risk of eviction"],
                PrimaryCaregiver=[
                    "Yes - primary caregiver: children 5 - 15 years old"
                ],
                Past4WkEngagedInOtheractivities={"Paid Work": {"Days": "0"}},
                Past4WkBeenArrested="Yes - please provide details",
                Past4WkMentalHealth="",  # empty -> -1
                Past4WkPhysicalHealth="5",
            ),
        },
        # -- episode 100003: no drug fields at all -> 'structure' warning, row KEPT
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "13K034",
            "Program": "MURMICE",
            "PMSEpisodeID": "100003",
            "PMSPersonID": "9",
            "PDCCode": "0",
            "SLK": "DDDEEE030319803",
            "RowKey": "rk-0003-a",
            "AssessmentDate": "2025-01-15",
            "SurveyData": survey(SDSIsAODUseOutOfControl="2", K10Q01="5"),
        },
        # -- episode 100004: quantity '0' and quantity 'Other';
        #    unmapped PDC AND unmapped ODC collide on 'Another Drug1'
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "13Q035",
            "Program": "GOLBICE",
            "PMSEpisodeID": "100004",
            "PMSPersonID": "12345",  # already 5 wide -> zfill is a no-op
            "PDCCode": "12",
            "SLK": "EEEFFF040419754",
            "RowKey": "rk-0004-a",
            "AssessmentDate": "2025-02-28",
            "SurveyData": survey(
                PDC=[
                    {
                        "PDCSubstanceOrGambling": "Betel Nut",  # unmapped
                        "PDCDaysInLast28": "11",
                        "PDCUnits": "nuts",
                        "PDCHowMuchPerOccasion": "5",
                    }
                ],
                ODC=[
                    {
                        "OtherSubstancesConcernGambling": "Methamphetamine",
                        "DaysInLast28": "6",
                        "Units": "points",
                        "HowMuchPerOccasion": "0",  # '0' -> Units blanked
                    },
                    {
                        "OtherSubstancesConcernGambling": "Oxycodone",
                        "DaysInLast28": "3",
                        "Units": "tablets",
                        "HowMuchPerOccasion": "Other",  # -> everything blanked
                    },
                    {
                        "OtherSubstancesConcernGambling": "Yerba Mate",  # unmapped
                        "DaysInLast28": "2",
                        "Units": "gourds",
                        "HowMuchPerOccasion": "4",
                    },
                ],
                SDSIsAODUseOutOfControl="5",
                Past4WkAodRisks=["Violence / Assault"],
                PrimaryCaregiver=[],
                Past4WkEngagedInOtheractivities={"Paid Work": {"Days": "20"}},
                Past4WkQualityOfLifeScore="3",
            ),
        },
    ]
    # dtype=str everywhere: that is how the matched CSV is read in production.
    return pd.DataFrame(rows).astype(str)


def build_dual_structure_assessments():
    """Two rows; the first carries BOTH the new and the old drug structure."""
    d = _episode_defaults()
    rows = [
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03062",
            "PMSEpisodeID": "200001",
            "PMSPersonID": "5",
            "PDCCode": "1201",
            "SLK": "AAABBB010119901",
            "RowKey": "rk-dual",
            "AssessmentDate": "2025-02-01",
            "SurveyData": survey(
                PDC=[
                    {
                        "PDCSubstanceOrGambling": "Heroin",
                        "PDCDaysInLast28": "5",
                        "PDCUnits": "caps",
                        "PDCHowMuchPerOccasion": "2",
                    }
                ],
                PDCSubstanceOrGambling="Heroin",
                DrugsOfConcernDetails=[
                    {
                        "DrugsOfConcern": "Heroin",
                        "DaysInLast28": "5",
                        "Units": "caps",
                        "HowMuchPerOccasion": "2",
                    }
                ],
            ),
        },
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03062",
            "PMSEpisodeID": "200002",
            "PMSPersonID": "6",
            "PDCCode": "1301",
            "SLK": "BBBCCC020219852",
            "RowKey": "rk-clean",
            "AssessmentDate": "2025-02-02",
            "SurveyData": survey(
                PDC=[
                    {
                        "PDCSubstanceOrGambling": "Cocaine",
                        "PDCDaysInLast28": "9",
                        "PDCUnits": "grams",
                        "PDCHowMuchPerOccasion": "1",
                    }
                ]
            ),
        },
    ]
    return pd.DataFrame(rows).astype(str)


def build_two_row_batch():
    """Exactly two rows, both carrying day-counts in duplicated NADA columns.

    Two is the pathological batch size -- see
    ``test_characterise_two_row_batch_corrupts_duplicated_day_counts``.
    """
    d = _episode_defaults()
    rows = [
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03062",
            "PMSEpisodeID": "300001",
            "PMSPersonID": "11",
            "PDCCode": "1201",
            "SLK": "AAABBB010119901",
            "RowKey": "rk-two-a",
            "AssessmentDate": "2025-02-01",
            "SurveyData": survey(
                PDC=[{"PDCSubstanceOrGambling": "Heroin", "PDCDaysInLast28": "14",
                      "PDCUnits": "caps", "PDCHowMuchPerOccasion": "2"}],
                ODC=[{"OtherSubstancesConcernGambling": "Ethanol",
                      "DaysInLast28": "21", "Units": "standard drinks",
                      "HowMuchPerOccasion": "8"}],
            ),
        },
        {
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03076",
            "PMSEpisodeID": "300002",
            "PMSPersonID": "22",
            "PDCCode": "1301",
            "SLK": "BBBCCC020219852",
            "RowKey": "rk-two-b",
            "AssessmentDate": "2025-02-02",
            "SurveyData": survey(
                PDC=[{"PDCSubstanceOrGambling": "Heroin", "PDCDaysInLast28": "3",
                      "PDCUnits": "caps", "PDCHowMuchPerOccasion": "1"}],
                ODC=[{"OtherSubstancesConcernGambling": "Ethanol",
                      "DaysInLast28": "5", "Units": "standard drinks",
                      "HowMuchPerOccasion": "2"}],
            ),
        },
    ]
    return pd.DataFrame(rows).astype(str)


# --------------------------------------------------------------------------
# snapshot encoding / comparison
# --------------------------------------------------------------------------
def encode_cell(value):
    """Encode one cell so ``''``, ``NaN`` and ``None`` stay distinguishable."""
    if value is None:
        return ["none", ""]
    if value is pd.NA or value is pd.NaT:
        return ["na", ""]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, bool):
        return ["bool", str(value)]
    if isinstance(value, float):
        if math.isnan(value):
            return ["nan", ""]
        return ["float", repr(value)]
    if isinstance(value, int):
        return ["int", str(value)]
    return ["other", repr(value)]


def encode_frame(df):
    # positional access: the frame has duplicate column labels by design
    return {
        "columns": list(df.columns),
        "rows": [
            [encode_cell(df.iloc[r, c]) for c in range(df.shape[1])]
            for r in range(df.shape[0])
        ],
    }


def encode_warnings(warnings_aod):
    return [asdict(w) for w in warnings_aod]


def _snapshot(name, suffix, payload_text):
    """Read a snapshot; rewrite it first only when explicitly regenerating.

    A missing snapshot is a FAILURE, not something to quietly create -- these
    files are the committed record of current behaviour.
    """
    path = GOLDEN_DIR / f"{name}{suffix}"
    if REGENERATE:
        GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(payload_text, encoding="utf-8")
    if not path.exists():
        raise AssertionError(
            f"missing golden snapshot {path}. If this is intentional, run:\n"
            f"  REGENERATE_NADA_GOLDEN=1 python -m pytest {Path(__file__).name}"
        )
    return path.read_text(encoding="utf-8")


def snapshot_scenario(name, survey_df, warnings_aod):
    """Compare a scenario against its three snapshot files."""
    actual_csv = survey_df.to_csv(index=False, lineterminator="\n")
    expected_csv = _snapshot(name, ".csv", actual_csv)

    actual_typed = json.dumps(encode_frame(survey_df), indent=1) + "\n"
    expected_typed = _snapshot(name, ".typed.json", actual_typed)

    actual_warn = json.dumps(encode_warnings(warnings_aod), indent=1) + "\n"
    expected_warn = _snapshot(name, ".warnings.json", actual_warn)

    expected = json.loads(expected_typed)

    # column order IS the file format
    assert list(survey_df.columns) == expected["columns"]
    # cell by cell, types included
    assert encode_frame(survey_df)["rows"] == expected["rows"]
    # and the bytes we actually ship
    assert actual_csv == expected_csv
    assert json.loads(actual_warn) == json.loads(expected_warn)


# --------------------------------------------------------------------------
# module-scoped pipeline runs
# --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def main_run(config):
    return build_survey_df(build_matched_assessments(), "20250101", "20251231", config)


@pytest.fixture(scope="module")
def dual_run(config):
    return build_survey_df(
        build_dual_structure_assessments(), "20250101", "20251231", config
    )


@pytest.fixture(scope="module")
def two_row_run(config):
    return build_survey_df(build_two_row_batch(), "20250101", "20251231", config)


# ==========================================================================
# 1. the output shape -- 172 columns, in order
# ==========================================================================
def test_final_field_list_is_172_entries_164_distinct():
    # 8 names appear twice on purpose: NADA asks for the day-counts in both the
    # DU block and the ATOP block.
    assert len(nada_final_fields) == 172
    assert len(set(nada_final_fields)) == 164
    repeated = sorted({f for f in nada_final_fields if nada_final_fields.count(f) > 1})
    assert repeated == [
        "Alcohol_DaysInLast28",
        "Amphetamines_DaysInLast28",
        "Another Drug1_DaysInLast28",
        "Benzodiazepines_DaysInLast28",
        "Cannabis_DaysInLast28",
        "Cocaine_DaysInLast28",
        "Heroin_DaysInLast28",
        "Other Opioids_DaysInLast28",
    ]


def test_output_column_count_and_exact_order(main_run):
    survey_df, _ = main_run
    assert survey_df.shape[1] == 172
    assert list(survey_df.columns) == nada_final_fields
    assert list(survey_df.columns)[:6] == [
        "AgencyCode",
        "PMSEpisodeID",
        "PMSPersonID",
        "Stage",
        "AssessmentDate",
        "PDCCode",
    ]


def test_duplicated_day_count_columns_hold_identical_values(main_run):
    """True for every batch size EXCEPT two -- see the 2-row characterisation."""
    survey_df, _ = main_run
    assert len(survey_df) != 2
    for name in {f for f in nada_final_fields if nada_final_fields.count(f) > 1}:
        positions = [i for i, c in enumerate(survey_df.columns) if c == name]
        assert len(positions) == 2
        left = survey_df.iloc[:, positions[0]].tolist()
        right = survey_df.iloc[:, positions[1]].tolist()
        assert encode_cell_list(left) == encode_cell_list(right), name


def encode_cell_list(values):
    return [encode_cell(v) for v in values]


# ==========================================================================
# 2. the golden masters
# ==========================================================================
def test_golden_master_main(main_run):
    survey_df, warnings_aod = main_run
    assert len(survey_df) == 6
    snapshot_scenario("nada_survey_main", survey_df, warnings_aod)


def test_golden_master_dual_structure(dual_run):
    survey_df, warnings_aod = dual_run
    assert len(survey_df) == 2
    snapshot_scenario("nada_survey_dual_structure", survey_df, warnings_aod)


def test_golden_master_two_row_batch(two_row_run):
    survey_df, warnings_aod = two_row_run
    assert len(survey_df) == 2
    snapshot_scenario("nada_survey_two_row_batch", survey_df, warnings_aod)


# ==========================================================================
# 3. behaviour the golden master encodes, asserted explicitly so a failure
#    says WHAT changed rather than just "the snapshot moved"
# ==========================================================================
def test_rows_are_sorted_by_slk_then_assessmentdate(main_run):
    survey_df, _ = main_run
    # input order is scrambled; prep_nada_fields sorts on SLK then AssessmentDate
    assert survey_df["PMSEpisodeID"].tolist() == [
        "100001",
        "100001",
        "100001",
        "100002",
        "100003",
        "100004",
    ]
    assert survey_df["AssessmentDate"].tolist() == [
        "03022025",
        "17022025",
        "10032025",
        "20022025",
        "15012025",
        "28022025",
    ]


def test_stage_is_zero_based_within_an_episode(main_run):
    survey_df, _ = main_run
    ep1 = survey_df[survey_df["PMSEpisodeID"] == "100001"]
    # get_stage_per_episode is groupby(...).cumcount(): FIRST assessment is 0.
    assert ep1["Stage"].tolist() == [0, 1, 2]
    assert survey_df[survey_df["PMSEpisodeID"] == "100002"]["Stage"].tolist() == [0]


def test_assessmentdate_is_reformatted_ddmmyyyy(main_run):
    survey_df, _ = main_run
    assert "03022025" in survey_df["AssessmentDate"].tolist()  # 2025-02-03


def test_mapped_and_unmapped_drugs_land_in_the_right_buckets(main_run):
    survey_df, _ = main_run
    row = survey_df[survey_df["PMSEpisodeID"] == "100002"]
    assert row["Another Drug1"].iloc[0] == "Kava"
    assert row["Another Drug2"].iloc[0] == "Kratom"
    assert row["Alcohol_TypicalQtyStr"].iloc[0] == "6.0; standard drinks"
    assert row["Cannabis_TypicalQtyStr"].iloc[0] == "3.0; cones / joints"
    # '50-59' -> range_average -> 54.5
    assert row["Another Drug1_TypicalQtyStr"].iloc[0] == "54.5; bowls"


def test_multiselect_answers_become_zero_one_strings(main_run):
    survey_df, _ = main_run
    first = survey_df.iloc[0]
    assert first["ATOPHomeless"] == "1"
    assert first["Past4Wk_ViolentToYou"] == "1"
    assert first["ATOPRiskEviction"] == "0"
    assert first["PrimaryCaregiver_0-5"] == "1"
    assert first["PrimaryCaregiver_5-15"] == "0"
    # PaidWorkDays / StudyDays are NOT booleanised -- the nested 'Days' value
    # is copied straight through.
    assert first["PaidWorkDays"] == "12"
    assert first["StudyDays"] == "4"


def test_k10_answers_pass_through_unchanged(main_run):
    survey_df, _ = main_run
    first = survey_df.iloc[0]
    assert [first[f"K10Q{n:02d}"] for n in range(1, 15)] == [
        "1", "2", "3", "4", "5", "1", "2", "3", "4", "5", "1", "2", "3", "4",
    ]


def test_missing_answers_become_minus_one_for_notanswered_defaults(main_run):
    survey_df, _ = main_run
    third = survey_df.iloc[2]  # rk-0001-c: Past4WkMentalHealth was ''
    assert third["Past4WkMentalHealth"] == -1
    assert third["Past4WkQualityOfLifeScore"] == -1
    assert survey_df.iloc[0]["ATOPInjectedUsedEquipment"] == -1.0
    # ...and every notanswered column really is in the output
    for col in notanswered_defaults:
        assert col in survey_df.columns


def test_numeric_answers_are_truncated_to_int_strings(main_run):
    survey_df, _ = main_run
    scrambled = survey_df[survey_df["Stage"] == 1].iloc[0]
    # '2.7' -> to_numeric 2.7 -> astype(int) -> '2'.  Truncation, not rounding.
    assert scrambled["SDSIsAODUseOutOfControl"] == "2"
    assert scrambled["Past4WkPhysicalHealth"] == "4"


def test_yes_no_answers(main_run):
    survey_df, _ = main_run
    assert survey_df.iloc[0]["Past4WkBeenArrested"] == "0"  # exactly 'No'
    # anything else non-null is '1', however it is worded
    assert survey_df.iloc[2]["Past4WkBeenArrested"] == "1"


def test_surveydata_keys_shadowed_by_parent_columns_are_ignored(main_run):
    survey_df, _ = main_run
    # rk-0001-a put AssessmentDate='1999-09-09' inside SurveyData
    assert "09091999" not in survey_df["AssessmentDate"].tolist()
    assert survey_df.iloc[0]["AssessmentDate"] == "03022025"


def test_invalid_structure_row_is_kept_not_dropped(main_run):
    survey_df, warnings_aod = main_run
    assert "100003" in survey_df["PMSEpisodeID"].tolist()
    structure_warnings = [w for w in warnings_aod if w.field_name == "structure"]
    assert len(structure_warnings) == 1
    assert structure_warnings[0].RowKey == "rk-0003-a"


# ==========================================================================
# 4. CHARACTERISATION OF KNOWN DEFECTS
#    Every assertion below records behaviour that is WRONG.  It is here so a
#    port reproduces it, not because it is desirable.
# ==========================================================================
def test_characterise_units_blanked_when_quantity_missing(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    This is the defect behind the two pre-existing failures in
    ``tests/test_aod.py`` (``test_noncontiguous_indices`` /
    ``test_mixed_structures``), which assert ``*_Units == 'pills'`` and get
    ``''``.

    ``get_typical_qty`` bails out the moment the per-occasion quantity is
    falsy and returns an empty unit, so a KNOWN unit is discarded whenever the
    quantity is absent.  The user-visible casualty is ``*_TypicalQtyStr``,
    which IS a NADA output column.
    """
    df = pd.DataFrame({"SLK": ["S1"], "RowKey": ["r1"]})
    df["PDC"] = None
    df["ODC"] = None
    df._set_value(
        0,
        "ODC",
        [
            {
                "OtherSubstancesConcernGambling": "Cocaine",
                "DaysInLast28": "4",
                "Units": "pills",
                # no HowMuchPerOccasion
            }
        ],
    )
    out, warnings_aod = expand_drug_info(df, config)

    assert out.loc[0, "Cocaine_DaysInLast28"] == "4"
    assert out.loc[0, "Cocaine_Units"] == ""  # WRONG: the unit was 'pills'
    assert out.loc[0, "Cocaine_TypicalQtyStr"] == ""  # WRONG: loses the unit too
    # the warning that is raised names the FIELD, not the drug -- also wrong
    assert warnings_aod[0].drug_name == "OtherSubstancesConcernGambling"
    assert warnings_aod[0].field_name == "HowMuchPerOccasion"


def test_characterise_units_blanked_when_quantity_is_zero_or_other(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    A quantity of the string ``'0'`` yields ``TypicalQtyStr == '0'`` with the
    unit thrown away, and no warning at all.  A quantity of ``'Other'`` throws
    away everything.
    """
    def one(item):
        df = pd.DataFrame({"SLK": ["S1"], "RowKey": ["r1"]})
        df["PDC"] = None
        df["ODC"] = None
        df._set_value(0, "ODC", [item])
        return expand_drug_info(df, config)

    out_zero, warn_zero = one(
        {
            "OtherSubstancesConcernGambling": "Cocaine",
            "DaysInLast28": "4",
            "Units": "pills",
            "HowMuchPerOccasion": "0",
        }
    )
    assert out_zero.loc[0, "Cocaine_TypicalQtyStr"] == "0"
    assert out_zero.loc[0, "Cocaine_Units"] == ""  # WRONG
    assert warn_zero == []  # WRONG: silent

    out_other, warn_other = one(
        {
            "OtherSubstancesConcernGambling": "Cocaine",
            "DaysInLast28": "4",
            "Units": "pills",
            "HowMuchPerOccasion": "Other",
        }
    )
    assert out_other.loc[0, "Cocaine_TypicalQtyStr"] == ""
    assert out_other.loc[0, "Cocaine_Units"] == ""
    assert warn_other[0].field_value == "Other"


def test_characterise_per_occasion_truncated_but_typicalqtystr_is_not(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    The same 54.5 is rendered as ``'54'`` in ``*_PerOccassionUse`` (``int()``)
    and as ``'54.5'`` in ``*_TypicalQtyStr``.  Two representations of one
    answer.
    """
    df = pd.DataFrame({"SLK": ["S1"], "RowKey": ["r1"]})
    df["PDC"] = None
    df["ODC"] = None
    df._set_value(
        0,
        "ODC",
        [
            {
                "OtherSubstancesConcernGambling": "Cocaine",
                "DaysInLast28": "4",
                "HowMuchPerOccasion": "50-59",
            }
        ],
    )
    out, _ = expand_drug_info(df, config)
    assert out.loc[0, "Cocaine_PerOccassionUse"] == "54"
    assert out.loc[0, "Cocaine_TypicalQtyStr"] == "54.5"


def test_characterise_blank_pdccode_and_pmspersonid_become_0000(main_run):
    """CHARACTERISATION of a known defect -- not an endorsement.

    ``zfill(4)`` is applied to the string form of the value, so an EMPTY code
    becomes ``'0000'`` -- indistinguishable from a real code of 0.  Episode
    100002 has both fields blank; episode 100003 has ``PDCCode == '0'``.
    """
    survey_df, _ = main_run
    ep2 = survey_df[survey_df["PMSEpisodeID"] == "100002"].iloc[0]
    assert ep2["PMSPersonID"] == "0000"  # was ''
    assert ep2["PDCCode"] == "0000"  # was ''

    ep3 = survey_df[survey_df["PMSEpisodeID"] == "100003"].iloc[0]
    assert ep3["PDCCode"] == "0000"  # was '0' -- same output as blank
    assert ep3["PMSPersonID"] == "0009"

    ep4 = survey_df[survey_df["PMSEpisodeID"] == "100004"].iloc[0]
    assert ep4["PMSPersonID"] == "12345"  # longer than 4: zfill is a no-op


def test_characterise_second_unmapped_drug_slot_is_overwritten(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    There are only two 'Another Drug' slots.  The FIRST unmapped drug takes
    slot 1; every subsequent one overwrites slot 2, so with three unmapped
    drugs the middle one vanishes without any dedicated warning.
    """
    df = pd.DataFrame({"SLK": ["S1"], "RowKey": ["r1"]})
    df["PDC"] = None
    df["ODC"] = None
    df._set_value(
        0,
        "ODC",
        [
            {"OtherSubstancesConcernGambling": "D1", "DaysInLast28": "1",
             "Units": "u1", "HowMuchPerOccasion": "1"},
            {"OtherSubstancesConcernGambling": "D2", "DaysInLast28": "2",
             "Units": "u2", "HowMuchPerOccasion": "2"},
            {"OtherSubstancesConcernGambling": "D3", "DaysInLast28": "3",
             "Units": "u3", "HowMuchPerOccasion": "3"},
        ],
    )
    out, _ = expand_drug_info(df, config)
    assert out.loc[0, "Another Drug1"] == "D1"
    assert out.loc[0, "Another Drug2"] == "D3"  # D2 is gone


def test_characterise_odc_unmapped_drug_overwrites_pdc_unmapped_drug(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    PDC and ODC are expanded into two SEPARATE dicts and then merged with
    ``pdc_row_data | odc_row_data``.  Both independently allocate
    'Another Drug1', so an unmapped PRINCIPAL drug is silently replaced by an
    unmapped OTHER drug.
    """
    df = pd.DataFrame({"SLK": ["S1"], "RowKey": ["r1"]})
    df["PDC"] = None
    df["ODC"] = None
    df._set_value(
        0,
        "PDC",
        [{"PDCSubstanceOrGambling": "Kava", "PDCDaysInLast28": "3",
          "PDCUnits": "bowls", "PDCHowMuchPerOccasion": "2"}],
    )
    df._set_value(
        0,
        "ODC",
        [{"OtherSubstancesConcernGambling": "Durian", "DaysInLast28": "9",
          "Units": "fruit", "HowMuchPerOccasion": "4"}],
    )
    out, _ = expand_drug_info(df, config)
    assert out.loc[0, "Another Drug1"] == "Durian"  # the PDC 'Kava' is lost
    assert out.loc[0, "Another Drug1_DaysInLast28"] == "9"
    assert "Another Drug2" not in out.columns or pd.isna(out.loc[0, "Another Drug2"])


def test_characterise_row_with_both_structures_wipes_drug_data_for_batch(dual_run):
    """CHARACTERISATION of a known defect -- not an endorsement.

    ``create_structure_masks`` lets a row satisfy BOTH masks, so
    ``expand_drug_info`` puts it in both split frames.  ``pd.concat`` then
    produces a duplicated index and ``reindex`` raises; the blanket
    ``except`` swallows it and substitutes an EMPTY frame -- so EVERY row in
    the batch loses ALL of its drug data, including rows that were perfectly
    well formed.
    """
    survey_df, warnings_aod = dual_run
    assert [w.field_name for w in warnings_aod] == ["concat"]
    assert "duplicate labels" in warnings_aod[0].field_value

    # the clean second row had Cocaine data; it is gone
    for col in ("Heroin_DaysInLast28", "Cocaine_DaysInLast28",
                "Heroin_TypicalQtyStr", "Cocaine_TypicalQtyStr"):
        values = survey_df[col]
        frame = values.to_frame() if isinstance(values, pd.Series) else values
        assert (frame == "").all().all(), col


def test_characterise_two_row_batch_corrupts_duplicated_day_counts(two_row_run):
    """CHARACTERISATION of a known defect -- not an endorsement.

    A batch of EXACTLY TWO rows silently corrupts all 8 duplicated day-count
    columns.

    Why two: ``prescribe_fields`` does ``df_final[column] = matched_df[column]``
    on a frame whose columns contain 8 repeated labels.  pandas'
    ``DataFrame.__setitem__`` routes a repeated label into its duplicate-column
    branch only when ``len(duplicate columns) == len(value)``.  There are always
    2 duplicates, so the branch fires exactly when the frame has 2 ROWS.  That
    branch then walks the value ``value[i]`` for i in 0..1, which on a Series is
    a LABEL lookup, so it pulls a SCALAR and broadcasts it down the column.

    Result: the DU-block copy of each day-count is filled with row 1's value
    for both rows, and the ATOP-block copy with row 2's value for both rows.
    Real Heroin day counts of 14 and 3 are shipped to NADA as 14/14 and 3/3.
    """
    survey_df, _ = two_row_run
    assert len(survey_df) == 2

    positions = [i for i, c in enumerate(survey_df.columns)
                 if c == "Heroin_DaysInLast28"]
    du_copy = survey_df.iloc[:, positions[0]].tolist()
    atop_copy = survey_df.iloc[:, positions[1]].tolist()
    # the true values are ['14', '3']
    assert du_copy == ["14", "14"]  # WRONG
    assert atop_copy == ["3", "3"]  # WRONG

    positions = [i for i, c in enumerate(survey_df.columns)
                 if c == "Alcohol_DaysInLast28"]
    assert survey_df.iloc[:, positions[0]].tolist() == ["21", "21"]  # WRONG
    assert survey_df.iloc[:, positions[1]].tolist() == ["5", "5"]  # WRONG

    # a NON-duplicated column in the very same frame is correct
    assert survey_df["Alcohol_PerOccassionUse"].tolist() == ["8", "2"]


def test_characterise_two_row_batch_with_gapped_index_raises_keyerror(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    Same duplicate-column branch as above.  When the two surviving rows do not
    carry the labels 0 and 1, the label lookup fails and the export dies with a
    bare ``KeyError``.

    Both real filters can produce such an index: the reporting-period filter in
    ``generate_nada_export`` and the SurveyData validity filter inside
    ``get_surveydata_expanded``.
    """
    # via the full pipeline: 3 rows in, 1 excluded by the reporting window,
    # leaving 2 rows on index [1, 2]
    matched = build_two_row_batch()
    early = matched.iloc[[0]].copy()
    early["AssessmentDate"] = "2024-12-31"
    early["RowKey"] = "rk-two-early"
    early["PMSEpisodeID"] = "300000"
    three = pd.concat([early, matched], ignore_index=True)
    with pytest.raises(KeyError):
        build_survey_df(three, "20250101", "20251231", config)

    # and minimally, at prescribe_fields itself
    for index in ([0, 2], [1, 2]):
        gapped = pd.DataFrame(
            {"AgencyCode": ["x", "y"], "Heroin_DaysInLast28": ["1", "2"]}, index=index
        )
        with pytest.raises(KeyError):
            prescribe_fields(gapped, nada_final_fields)

    # it is specific to 2 rows + a duplicated label + an integer index:
    no_dupe_label = pd.DataFrame(
        {"AgencyCode": ["x", "y"], "Nicotine_DaysInLast28": ["1", "2"]}, index=[0, 2]
    )
    assert prescribe_fields(no_dupe_label, nada_final_fields).shape == (2, 172)

    three_rows_gapped = pd.DataFrame(
        {"AgencyCode": ["x", "y", "z"], "Heroin_DaysInLast28": ["1", "2", "3"]},
        index=[0, 2, 4],
    )
    ok = prescribe_fields(three_rows_gapped, nada_final_fields)
    heroin = [i for i, c in enumerate(ok.columns) if c == "Heroin_DaysInLast28"]
    assert ok.iloc[:, heroin[0]].tolist() == ["1", "2", "3"]
    assert ok.iloc[:, heroin[1]].tolist() == ["1", "2", "3"]


def test_characterise_non_dict_surveydata_row_is_dropped_silently(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    A SurveyData payload that parses to a non-dict (here ``"[]"``) removes the
    row with no warning of any kind.  Malformed JSON, by contrast, is logged
    and becomes ``{}`` -- so the row SURVIVES with every answer blank, which is
    arguably worse.
    """
    d = _episode_defaults()
    rows = []
    for i, payload in enumerate(["[]", "{not json}", survey(PDC=[
        {"PDCSubstanceOrGambling": "Heroin", "PDCDaysInLast28": "5",
         "PDCUnits": "caps", "PDCHowMuchPerOccasion": "2"}])]):
        rows.append({
            **d,
            "ESTABLISHMENT IDENTIFIER": "12QQ03062",
            "PMSEpisodeID": f"3000{i}",
            "PMSPersonID": "1",
            "PDCCode": "1201",
            "SLK": f"SLK{i}",
            "RowKey": f"r{i}",
            "AssessmentDate": "2025-02-0%d" % (i + 1),
            "SurveyData": payload,
        })
    matched = pd.DataFrame(rows).astype(str)
    matched["Stage"] = nada_df_generator.get_stage_per_episode(matched)

    prepped, _ = prep_nada_fields(matched, config)
    # row 0 ("[]") is gone; the malformed-JSON row 1 survives
    assert prepped["RowKey"].tolist() == ["r1", "r2"]
    # and the surviving index is now gapped -> the export would then crash
    assert list(prepped.index) == [1, 2]


def test_characterise_non_string_surveydata_raises(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    ``clean_and_parse_json`` calls ``str.translate`` on the raw value and only
    catches ``JSONDecodeError``.  A NaN (which is what ``read_csv`` yields for
    an empty cell when dtype is not forced) raises ``AttributeError``.
    """
    d = _episode_defaults()
    matched = pd.DataFrame([{
        **d,
        "ESTABLISHMENT IDENTIFIER": "12QQ03062",
        "PMSEpisodeID": "40001",
        "PMSPersonID": "1",
        "PDCCode": "1201",
        "SLK": "SLK0",
        "RowKey": "r0",
        "AssessmentDate": "2025-02-01",
        "SurveyData": float("nan"),
        "Stage": 0,
    }])
    with pytest.raises(AttributeError):
        prep_nada_fields(matched, config)


def test_characterise_bad_assessmentdate_format_raises(config):
    """CHARACTERISATION of a known defect -- not an endorsement.

    ``date_str_format`` is hard-wired to ``'%Y-%m-%d'`` in and ``'%d%m%Y'``
    out, with no error handling.  A ``dd/mm/yyyy`` AssessmentDate -- which the
    date FILTER upstream would happily accept, since it parses with
    ``dayfirst=True`` -- kills the export.
    """
    d = _episode_defaults()
    matched = pd.DataFrame([{
        **d,
        "ESTABLISHMENT IDENTIFIER": "12QQ03062",
        "PMSEpisodeID": "50001",
        "PMSPersonID": "1",
        "PDCCode": "1201",
        "SLK": "SLK0",
        "RowKey": "r0",
        "AssessmentDate": "01/02/2025",
        "SurveyData": survey(PDC=[
            {"PDCSubstanceOrGambling": "Heroin", "PDCDaysInLast28": "5",
             "PDCUnits": "caps", "PDCHowMuchPerOccasion": "2"}]),
    }]).astype(str)
    matched["Stage"] = nada_df_generator.get_stage_per_episode(matched)
    prepped, _ = prep_nada_fields(matched, config)
    with pytest.raises(ValueError):
        nada_df_generator.generate_finaloutput_df(prepped)


def test_characterise_drug_buckets_with_no_output_column(config):
    """CHARACTERISATION of a known gap -- not an endorsement.

    ``configuration.json`` defines the buckets Nicotine and Gambling, but
    ``nada_final_fields`` has no ``Gambling_*`` column at all and asks instead
    for ``Tobacco_Cigarettes_*`` and ``Nicotine_Vaping_*``, which no bucket can
    ever produce.  Tobacco/vaping ATOP columns are therefore permanently blank
    and gambling answers are dropped on the floor.
    """
    buckets = set(config["drug_categories"])
    assert "Gambling" in buckets and "Nicotine" in buckets
    assert not any(f.startswith("Gambling") for f in nada_final_fields)
    assert "Tobacco_Cigarettes_DaysInLast28" in nada_final_fields
    assert "Nicotine_Vaping_DaysInLast28" in nada_final_fields
    assert "Tobacco_Cigarettes" not in buckets
    assert "Nicotine_Vaping" not in buckets

    df = pd.DataFrame({"SLK": ["S1"], "RowKey": ["r1"]})
    df["PDC"] = None
    df["ODC"] = None
    df._set_value(0, "PDC", [{"PDCSubstanceOrGambling": "Nicotine",
                              "PDCDaysInLast28": "28", "PDCUnits": "cigs",
                              "PDCHowMuchPerOccasion": "10"}])
    out, _ = expand_drug_info(df, config)
    # lands in Nicotine_*, which the ATOP block of the export never reads
    assert out.loc[0, "Nicotine_DaysInLast28"] == "28"
    assert "Tobacco_Cigarettes_DaysInLast28" not in out.columns
