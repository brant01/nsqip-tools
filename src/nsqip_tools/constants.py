"""Constants and configuration for NSQIP Tools.

This module contains variable type definitions, column names, and other
constants used throughout the package.
"""
from typing import Set, Dict, List

# Variables that should always be treated as strings (not numeric)
# These are non-code fields that look numeric but are actually categorical
STRING_VARIABLES: Set[str] = {
    # Identifiers
    "CASEID",
    
    # Year/Date fields (categorical, not continuous)
    "OPERYR",
    "ADMYR", 
    "ADMSYR",
    "ADMQTR",
    "PUFYEAR",
    "YRDEATH",
    "HDISDT",
    
    # Training year (categorical levels)
    "PGY",
}

# CPT code columns - these are procedure codes that must remain strings
CPT_COLUMNS: List[str] = [
    "CPT",
    *[f"CONCPT{i}" for i in range(1, 11)],  # CONCPT1 through CONCPT10
    *[f"OTHERCPT{i}" for i in range(1, 11)],  # OTHERCPT1 through OTHERCPT10
]

# Diagnosis code columns - these are ICD codes that must remain strings
DIAGNOSIS_COLUMNS: List[str] = [
    "PODIAG",  # Post-operative diagnosis
    "PODIAG10",  # ICD-10 post-op diagnosis
    "READMUNRELICD93",  # ICD-9 readmission diagnosis
    "READMUNRELICD94",
    "READMUNRELICD95",
    "READMUNRELICD101",  # ICD-10 readmission diagnosis
    "READMUNRELICD102",
    "READMUNRELICD103",
    "READMUNRELICD104",
    "READMUNRELICD105",
]

# All columns that should never be converted to numeric
# This is the union of all the above categories
NEVER_NUMERIC: Set[str] = (
    STRING_VARIABLES | 
    set(CPT_COLUMNS) | 
    set(DIAGNOSIS_COLUMNS)
)

# Columns that contain comma-separated values
COMMA_SEPARATED_COLUMNS: Set[str] = {
    "ANESTHES_OTHER",
    "IMMUNO_CAT",
    "OP_APPROACH",
}

# Special handling for age field
AGE_FIELD: str = "AGE"
AGE_NINETY_PLUS: str = "90+"
AGE_AS_INT_FIELD: str = "AGE_AS_INT"
AGE_IS_90_PLUS_FIELD: str = "AGE_IS_90_PLUS"

# Special handling for race fields
RACE_FIELD: str = "RACE"
RACE_NEW_FIELD: str = "RACE_NEW"
RACE_COMBINED_FIELD: str = "RACE_COMBINED"

# Dataset naming convention
DATASET_NAME_TEMPLATE: str = "{dataset_type}_nsqip_parquet"

# Supported dataset types
DATASET_TYPES: Set[str] = {"adult", "pediatric"}

# Array column names created during transformation
ALL_CPT_CODES_FIELD: str = "ALL_CPT_CODES"
ALL_DIAGNOSIS_CODES_FIELD: str = "ALL_DIAGNOSIS_CODES"

# Expected case counts by year for data verification
EXPECTED_CASE_COUNTS: Dict[str, Dict[str, int]] = {
    "adult": {
        "2005": 152490,  # Combined 2005/2006
        "2006": 152490,  # Combined 2005/2006
        "2007": 211407,
        "2008": 271368,
        "2009": 336190,
        "2010": 363431,
        "2011": 442149,
        "2012": 543885,
        "2013": 651940,
        "2014": 750397,
        "2015": 885502,
        "2016": 1000393,
        "2017": 1028713,
        "2018": 1020511,
        "2019": 1076441,
        "2020": 902968,
        "2021": 983851,
    },
    "pediatric": {
        "2012": 51008,
        "2013": 63387,
        "2014": 68838,
        "2015": 84056,
        "2016": 101887,
        "2017": 113922,
        "2018": 119486,
        "2019": 132881,
        "2020": 128395,
        "2021": 141843,
        "2022": 146331,
    }
}