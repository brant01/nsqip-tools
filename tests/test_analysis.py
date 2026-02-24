"""
Unit tests for nsqip_tools.analysis module.

Covers detect_dataset_type, calculate_composite_ssi, calculate_serious_morbidity,
filter_by_age, create_age_groups, clean_asa_class, calculate_bmi, standardize_sex,
get_surgery_year, filter_elective_cases, create_outcome_summary, and export_for_stats.
"""

from pathlib import Path

import polars as pl
import pytest

from nsqip_tools.analysis import (
    calculate_bmi,
    calculate_composite_ssi,
    calculate_serious_morbidity,
    clean_asa_class,
    create_age_groups,
    create_outcome_summary,
    detect_dataset_type,
    export_for_stats,
    filter_by_age,
    filter_elective_cases,
    get_surgery_year,
    standardize_sex,
)


class TestDetectDatasetType:
    """Tests for detect_dataset_type function."""

    def test_detects_adult_from_age_as_int(self, adult_nsqip_df: pl.DataFrame):
        result = detect_dataset_type(adult_nsqip_df)
        assert result == "adult"

    def test_detects_adult_from_age_column(self):
        df = pl.DataFrame({"AGE": ["45", "67", "90+"]})
        result = detect_dataset_type(df)
        assert result == "adult"

    def test_detects_pediatric_from_age_days(self, pediatric_nsqip_df: pl.DataFrame):
        result = detect_dataset_type(pediatric_nsqip_df)
        assert result == "pediatric"

    def test_age_days_takes_precedence_over_age(self):
        # AGE_DAYS should win when both columns present
        df = pl.DataFrame({"AGE_DAYS": [14, 180], "AGE": ["0", "0"]})
        result = detect_dataset_type(df)
        assert result == "pediatric"

    def test_respects_explicit_adult_type(self, adult_nsqip_df: pl.DataFrame):
        result = detect_dataset_type(adult_nsqip_df, dataset_type="adult")
        assert result == "adult"

    def test_respects_explicit_pediatric_type(self, adult_nsqip_df: pl.DataFrame):
        # Overrides auto-detection even when adult columns are present
        result = detect_dataset_type(adult_nsqip_df, dataset_type="pediatric")
        assert result == "pediatric"

    def test_raises_on_missing_age_columns(self):
        df = pl.DataFrame({"OTHER_COL": [1, 2, 3]})
        with pytest.raises(ValueError, match="Cannot determine dataset type"):
            detect_dataset_type(df)

    def test_works_with_lazyframe(self, adult_lazyframe: pl.LazyFrame):
        result = detect_dataset_type(adult_lazyframe)
        assert result == "adult"

    def test_works_with_pediatric_lazyframe(self, pediatric_lazyframe: pl.LazyFrame):
        result = detect_dataset_type(pediatric_lazyframe)
        assert result == "pediatric"


class TestCalculateCompositeSSI:
    """Tests for calculate_composite_ssi function."""

    def test_creates_any_ssi_column(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_composite_ssi(adult_nsqip_df)
        assert "ANY_SSI" in result.columns

    def test_detects_superficial_ssi(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_composite_ssi(adult_nsqip_df)
        # Row 1 has superficial SSI
        assert result["ANY_SSI"][1] == 1

    def test_detects_deep_ssi(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_composite_ssi(adult_nsqip_df)
        # Row 2 has deep SSI
        assert result["ANY_SSI"][2] == 1

    def test_detects_organ_space_ssi(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_composite_ssi(adult_nsqip_df)
        # Row 3 has organ/space SSI
        assert result["ANY_SSI"][3] == 1

    def test_no_ssi_is_zero(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_composite_ssi(adult_nsqip_df)
        # Row 0 and row 4 have no SSI
        assert result["ANY_SSI"][0] == 0
        assert result["ANY_SSI"][4] == 0

    def test_any_ssi_is_integer_type(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_composite_ssi(adult_nsqip_df)
        assert result["ANY_SSI"].dtype in (pl.Int8, pl.Int32, pl.Int64)

    def test_works_with_partial_ssi_columns(self):
        # Only SUPINFEC present - should still work
        df = pl.DataFrame({
            "AGE_AS_INT": [50, 60],
            "SUPINFEC": ["Superficial Incisional SSI", "No Complication"],
        })
        result = calculate_composite_ssi(df)
        assert "ANY_SSI" in result.columns
        assert result["ANY_SSI"][0] == 1
        assert result["ANY_SSI"][1] == 0

    def test_raises_on_missing_ssi_columns(self):
        df = pl.DataFrame({"OTHER_COL": [1, 2, 3]})
        with pytest.raises(ValueError, match="No SSI columns found"):
            calculate_composite_ssi(df)

    def test_works_with_lazyframe(self, adult_lazyframe: pl.LazyFrame):
        result = calculate_composite_ssi(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert "ANY_SSI" in collected.columns

    def test_lazyframe_preserves_ssi_values(self, adult_lazyframe: pl.LazyFrame):
        collected = calculate_composite_ssi(adult_lazyframe).collect()
        assert collected["ANY_SSI"][0] == 0
        assert collected["ANY_SSI"][1] == 1

    def test_works_with_pediatric_dataframe(self, pediatric_nsqip_df: pl.DataFrame):
        result = calculate_composite_ssi(pediatric_nsqip_df)
        assert "ANY_SSI" in result.columns
        # Row 2 has superficial SSI in pediatric fixture
        assert result["ANY_SSI"][2] == 1


class TestCalculateSeriousMorbidity:
    """Tests for calculate_serious_morbidity function."""

    def test_creates_serious_morbidity_column(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(adult_nsqip_df)
        assert "SERIOUS_MORBIDITY" in result.columns

    def test_serious_morbidity_is_integer_type(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(adult_nsqip_df)
        assert result["SERIOUS_MORBIDITY"].dtype in (pl.Int8, pl.Int32, pl.Int64)

    def test_detects_deep_ssi_as_serious(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(adult_nsqip_df)
        # Row 2 has deep SSI
        assert result["SERIOUS_MORBIDITY"][2] == 1

    def test_detects_organ_space_ssi_as_serious(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(adult_nsqip_df)
        # Row 3 has organ/space SSI
        assert result["SERIOUS_MORBIDITY"][3] == 1

    def test_detects_reoperation_as_serious_adult(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(adult_nsqip_df)
        # Row 2 has REOPERATION1 = "Yes"
        assert result["SERIOUS_MORBIDITY"][2] == 1

    def test_detects_readmission_as_serious_adult(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(adult_nsqip_df)
        # Row 1 has READMISSION1 = "Yes"
        assert result["SERIOUS_MORBIDITY"][1] == 1

    def test_no_morbidity_is_zero(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(adult_nsqip_df)
        # Row 0 has no complications
        assert result["SERIOUS_MORBIDITY"][0] == 0

    def test_uses_reoperation_column_for_pediatric(
        self, pediatric_nsqip_df: pl.DataFrame
    ):
        result = calculate_serious_morbidity(pediatric_nsqip_df)
        assert "SERIOUS_MORBIDITY" in result.columns
        # Row 4 has REOPERATION = "Yes"
        assert result["SERIOUS_MORBIDITY"][4] == 1

    def test_uses_readmission1_for_pediatric(self, pediatric_nsqip_df: pl.DataFrame):
        result = calculate_serious_morbidity(pediatric_nsqip_df)
        # Row 3 has READMISSION1 = "Yes"
        assert result["SERIOUS_MORBIDITY"][3] == 1

    def test_raises_on_no_complication_columns(self):
        df = pl.DataFrame({"AGE_AS_INT": [45, 67], "SEX": ["Male", "Female"]})
        with pytest.raises(ValueError, match="No complication columns found"):
            calculate_serious_morbidity(df)

    def test_works_with_lazyframe(self, adult_lazyframe: pl.LazyFrame):
        result = calculate_serious_morbidity(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert "SERIOUS_MORBIDITY" in collected.columns

    def test_explicit_dataset_type_overrides_detection(
        self, pediatric_nsqip_df: pl.DataFrame
    ):
        # Force adult logic on pediatric fixture - should look for REOPERATION1 (absent)
        # but still succeed because other columns are present
        result = calculate_serious_morbidity(pediatric_nsqip_df, dataset_type="adult")
        assert "SERIOUS_MORBIDITY" in result.columns


class TestFilterByAge:
    """Tests for filter_by_age function."""

    def test_min_age_filter_adult(self, adult_nsqip_df: pl.DataFrame):
        result = filter_by_age(adult_nsqip_df, min_age=60)
        assert len(result) == 2  # 67 and 78 year olds

    def test_max_age_filter_adult(self, adult_nsqip_df: pl.DataFrame):
        result = filter_by_age(adult_nsqip_df, max_age=50)
        assert len(result) == 2  # 45 and 32 year olds

    def test_age_range_filter_adult(self, adult_nsqip_df: pl.DataFrame):
        result = filter_by_age(adult_nsqip_df, min_age=40, max_age=60)
        assert len(result) == 2  # 45 and 55 year olds

    def test_no_filter_returns_all_rows(self, adult_nsqip_df: pl.DataFrame):
        result = filter_by_age(adult_nsqip_df)
        assert len(result) == len(adult_nsqip_df)

    def test_min_age_boundary_inclusive(self, adult_nsqip_df: pl.DataFrame):
        # 45 should be included with min_age=45
        result = filter_by_age(adult_nsqip_df, min_age=45)
        ages = result["AGE_AS_INT"].to_list()
        assert 45 in ages

    def test_max_age_boundary_inclusive(self, adult_nsqip_df: pl.DataFrame):
        # 67 should be included with max_age=67
        result = filter_by_age(adult_nsqip_df, max_age=67)
        ages = result["AGE_AS_INT"].to_list()
        assert 67 in ages

    def test_pediatric_age_filter_by_years(self, pediatric_nsqip_df: pl.DataFrame):
        # Filter to children 2 years and older (730 days = ~1.999 years, excluded)
        result = filter_by_age(pediatric_nsqip_df, min_age=2)
        assert len(result) == 2  # 7yr (2555 days), 15yr (5475 days)

    def test_pediatric_age_filter_infants(self, pediatric_nsqip_df: pl.DataFrame):
        # Filter to under 1 year
        result = filter_by_age(pediatric_nsqip_df, max_age=1)
        assert len(result) == 2  # neonate (14 days) and infant (180 days)

    def test_pediatric_converts_years_to_days(self, pediatric_nsqip_df: pl.DataFrame):
        # min_age in years should be converted to days for AGE_DAYS comparison
        result = filter_by_age(pediatric_nsqip_df, min_age=10)
        for row_age_days in result["AGE_DAYS"].to_list():
            assert row_age_days >= 10 * 365.25

    def test_returns_lazyframe_when_given_lazyframe(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = filter_by_age(adult_lazyframe, min_age=50)
        assert isinstance(result, pl.LazyFrame)

    def test_lazyframe_filter_correct_count(self, adult_lazyframe: pl.LazyFrame):
        result = filter_by_age(adult_lazyframe, min_age=60).collect()
        assert len(result) == 2  # 67 and 78 year olds

    def test_explicit_dataset_type_forces_adult_logic(
        self, adult_nsqip_df: pl.DataFrame
    ):
        result = filter_by_age(adult_nsqip_df, min_age=60, dataset_type="adult")
        assert len(result) == 2


class TestCreateAgeGroups:
    """Tests for create_age_groups function."""

    def test_creates_age_group_column_adult(self, adult_nsqip_df: pl.DataFrame):
        result = create_age_groups(adult_nsqip_df)
        assert "AGE_GROUP" in result.columns

    def test_default_adult_bins_assign_correct_groups(
        self, adult_nsqip_df: pl.DataFrame
    ):
        result = create_age_groups(adult_nsqip_df)
        groups = result["AGE_GROUP"].to_list()
        # Ages: 45, 67, 32, 78, 55 -> 40-64, 65-79, 18-39, 65-79, 40-64
        assert groups[0] == "40-64"  # age 45
        assert groups[1] == "65-79"  # age 67
        assert groups[2] == "18-39"  # age 32
        assert groups[4] == "40-64"  # age 55

    def test_adult_age_78_in_65_79_group(self, adult_nsqip_df: pl.DataFrame):
        result = create_age_groups(adult_nsqip_df)
        assert result["AGE_GROUP"][3] == "65-79"

    def test_custom_bins_adult(self, adult_nsqip_df: pl.DataFrame):
        custom_bins = [0, 50, 70, 150]
        result = create_age_groups(adult_nsqip_df, custom_bins=custom_bins)
        assert "AGE_GROUP" in result.columns
        groups = result["AGE_GROUP"].to_list()
        assert groups[0] == "0-49"
        assert groups[1] == "50-69"
        assert groups[3] == "70+"

    def test_creates_age_group_column_pediatric(
        self, pediatric_nsqip_df: pl.DataFrame
    ):
        result = create_age_groups(pediatric_nsqip_df)
        assert "AGE_GROUP" in result.columns

    def test_default_pediatric_bins_assign_correct_groups(
        self, pediatric_nsqip_df: pl.DataFrame
    ):
        result = create_age_groups(pediatric_nsqip_df)
        groups = result["AGE_GROUP"].to_list()
        # AGE_DAYS: 14 (neonate), 180 (infant), 730 (~2yr), 2555 (~7yr), 5475 (~15yr)
        assert groups[0] == "1-30d"   # 14 days
        assert groups[1] == "1mo-1y"  # 180 days
        assert groups[3] == "5-12y"   # 2555 days (~7yr)
        assert groups[4] == "12-18y"  # 5475 days (~15yr)

    def test_returns_same_type_as_input_dataframe(self, adult_nsqip_df: pl.DataFrame):
        result = create_age_groups(adult_nsqip_df)
        assert isinstance(result, pl.DataFrame)

    def test_returns_lazyframe_when_given_lazyframe(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = create_age_groups(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert "AGE_GROUP" in collected.columns

    def test_lazyframe_age_groups_correct(self, adult_lazyframe: pl.LazyFrame):
        collected = create_age_groups(adult_lazyframe).collect()
        assert "AGE_GROUP" in collected.columns
        assert collected["AGE_GROUP"][0] == "40-64"  # age 45

    def test_explicit_dataset_type_pediatric(self, pediatric_nsqip_df: pl.DataFrame):
        result = create_age_groups(pediatric_nsqip_df, dataset_type="pediatric")
        assert "AGE_GROUP" in result.columns


class TestCleanAsaClass:
    """Tests for clean_asa_class function."""

    def test_creates_asa_simple_column(self, adult_nsqip_df: pl.DataFrame):
        result = clean_asa_class(adult_nsqip_df)
        assert "ASA_SIMPLE" in result.columns

    def test_extracts_asa_number_class_2(self, adult_nsqip_df: pl.DataFrame):
        result = clean_asa_class(adult_nsqip_df)
        assert result["ASA_SIMPLE"][0] == "2"

    def test_extracts_asa_number_class_3(self, adult_nsqip_df: pl.DataFrame):
        result = clean_asa_class(adult_nsqip_df)
        assert result["ASA_SIMPLE"][1] == "3"

    def test_extracts_asa_number_class_1(self, adult_nsqip_df: pl.DataFrame):
        result = clean_asa_class(adult_nsqip_df)
        assert result["ASA_SIMPLE"][2] == "1"

    def test_extracts_asa_number_class_4(self, adult_nsqip_df: pl.DataFrame):
        result = clean_asa_class(adult_nsqip_df)
        assert result["ASA_SIMPLE"][3] == "4"

    def test_raises_on_missing_asaclas_column(self):
        df = pl.DataFrame({"OTHER_COL": [1, 2, 3]})
        with pytest.raises(ValueError, match="ASACLAS column not found"):
            clean_asa_class(df)

    def test_works_with_pediatric_dataframe(self, pediatric_nsqip_df: pl.DataFrame):
        result = clean_asa_class(pediatric_nsqip_df)
        assert "ASA_SIMPLE" in result.columns
        assert result["ASA_SIMPLE"][0] == "2"
        assert result["ASA_SIMPLE"][1] == "1"

    def test_returns_lazyframe_when_given_lazyframe(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = clean_asa_class(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)

    def test_lazyframe_extracts_asa_correctly(self, adult_lazyframe: pl.LazyFrame):
        collected = clean_asa_class(adult_lazyframe).collect()
        assert collected["ASA_SIMPLE"][0] == "2"


class TestCalculateBMI:
    """Tests for calculate_bmi function."""

    def test_creates_bmi_column(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_bmi(adult_nsqip_df)
        assert "BMI" in result.columns

    def test_calculates_bmi_correctly(self, adult_nsqip_df: pl.DataFrame):
        result = calculate_bmi(adult_nsqip_df)
        # Row 0: 180 lbs, 70 inches
        # BMI = (180 * 0.453592) / (70 * 0.0254)^2 ≈ 25.8
        assert 25.0 < result["BMI"][0] < 27.0

    def test_skips_calculation_if_bmi_already_exists(
        self, adult_nsqip_df: pl.DataFrame
    ):
        df_with_bmi = adult_nsqip_df.with_columns(pl.lit(99.0).alias("BMI"))
        result = calculate_bmi(df_with_bmi)
        assert result["BMI"][0] == 99.0

    def test_returns_unchanged_dataframe_if_height_missing(self):
        df = pl.DataFrame({"WEIGHT": [150, 180], "AGE_AS_INT": [45, 67]})
        result = calculate_bmi(df)
        assert "BMI" not in result.columns

    def test_returns_unchanged_dataframe_if_weight_missing(self):
        df = pl.DataFrame({"HEIGHT": [70, 65], "AGE_AS_INT": [45, 67]})
        result = calculate_bmi(df)
        assert "BMI" not in result.columns

    def test_returns_unchanged_if_no_height_or_weight(self):
        df = pl.DataFrame({"OTHER_COL": [1, 2, 3]})
        result = calculate_bmi(df)
        assert "BMI" not in result.columns

    def test_returns_lazyframe_when_given_lazyframe(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = calculate_bmi(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)

    def test_lazyframe_calculates_bmi(self, adult_lazyframe: pl.LazyFrame):
        collected = calculate_bmi(adult_lazyframe).collect()
        assert "BMI" in collected.columns
        assert 25.0 < collected["BMI"][0] < 27.0


class TestStandardizeSex:
    """Tests for standardize_sex function."""

    def test_creates_sex_standard_column(self, adult_nsqip_df: pl.DataFrame):
        result = standardize_sex(adult_nsqip_df)
        assert "SEX_STANDARD" in result.columns

    def test_male_full_word_to_m(self, adult_nsqip_df: pl.DataFrame):
        result = standardize_sex(adult_nsqip_df)
        assert result["SEX_STANDARD"][0] == "M"  # "Male"

    def test_female_full_word_to_f(self, adult_nsqip_df: pl.DataFrame):
        result = standardize_sex(adult_nsqip_df)
        assert result["SEX_STANDARD"][1] == "F"  # "Female"

    def test_lowercase_male_to_m(self, adult_nsqip_df: pl.DataFrame):
        result = standardize_sex(adult_nsqip_df)
        assert result["SEX_STANDARD"][2] == "M"  # "male"

    def test_uppercase_female_to_f(self, adult_nsqip_df: pl.DataFrame):
        result = standardize_sex(adult_nsqip_df)
        assert result["SEX_STANDARD"][3] == "F"  # "FEMALE"

    def test_single_letter_m_preserved(self, adult_nsqip_df: pl.DataFrame):
        result = standardize_sex(adult_nsqip_df)
        assert result["SEX_STANDARD"][4] == "M"  # "M"

    def test_only_m_and_f_values_present(self, adult_nsqip_df: pl.DataFrame):
        result = standardize_sex(adult_nsqip_df)
        unique_values = set(result["SEX_STANDARD"].to_list())
        assert unique_values == {"M", "F"}

    def test_returns_unchanged_if_no_sex_column(self):
        df = pl.DataFrame({"AGE_AS_INT": [45, 67]})
        result = standardize_sex(df)
        assert "SEX_STANDARD" not in result.columns

    def test_returns_lazyframe_when_given_lazyframe(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = standardize_sex(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)

    def test_lazyframe_standardizes_correctly(self, adult_lazyframe: pl.LazyFrame):
        collected = standardize_sex(adult_lazyframe).collect()
        assert collected["SEX_STANDARD"][0] == "M"
        assert collected["SEX_STANDARD"][1] == "F"


class TestGetSurgeryYear:
    """Tests for get_surgery_year function."""

    def test_creates_surgery_year_column(self, adult_nsqip_df: pl.DataFrame):
        result = get_surgery_year(adult_nsqip_df)
        assert "SURGERY_YEAR" in result.columns

    def test_surgery_year_is_int32(self, adult_nsqip_df: pl.DataFrame):
        result = get_surgery_year(adult_nsqip_df)
        assert result["SURGERY_YEAR"].dtype == pl.Int32

    def test_extracts_correct_year_from_operyr(self, adult_nsqip_df: pl.DataFrame):
        result = get_surgery_year(adult_nsqip_df)
        assert result["SURGERY_YEAR"][0] == 2022
        assert result["SURGERY_YEAR"][2] == 2023

    def test_uses_admyr_when_operyr_absent(self):
        df = pl.DataFrame({
            "AGE_AS_INT": [45, 67],
            "ADMYR": [2021, 2022],
        })
        result = get_surgery_year(df)
        assert "SURGERY_YEAR" in result.columns
        assert result["SURGERY_YEAR"][0] == 2021

    def test_returns_unchanged_when_neither_year_column_present(self):
        df = pl.DataFrame({"AGE_AS_INT": [45, 67]})
        result = get_surgery_year(df)
        assert "SURGERY_YEAR" not in result.columns

    def test_works_with_string_operyr(self):
        df = pl.DataFrame({
            "AGE_AS_INT": [45, 67],
            "OPERYR": ["2020", "2021"],
        })
        result = get_surgery_year(df)
        assert result["SURGERY_YEAR"].dtype == pl.Int32
        assert result["SURGERY_YEAR"][0] == 2020

    def test_returns_lazyframe_when_given_lazyframe(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = get_surgery_year(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)

    def test_lazyframe_extracts_year_correctly(self, adult_lazyframe: pl.LazyFrame):
        collected = get_surgery_year(adult_lazyframe).collect()
        assert "SURGERY_YEAR" in collected.columns
        assert collected["SURGERY_YEAR"][0] == 2022


class TestFilterElectiveCases:
    """Tests for filter_elective_cases function."""

    def test_filters_to_elective_only_adult(self, adult_nsqip_df: pl.DataFrame):
        result = filter_elective_cases(adult_nsqip_df)
        # Original has 1 emergency (row 3 with EMERGENT="Yes"), so 4 elective remain
        assert len(result) == 4

    def test_filters_to_elective_only_pediatric(
        self, pediatric_nsqip_df: pl.DataFrame
    ):
        result = filter_elective_cases(pediatric_nsqip_df)
        # Pediatric fixture has 1 emergency (row 0), so 4 elective remain
        assert len(result) == 4

    def test_no_emergency_rows_in_result(self, adult_nsqip_df: pl.DataFrame):
        result = filter_elective_cases(adult_nsqip_df)
        assert "Yes" not in result["EMERGENT"].to_list()

    def test_all_result_rows_are_elective(self, adult_nsqip_df: pl.DataFrame):
        result = filter_elective_cases(adult_nsqip_df)
        assert all(v == "No" for v in result["EMERGENT"].to_list())

    def test_raises_on_missing_emergent_column(self):
        df = pl.DataFrame({"OTHER_COL": [1, 2, 3]})
        with pytest.raises(ValueError, match="EMERGENT column not found"):
            filter_elective_cases(df)

    def test_returns_lazyframe_when_given_lazyframe(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = filter_elective_cases(adult_lazyframe)
        assert isinstance(result, pl.LazyFrame)

    def test_lazyframe_correct_count_after_filter(
        self, adult_lazyframe: pl.LazyFrame
    ):
        result = filter_elective_cases(adult_lazyframe).collect()
        assert len(result) == 4


class TestCreateOutcomeSummary:
    """Tests for create_outcome_summary function."""

    def test_creates_summary_table(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        assert isinstance(result, pl.DataFrame)

    def test_summary_has_outcome_column(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        assert "Outcome" in result.columns

    def test_summary_has_n_column(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        assert "N" in result.columns

    def test_summary_has_total_column(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        assert "Total" in result.columns

    def test_summary_has_rate_column(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        assert "Rate (%)" in result.columns

    def test_ungrouped_total_equals_row_count(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        # All rows should have the same Total (5 rows in fixture)
        assert all(t == 5 for t in result["Total"].to_list())

    def test_ssi_rate_calculated_correctly(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        # Superficial SSI: 1 out of 5 = 20%
        superficial_row = result.filter(pl.col("Outcome") == "Superficial SSI")
        assert superficial_row["Rate (%)"][0] == 20.0

    def test_grouped_summary_includes_group_column(
        self, adult_nsqip_df: pl.DataFrame
    ):
        result = create_outcome_summary(adult_nsqip_df, group_var="OPERYR")
        assert "OPERYR" in result.columns

    def test_grouped_summary_has_correct_groups(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df, group_var="OPERYR")
        years_in_summary = result["OPERYR"].unique().to_list()
        assert 2022 in years_in_summary
        assert 2023 in years_in_summary

    def test_pediatric_summary_uses_reoperation_column(
        self, pediatric_nsqip_df: pl.DataFrame
    ):
        result = create_outcome_summary(pediatric_nsqip_df)
        outcomes = result["Outcome"].to_list()
        assert "Reoperation" in outcomes

    def test_adult_summary_includes_readmission(self, adult_nsqip_df: pl.DataFrame):
        result = create_outcome_summary(adult_nsqip_df)
        outcomes = result["Outcome"].to_list()
        assert "Readmission" in outcomes

    def test_raises_on_lazyframe_input(self, adult_lazyframe: pl.LazyFrame):
        with pytest.raises(TypeError, match="requires a DataFrame"):
            create_outcome_summary(adult_lazyframe)

    def test_ignores_unknown_group_var(self, adult_nsqip_df: pl.DataFrame):
        # Non-existent group_var should fall back to ungrouped summary
        result = create_outcome_summary(adult_nsqip_df, group_var="NONEXISTENT_COL")
        assert "NONEXISTENT_COL" not in result.columns
        assert "Outcome" in result.columns


class TestExportForStats:
    """Tests for export_for_stats function."""

    def test_exports_csv_file(self, adult_nsqip_df: pl.DataFrame, tmp_path: Path):
        output_path = tmp_path / "output.csv"
        export_for_stats(adult_nsqip_df, output_path, format="csv")
        assert output_path.exists()

    def test_exports_parquet_file(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.parquet"
        export_for_stats(adult_nsqip_df, output_path, format="parquet")
        assert output_path.exists()

    def test_csv_is_readable(self, adult_nsqip_df: pl.DataFrame, tmp_path: Path):
        output_path = tmp_path / "output.csv"
        export_for_stats(adult_nsqip_df, output_path, format="csv")
        loaded = pl.read_csv(output_path)
        assert len(loaded) == len(adult_nsqip_df)

    def test_parquet_is_readable(self, adult_nsqip_df: pl.DataFrame, tmp_path: Path):
        output_path = tmp_path / "output.parquet"
        export_for_stats(adult_nsqip_df, output_path, format="parquet")
        loaded = pl.read_parquet(output_path)
        assert len(loaded) == len(adult_nsqip_df)

    def test_adds_binary_supinfec_column(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.parquet"
        export_for_stats(adult_nsqip_df, output_path, format="parquet")
        loaded = pl.read_parquet(output_path)
        assert "SUPINFEC_BINARY" in loaded.columns

    def test_adds_binary_wndinfd_column(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.parquet"
        export_for_stats(adult_nsqip_df, output_path, format="parquet")
        loaded = pl.read_parquet(output_path)
        assert "WNDINFD_BINARY" in loaded.columns

    def test_binary_supinfec_values_correct(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.parquet"
        export_for_stats(adult_nsqip_df, output_path, format="parquet")
        loaded = pl.read_parquet(output_path)
        # Row 1 has superficial SSI, all others do not
        assert loaded["SUPINFEC_BINARY"][0] == 0
        assert loaded["SUPINFEC_BINARY"][1] == 1

    def test_binary_wndinfd_values_correct(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.parquet"
        export_for_stats(adult_nsqip_df, output_path, format="parquet")
        loaded = pl.read_parquet(output_path)
        # Row 2 has deep SSI
        assert loaded["WNDINFD_BINARY"][1] == 0
        assert loaded["WNDINFD_BINARY"][2] == 1

    def test_include_vars_subsets_columns(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.csv"
        include_vars = ["AGE_AS_INT", "SEX", "SUPINFEC"]
        export_for_stats(
            adult_nsqip_df, output_path, format="csv", include_vars=include_vars
        )
        loaded = pl.read_csv(output_path)
        # Should have selected columns plus SUPINFEC_BINARY added automatically
        assert "AGE_AS_INT" in loaded.columns
        assert "SEX" in loaded.columns
        assert "SUPINFEC" in loaded.columns

    def test_include_vars_excludes_other_columns(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.csv"
        include_vars = ["AGE_AS_INT", "SEX"]
        export_for_stats(
            adult_nsqip_df, output_path, format="csv", include_vars=include_vars
        )
        loaded = pl.read_csv(output_path)
        assert "HEIGHT" not in loaded.columns
        assert "WEIGHT" not in loaded.columns

    def test_include_vars_ignores_missing_columns(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.csv"
        include_vars = ["AGE_AS_INT", "NONEXISTENT_COLUMN"]
        # Should not raise even when a requested column doesn't exist
        export_for_stats(
            adult_nsqip_df, output_path, format="csv", include_vars=include_vars
        )
        loaded = pl.read_csv(output_path)
        assert "AGE_AS_INT" in loaded.columns
        assert "NONEXISTENT_COLUMN" not in loaded.columns

    def test_creates_parent_directories(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "nested" / "subdir" / "output.csv"
        export_for_stats(adult_nsqip_df, output_path, format="csv")
        assert output_path.exists()

    def test_accepts_string_path(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = str(tmp_path / "output.csv")
        export_for_stats(adult_nsqip_df, output_path, format="csv")
        assert Path(output_path).exists()

    def test_accepts_pathlib_path(
        self, adult_nsqip_df: pl.DataFrame, tmp_path: Path
    ):
        output_path = tmp_path / "output.csv"
        export_for_stats(adult_nsqip_df, output_path, format="csv")
        assert output_path.exists()
