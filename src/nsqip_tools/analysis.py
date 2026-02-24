"""Analysis functions for NSQIP data.

Common utilities for NSQIP data analysis using Polars.
These functions complement NSQIPQuery for frequent analysis patterns.

Supports both adult and pediatric NSQIP datasets.
Functions automatically detect dataset type and preserve DataFrame/LazyFrame types.
"""

import logging
from pathlib import Path
from typing import Literal, TypeVar

import polars as pl

logger = logging.getLogger(__name__)

# Type variable to preserve DataFrame or LazyFrame type
FrameType = TypeVar("FrameType", pl.DataFrame, pl.LazyFrame)


def _get_columns(df: pl.DataFrame | pl.LazyFrame) -> list[str]:
    """Get column names without triggering Polars PerformanceWarning on LazyFrames."""
    if isinstance(df, pl.LazyFrame):
        return df.collect_schema().names()
    return df.columns


def detect_dataset_type(
    df: pl.DataFrame | pl.LazyFrame,
    dataset_type: Literal["adult", "pediatric"] | None = None,
) -> Literal["adult", "pediatric"]:
    """Detect whether dataset is adult or pediatric NSQIP.

    Args:
        df: Polars DataFrame or LazyFrame
        dataset_type: If provided, uses this instead of auto-detection

    Returns:
        "adult" or "pediatric"

    Raises:
        ValueError: If dataset type cannot be determined
    """
    if dataset_type is not None:
        return dataset_type

    columns = _get_columns(df)

    if "AGE_DAYS" in columns:
        return "pediatric"
    elif "AGE_AS_INT" in columns or "AGE" in columns:
        return "adult"
    else:
        raise ValueError(
            "Cannot determine dataset type (adult vs pediatric). "
            "No age columns found. "
            "Please specify dataset_type='adult' or dataset_type='pediatric'"
        )


def calculate_composite_ssi(
    df: FrameType,
    dataset_type: Literal["adult", "pediatric"] | None = None,
) -> FrameType:
    """Calculate composite SSI (Surgical Site Infection) outcome.

    Creates ANY_SSI binary indicator from SSI complication text fields.

    Args:
        df: Polars DataFrame or LazyFrame with SSI variables
        dataset_type: Force "adult" or "pediatric" (auto-detects if None)

    Returns:
        Same type as input with added ANY_SSI column (0/1)

    Example:
        >>> df = calculate_composite_ssi(df)
        >>> ssi_rate = df.filter(pl.col("ANY_SSI") == 1).count()
    """
    ssi_vars = ["SUPINFEC", "WNDINFD", "ORGSPCSSI"]

    columns = _get_columns(df)
    ssi_cols = [col for col in ssi_vars if col in columns]

    if not ssi_cols:
        raise ValueError(f"No SSI columns found. Expected: {ssi_vars}")

    ssi_exprs = [(pl.col(col) != "No Complication").cast(pl.Int8) for col in ssi_cols]

    any_ssi_expr = pl.max_horizontal(ssi_exprs).fill_null(0).alias("ANY_SSI")

    return df.with_columns(any_ssi_expr)


def calculate_serious_morbidity(
    df: FrameType,
    dataset_type: Literal["adult", "pediatric"] | None = None,
) -> FrameType:
    """Calculate serious morbidity composite outcome.

    Creates binary indicator for any serious complication.

    Args:
        df: Polars DataFrame or LazyFrame
        dataset_type: Force "adult" or "pediatric" (auto-detects if None)

    Returns:
        Same type as input with added SERIOUS_MORBIDITY column (0/1)
    """
    common_complications = {
        "WNDINFD": "Deep Incisional SSI",
        "ORGSPCSSI": "Organ/Space SSI",
        "OUPNEUMO": "Pneumonia",
        "URNINFEC": "Urinary Tract Infection",
    }

    columns = _get_columns(df)
    comp_exprs = []

    for col, complication_text in common_complications.items():
        if col in columns:
            comp_exprs.append((pl.col(col) == complication_text).cast(pl.Int8))

    yes_no_vars = []

    ds_type = detect_dataset_type(df, dataset_type)

    if ds_type == "adult":
        yes_no_vars.extend(["REOPERATION1", "READMISSION1"])
        other_checks = ["CDARREST", "CDMI", "CNSCVA", "RENAINSF", "OPRENAFL"]
    else:
        yes_no_vars.extend(["REOPERATION", "READMISSION1"])
        other_checks = ["CDARREST", "STROKE", "SEIZURE", "RENALFAIL"]

    for var in yes_no_vars:
        if var in columns:
            comp_exprs.append((pl.col(var) == "Yes").cast(pl.Int8))

    for var in other_checks:
        if var in columns:
            comp_exprs.append((pl.col(var) == 1).cast(pl.Int8))

    if not comp_exprs:
        raise ValueError(f"No complication columns found in {ds_type} dataset")

    morbidity_expr = (
        pl.max_horizontal(comp_exprs).fill_null(0).alias("SERIOUS_MORBIDITY")
    )

    return df.with_columns(morbidity_expr)


def filter_by_age(
    df: FrameType,
    min_age: float | None = None,
    max_age: float | None = None,
    dataset_type: Literal["adult", "pediatric"] | None = None,
) -> FrameType:
    """Filter cases by age range (in years).

    Args:
        df: Polars DataFrame or LazyFrame
        min_age: Minimum age in years
        max_age: Maximum age in years
        dataset_type: Force "adult" or "pediatric" (auto-detects if None)

    Returns:
        Filtered DataFrame or LazyFrame

    Example:
        >>> elderly = filter_by_age(df, min_age=65)
        >>> infants = filter_by_age(df, max_age=1)
    """
    ds_type = detect_dataset_type(df, dataset_type)

    if ds_type == "adult":
        age_col = "AGE_AS_INT"
        if min_age is not None:
            df = df.filter(pl.col(age_col) >= min_age)
        if max_age is not None:
            df = df.filter(pl.col(age_col) <= max_age)
    else:
        if min_age is not None:
            df = df.filter(pl.col("AGE_DAYS") >= (min_age * 365.25))
        if max_age is not None:
            df = df.filter(pl.col("AGE_DAYS") <= (max_age * 365.25))

    return df


def create_age_groups(
    df: FrameType,
    custom_bins: list[float] | None = None,
    dataset_type: Literal["adult", "pediatric"] | None = None,
) -> FrameType:
    """Create age group categories for analysis.

    Args:
        df: Polars DataFrame or LazyFrame
        custom_bins: Custom age bins in years. If None, uses standard groupings.
        dataset_type: Force "adult" or "pediatric" (auto-detects if None)

    Returns:
        Same type as input with added AGE_GROUP column
    """
    ds_type = detect_dataset_type(df, dataset_type)

    if ds_type == "adult":
        if custom_bins is None:
            # Polars cut() takes inner breakpoints only
            breaks = [18, 40, 65, 80]
            labels = ["<18", "18-39", "40-64", "65-79", "80+"]
        else:
            # custom_bins are full boundaries; extract inner breakpoints
            breaks = custom_bins[1:-1]
            labels = []
            for i in range(len(custom_bins) - 1):
                if i == len(custom_bins) - 2:
                    labels.append(f"{int(custom_bins[i])}+")
                else:
                    lo = int(custom_bins[i])
                    hi = int(custom_bins[i + 1] - 1)
                    labels.append(f"{lo}-{hi}")

        age_expr = (
            pl.col("AGE_AS_INT")
            .cut(breaks, labels=labels)
            .alias("AGE_GROUP")
        )

    else:
        if custom_bins is None:
            bins_years = [0, 1 / 365.25, 30 / 365.25, 1, 2, 5, 12, 18, 100]
            bins_days = [b * 365.25 for b in bins_years]
            # Inner breakpoints only (exclude first and last boundary)
            breaks = bins_days[1:-1]
            labels = [
                "<1d",
                "1-30d",
                "1mo-1y",
                "1-2y",
                "2-5y",
                "5-12y",
                "12-18y",
                "18+y",
            ]
        else:
            bins_days = [b * 365.25 for b in custom_bins]
            breaks = bins_days[1:-1]
            labels = []
            for i in range(len(custom_bins) - 1):
                if custom_bins[i] < 1:
                    if custom_bins[i + 1] <= 30 / 365.25:
                        labels.append(
                            f"{int(custom_bins[i] * 365.25)}"
                            f"-{int(custom_bins[i + 1] * 365.25)}d"
                        )
                    else:
                        labels.append(
                            f"{int(custom_bins[i] * 12)}"
                            f"-{int(custom_bins[i + 1] * 12)}mo"
                        )
                else:
                    if i == len(custom_bins) - 2:
                        labels.append(f"{int(custom_bins[i])}+y")
                    else:
                        labels.append(
                            f"{int(custom_bins[i])}-{int(custom_bins[i + 1] - 1)}y"
                        )

        age_expr = (
            pl.col("AGE_DAYS")
            .cut(breaks, labels=labels)
            .alias("AGE_GROUP")
        )

    return df.with_columns(age_expr)


def clean_asa_class(df: FrameType) -> FrameType:
    """Clean ASA class to simple 1-5 categories.

    Handles different text formats in adult vs pediatric datasets.

    Args:
        df: Polars DataFrame or LazyFrame

    Returns:
        Same type as input with added ASA_SIMPLE column
    """
    if "ASACLAS" not in _get_columns(df):
        raise ValueError("ASACLAS column not found")

    asa_expr = pl.col("ASACLAS").str.extract(r"(\d)", 1).alias("ASA_SIMPLE")

    return df.with_columns(asa_expr)


def calculate_bmi(df: FrameType) -> FrameType:
    """Calculate BMI from height and weight if not already present.

    Args:
        df: Polars DataFrame or LazyFrame

    Returns:
        Same type as input with BMI column added (if HEIGHT and WEIGHT exist)
    """
    columns = _get_columns(df)
    if "BMI" in columns:
        return df

    if "HEIGHT" in columns and "WEIGHT" in columns:
        # BMI = weight(kg) / height(m)^2
        # NSQIP stores height in inches and weight in pounds
        bmi_expr = (
            (pl.col("WEIGHT") * 0.453592) / ((pl.col("HEIGHT") * 0.0254) ** 2)
        ).alias("BMI")

        return df.with_columns(bmi_expr)
    else:
        return df


def standardize_sex(df: FrameType) -> FrameType:
    """Standardize sex/gender coding to M/F.

    Handles different capitalizations between adult and pediatric.

    Args:
        df: Polars DataFrame or LazyFrame

    Returns:
        Same type as input with SEX_STANDARD column
    """
    if "SEX" not in _get_columns(df):
        return df

    sex_expr = pl.col("SEX").str.to_uppercase().str.slice(0, 1).alias("SEX_STANDARD")

    return df.with_columns(sex_expr)


def get_surgery_year(df: FrameType) -> FrameType:
    """Extract surgery year as integer.

    Args:
        df: Polars DataFrame or LazyFrame

    Returns:
        Same type as input with SURGERY_YEAR column
    """
    columns = _get_columns(df)
    year_col = "OPERYR" if "OPERYR" in columns else "ADMYR"

    if year_col in columns:
        year_expr = pl.col(year_col).cast(pl.Int32).alias("SURGERY_YEAR")
        return df.with_columns(year_expr)
    else:
        return df


def filter_elective_cases(df: FrameType) -> FrameType:
    """Filter to only elective (non-emergency) cases.

    Args:
        df: Polars DataFrame or LazyFrame

    Returns:
        Filtered DataFrame or LazyFrame
    """
    if "EMERGENT" in _get_columns(df):
        return df.filter(pl.col("EMERGENT") == "No")
    else:
        raise ValueError("EMERGENT column not found")


def create_outcome_summary(
    df: pl.DataFrame,
    group_var: str | None = None,
    dataset_type: Literal["adult", "pediatric"] | None = None,
) -> pl.DataFrame:
    """Create a summary table of common outcomes.

    Note: This function requires a collected DataFrame (not LazyFrame)
    because it needs to calculate percentages.

    Args:
        df: Polars DataFrame with outcome variables
        group_var: Optional grouping variable (e.g., "SURGERY_YEAR", "ASA_SIMPLE")
        dataset_type: Force "adult" or "pediatric" (auto-detects if None)

    Returns:
        Summary DataFrame with counts and rates

    Example:
        >>> summary = create_outcome_summary(df.collect())
        >>> yearly = create_outcome_summary(df.collect(), group_var="SURGERY_YEAR")
    """
    if isinstance(df, pl.LazyFrame):
        raise TypeError(
            "create_outcome_summary requires a DataFrame. Call .collect() first."
        )

    ds_type = detect_dataset_type(df, dataset_type)

    outcomes = [
        ("SUPINFEC", "Superficial Incisional SSI", "Superficial SSI"),
        ("WNDINFD", "Deep Incisional SSI", "Deep SSI"),
        ("ORGSPCSSI", "Organ/Space SSI", "Organ Space SSI"),
        ("OUPNEUMO", "Pneumonia", "Pneumonia"),
        ("URNINFEC", "Urinary Tract Infection", "UTI"),
    ]

    if ds_type == "adult":
        outcomes.extend(
            [
                ("READMISSION1", "Yes", "Readmission"),
                ("REOPERATION1", "Yes", "Reoperation"),
            ]
        )
    else:
        outcomes.extend(
            [
                ("READMISSION1", "Yes", "Readmission"),
                ("REOPERATION", "Yes", "Reoperation"),
            ]
        )

    results: list[dict] = []

    if group_var and group_var in df.columns:
        groups = df.select(pl.col(group_var).unique().sort()).to_series().to_list()

        for group in groups:
            group_df = df.filter(pl.col(group_var) == group)
            n_total = len(group_df)

            for col, positive_val, display_name in outcomes:
                if col in df.columns:
                    n_positive = len(group_df.filter(pl.col(col) == positive_val))
                    rate = (n_positive / n_total * 100) if n_total > 0 else 0

                    results.append(
                        {
                            group_var: group,
                            "Outcome": display_name,
                            "N": n_positive,
                            "Total": n_total,
                            "Rate (%)": round(rate, 2),
                        }
                    )
    else:
        n_total = len(df)

        for col, positive_val, display_name in outcomes:
            if col in df.columns:
                n_positive = len(df.filter(pl.col(col) == positive_val))
                rate = (n_positive / n_total * 100) if n_total > 0 else 0

                results.append(
                    {
                        "Outcome": display_name,
                        "N": n_positive,
                        "Total": n_total,
                        "Rate (%)": round(rate, 2),
                    }
                )

    return pl.DataFrame(results)


def export_for_stats(
    df: pl.DataFrame,
    output_path: str | Path,
    format: Literal["csv", "parquet", "stata"] = "csv",
    include_vars: list[str] | None = None,
) -> None:
    """Export data for statistical analysis in other software.

    Converts string outcomes to binary numeric for easier analysis.

    Args:
        df: Polars DataFrame to export
        output_path: Path to save file
        format: Export format
        include_vars: List of variables to include (None = all)

    Example:
        >>> export_for_stats(
        ...     df,
        ...     "analysis_data.csv",
        ...     include_vars=["AGE_AS_INT", "SEX", "ASA_SIMPLE", "ANY_SSI"]
        ... )
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if include_vars:
        df = df.select([col for col in include_vars if col in df.columns])

    binary_conversions = {
        "SUPINFEC": ("Superficial Incisional SSI", "SUPINFEC_BINARY"),
        "WNDINFD": ("Deep Incisional SSI", "WNDINFD_BINARY"),
        "ORGSPCSSI": ("Organ/Space SSI", "ORGSPCSSI_BINARY"),
        "OUPNEUMO": ("Pneumonia", "PNEUMO_BINARY"),
        "URNINFEC": ("Urinary Tract Infection", "UTI_BINARY"),
    }

    for col, (positive_val, new_col) in binary_conversions.items():
        if col in df.columns:
            df = df.with_columns(
                (pl.col(col) == positive_val).cast(pl.Int8).alias(new_col)
            )

    if format == "csv":
        df.write_csv(output_path)
    elif format == "parquet":
        df.write_parquet(output_path)
    elif format == "stata":
        try:
            df.to_pandas().to_stata(output_path)
        except ImportError:
            raise ImportError(
                "Stata export requires pandas. Install with: uv add pandas pyreadstat"
            ) from None

    logger.info("Data exported to: %s", output_path)
    logger.info("Shape: %s", df.shape)
    logger.info("Variables: %s", ", ".join(df.columns))
