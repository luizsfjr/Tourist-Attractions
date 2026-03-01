import logging

import pandas as pd

import project_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def append_validation_error(df: pd.DataFrame, condition: pd.Series, error_code: str) -> None:
    cond = condition.fillna(False)
    df.loc[cond, "validation_errors"] = df.loc[cond, "validation_errors"] + error_code


def run_transform() -> None:
    logger.info("Starting transform pipeline")
    input_path = f"{project_config.RAW}/Attraction_Belem.csv"
    logger.info("Reading raw file from %s", input_path)
    df = pd.read_csv(input_path, encoding="utf-8")
    logger.info("Raw dataframe loaded with %d rows and %d columns", len(df), len(df.columns))

    df_bronze = df[df["Name"].notna() & (df["Name"].astype("string").str.strip() != "")].copy()
    logger.info("Bronze layer created with %d rows", len(df_bronze))

    date_text = df_bronze["Date"].astype("string").str.lower()
    month_patterns = {
        r"janeiro": "01",
        r"fevereiro": "02",
        r"marÃ§o|marco|marÃƒÂ§o": "03",
        r"abril": "04",
        r"maio": "05",
        r"junho": "06",
        r"julho": "07",
        r"agosto": "08",
        r"setembro": "09",
        r"outubro": "10",
        r"novembro": "11",
        r"dezembro": "12",
    }

    month = pd.Series(pd.NA, index=df_bronze.index, dtype="string")
    for pattern, value in month_patterns.items():
        month = month.mask(date_text.str.contains(pattern, regex=True, na=False), value)

    year = date_text.str.extract(r"(\d{4})", expand=False)
    df_bronze["Date_Travel"] = pd.to_datetime(
        year + "-" + month + "-01",
        format="%Y-%m-%d",
        errors="coerce",
    )

    df_bronze["Rating_Review"] = pd.to_numeric(df_bronze["Rating_Review"], errors="coerce").astype("Int64")
    df_bronze["Rating_attaction"] = pd.to_numeric(df_bronze["Rating_attaction"], errors="coerce").astype("Int64")
    logger.info(
        "Type casting complete. Date_Travel nulls: %d, Rating_Review nulls: %d, Rating_attaction nulls: %d",
        df_bronze["Date_Travel"].isna().sum(),
        df_bronze["Rating_Review"].isna().sum(),
        df_bronze["Rating_attaction"].isna().sum(),
    )

    df_dim_silver_attractions = (
        df_bronze.loc[
            df_bronze["Name"].notna() & (df_bronze["Name"].astype("string").str.strip() != ""),
            ["Name", "Rating_attaction"],
        ]
        .drop_duplicates(subset=["Name"])
        .sort_values("Name")
        .reset_index(drop=True)
    )
    df_dim_silver_attractions["Attraction_id"] = (df_dim_silver_attractions.index + 1).astype("Int64")
    df_dim_silver_attractions["Rating_Attraction"] = pd.to_numeric(
        df_dim_silver_attractions["Rating_attaction"],
        errors="coerce",
    ).astype("Int64")
    df_dim_silver_attractions = df_dim_silver_attractions[["Attraction_id", "Name", "Rating_Attraction"]]
    logger.info("Silver dim_attractions created with %d rows", len(df_dim_silver_attractions))

    df_fact_silver_reviews = df_bronze.drop(columns=["Rating_attaction", "Date"]).copy()
    fact = df_fact_silver_reviews.copy()
    dim = df_dim_silver_attractions.copy()
    fact["_name_key"] = fact["Name"].astype("string").str.lower()
    dim["_name_key"] = dim["Name"].astype("string").str.lower()

    df_fact_silver_reviews = (
        fact.merge(
            dim[["_name_key", "Attraction_id", "Rating_Attraction"]],
            on="_name_key",
            how="left",
        )
        .drop(columns=["Name", "Rating_Attraction", "_name_key"])
    )
    logger.info("Silver fact_reviews created with %d rows", len(df_fact_silver_reviews))

    df_validation_dim_attractions = df_dim_silver_attractions.copy()
    df_validation_dim_attractions["validation_errors"] = ""
    append_validation_error(
        df_validation_dim_attractions,
        df_validation_dim_attractions["Attraction_id"].isna(),
        "ID_NULL;",
    )

    duplicate_ids = df_validation_dim_attractions["Attraction_id"].duplicated(keep=False)
    append_validation_error(df_validation_dim_attractions, duplicate_ids, "ID_NOT_UNIQUE;")

    invalid_rating_attr = (
        (df_validation_dim_attractions["Rating_Attraction"] <= 0)
        | (df_validation_dim_attractions["Rating_Attraction"] > 50)
    )
    append_validation_error(df_validation_dim_attractions, invalid_rating_attr, "RATING_ATTRACTION_INVALID;")
    append_validation_error(
        df_validation_dim_attractions,
        df_validation_dim_attractions["Name"].isna(),
        "NAME_NULL;",
    )

    df_validation_dim_attractions["is_valid"] = (
        df_validation_dim_attractions["validation_errors"].str.strip() == ""
    )

    df_dim_invalid_attractions = (
        df_validation_dim_attractions.loc[~df_validation_dim_attractions["is_valid"]]
        .drop(columns=["is_valid"])
    )
    df_dim_gold_attractions = (
        df_validation_dim_attractions.loc[df_validation_dim_attractions["is_valid"]]
        .drop(columns=["is_valid", "validation_errors"])
    )
    logger.info(
        "Dim validation finished. Gold: %d | Invalid: %d",
        len(df_dim_gold_attractions),
        len(df_dim_invalid_attractions),
    )

    df_validation_fact_reviews = df_fact_silver_reviews.copy()
    df_validation_fact_reviews["validation_errors"] = ""
    append_validation_error(
        df_validation_fact_reviews,
        df_validation_fact_reviews["Username"].isna(),
        "USERNAME_NULL;",
    )

    invalid_rating_review = (
        (df_validation_fact_reviews["Rating_Review"] <= 0)
        | (df_validation_fact_reviews["Rating_Review"] > 50)
    )
    append_validation_error(df_validation_fact_reviews, invalid_rating_review, "RATING_REVIEW_INVALID;")
    append_validation_error(df_validation_fact_reviews, df_validation_fact_reviews["Review"].isna(), "REVIEW_NULL;")
    append_validation_error(df_validation_fact_reviews, df_validation_fact_reviews["Date_Travel"].isna(), "DATE_NULL;")

    categories = ["couples", "families", "alone", "business", "friends"]
    traveler_clean = df_validation_fact_reviews["Type_traveler"].astype("string").str.strip().str.lower()
    invalid_type = df_validation_fact_reviews["Type_traveler"].isna() | (~traveler_clean.isin(categories).fillna(False))
    append_validation_error(df_validation_fact_reviews, invalid_type, "TYPE_TRAVELER_INVALID;")

    df_validation_fact_reviews["is_valid"] = (
        df_validation_fact_reviews["validation_errors"].str.strip() == ""
    )
    df_fact_invalid_reviews = (
        df_validation_fact_reviews.loc[~df_validation_fact_reviews["is_valid"]]
        .drop(columns=["is_valid"])
    )
    df_fact_gold_reviews = (
        df_validation_fact_reviews.loc[df_validation_fact_reviews["is_valid"]]
        .drop(columns=["is_valid", "validation_errors"])
    )
    logger.info(
        "Fact validation finished. Gold: %d | Invalid: %d",
        len(df_fact_gold_reviews),
        len(df_fact_invalid_reviews),
    )

    # Bronze
    df_bronze.to_csv(f"{project_config.BRONZE}/df_bronze.csv")

    # Silver
    df_fact_silver_reviews.to_csv(f"{project_config.SILVER}/fact_reviews.csv")
    df_dim_silver_attractions.to_csv(f"{project_config.SILVER}/dim_attractions.csv")

    # Gold
    df_fact_gold_reviews.to_csv(f"{project_config.GOLD}/fact_reviews.csv", index=False)
    df_dim_gold_attractions.to_csv(f"{project_config.GOLD}/dim_attractions.csv", index=False)

    # Invalid Bucket
    df_fact_invalid_reviews.to_csv(f"{project_config.INVALID}/fact_reviews_invalid.csv")
    df_dim_invalid_attractions.to_csv(f"{project_config.INVALID}/dim_invalid.csv")
    logger.info(
        "Export completed. Outputs written to Bronze=%s, Silver=%s, Gold=%s, Invalid=%s",
        project_config.BRONZE,
        project_config.SILVER,
        project_config.GOLD,
        project_config.INVALID,
    )
