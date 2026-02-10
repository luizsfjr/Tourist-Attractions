from pyspark.sql import SparkSession
import project_config

spark = (
    SparkSession.builder
    .appName("tripadvisor-etl")
    # .master("local[*]")
    # .config("spark.local.dir", "C:\\spark-temp")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# "data/raw/Attraction_Belem.csv",
df = spark.read.csv(
    f"{project_config.RAW}/Attraction_Belem.csv",
    header=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True
)

df.show(5)

df.summary().show()

df.printSchema()

df = df.drop('_c0')

df_bronze = df.filter((F.col("Name").isNotNull()) & (F.trim(F.col("Name")) != ""))
df_bronze.show(5)

df_bronze = df_bronze.withColumn(
    "Date_Travel",
    F.to_date(
        F.concat_ws(
            "-",
            F.regexp_extract(F.col("Date"), r"(\d{4})", 1),
            F.lpad(
                F.when(F.col("Date").rlike("janeiro"), "01")
                 .when(F.col("Date").rlike("fevereiro"), "02")
                 .when(F.col("Date").rlike("março|marco"), "03")
                 .when(F.col("Date").rlike("abril"), "04")
                 .when(F.col("Date").rlike("maio"), "05")
                 .when(F.col("Date").rlike("junho"), "06")
                 .when(F.col("Date").rlike("julho"), "07")
                 .when(F.col("Date").rlike("agosto"), "08")
                 .when(F.col("Date").rlike("setembro"), "09")
                 .when(F.col("Date").rlike("outubro"), "10")
                 .when(F.col("Date").rlike("novembro"), "11")
                 .when(F.col("Date").rlike("dezembro"), "12")
                 .otherwise(None),
                2,
                "0"
            ),
            F.lit("01")
        ),
        "yyyy-MM-dd"
    )
)
df_bronze = df_bronze.withColumn("Rating_Review", F.col("Rating_Review").cast("int"))
df_bronze = df_bronze.withColumn("Rating_attaction", F.col("Rating_attaction").cast("int"))
df_bronze.show(5)

df_bronze.show(5)

#--------------Modelagem-------------
# Attraction -> ID, Name, Rating_attraction
### User -> ID, Username, State (Cardinality problem)
# User_Review -> ID, ID_User, ID_Attraction, Title_Review, Rating_Review, Review, Username, State, Type_traveler, Date

# dim_name
df_dim_silver_attractions = (
    df_bronze.select(["Name","Rating_attaction"])
      .where(F.trim(F.col("Name")).isNotNull())
      .dropDuplicates(["Name"])
      .withColumn("Attraction_id", F.row_number().over(Window.orderBy("Name")))
      .withColumn("Rating_Attraction", F.col("Rating_attaction"))
      .select("Attraction_id", "Name", "Rating_Attraction")
)
df_dim_silver_attractions.show(100,truncate=False)

df_fact_silver_reviews = df_bronze.drop("Rating_attaction", "Date")
fact = df_fact_silver_reviews.alias("fact")
dim = df_dim_silver_attractions.alias("dim")

df_fact_silver_reviews = (
    fact.join(
        dim,
        F.lower(F.col("fact.Name")) == F.lower(F.col("dim.Name")),
        "left"
    )
    .drop("Name")           
    .drop("Rating_Attraction") 
)

df_fact_silver_reviews.show(5)

# # Quality Tests dim attractions

df_validation_dim_attractions = df_dim_silver_attractions.withColumn("validation_errors", F.lit(""))

# Rule 1: 'id' is REQUIRED and should not be null

df_validation_dim_attractions = df_validation_dim_attractions.withColumn(
    "validation_errors",
    F.when(F.isnull(F.col("Attraction_id")), F.concat(F.col("validation_errors"), F.lit("ID_NULL;"))) # Changed to use concat function
    .otherwise(F.col("validation_errors"))
)

# Rule 2: 'id' must be unique

w = Window.partitionBy("Attraction_id")

df_validation_dim_attractions = df_validation_dim_attractions.withColumn(
    "validation_errors",
    F.when(
        F.count("Attraction_id").over(w) > 1,
        F.concat(F.col("validation_errors"), F.lit("ID_NOT_UNIQUE"))
    ).otherwise(F.col("validation_errors"))
)

 # Rule 3: 'Rating_Attraction' should be a positive integer and less than 50

df_validation_dim_attractions = df_validation_dim_attractions.withColumn(
"validation_errors",
F.when((F.col("Rating_Attraction") <= 0) | (F.col("Rating_Attraction") > 50), F.concat(F.col("validation_errors"), F.lit("RATING_ATTRACTION_INVALID;"))) # Changed to use concat function
.otherwise(F.col("validation_errors"))
)

# Rule 4: 'Name' is REQUIRED and should not be null

df_validation_dim_attractions = df_validation_dim_attractions.withColumn(
"validation_errors",
F.when(F.isnull(F.col("Name")), F.concat(F.col("validation_errors"), F.lit("NAME_NULL;"))) # Changed to use concat function
.otherwise(F.col("validation_errors"))
)

df_validation_dim_attractions = df_validation_dim_attractions.withColumn(
  "is_valid",
  F.when(F.length(F.trim(F.col("validation_errors"))) == 0, True).otherwise(False)
 )

df_dim_invalid_attractions = df_validation_dim_attractions.filter(F.col("is_valid") == False).drop("is_valid")
df_dim_gold_attractions = df_validation_dim_attractions.filter(F.col("is_valid") == True).drop("is_valid", "validation_errors")

# # Quality Tests fact reviews

df_validation_fact_reviews = df_fact_silver_reviews.withColumn("validation_errors", F.lit(""))

# Rule 1: Username is REQUIRED
df_validation_fact_reviews = df_validation_fact_reviews.withColumn(
    "validation_errors",
    F.when(F.col("Username").isNull(), F.concat(F.col("validation_errors"), F.lit("USERNAME_NULL;")))
     .otherwise(F.col("validation_errors"))
)

# Rule 2: Rating_Review must be between 0 and 50 (adjust if scale is 0–5)
df_validation_fact_reviews = df_validation_fact_reviews.withColumn(
    "validation_errors",
    F.when((F.col("Rating_Review") <= 0) | (F.col("Rating_Review") > 50),
           F.concat(F.col("validation_errors"), F.lit("RATING_REVIEW_INVALID;")))
     .otherwise(F.col("validation_errors"))
)

# Rule 3: Review text is REQUIRED
df_validation_fact_reviews = df_validation_fact_reviews.withColumn(
    "validation_errors",
    F.when(F.col("Review").isNull(), F.concat(F.col("validation_errors"), F.lit("REVIEW_NULL;")))
     .otherwise(F.col("validation_errors"))
)

# Rule 4: Date is REQUIRED
df_validation_fact_reviews = df_validation_fact_reviews.withColumn(
    "validation_errors",
    F.when(F.col("Date_Travel").isNull(), F.concat(F.col("validation_errors"), F.lit("DATE_NULL;")))
     .otherwise(F.col("validation_errors"))
)

# Rule 5: Type_traveler is REQUIRED and must be one of existents categories
categories = ["couples", "families", "alone", "business", "friends"]
df_validation_fact_reviews = df_validation_fact_reviews.withColumn(
    "validation_errors",
    F.when((F.col("Type_traveler").isNull()) | (~F.lower(F.trim(F.col("Type_traveler"))).isin(categories)), F.concat(F.col("validation_errors"), F.lit("TYPE_TRAVELER_INVALID;")))
     .otherwise(F.col("validation_errors"))
)

df_validation_fact_reviews = df_validation_fact_reviews.withColumn(
    "is_valid",
    F.when(F.length(F.trim(F.col("validation_errors"))) == 0, True).otherwise(False)
)

df_fact_invalid_reviews = df_validation_fact_reviews.filter(F.col("is_valid") == False).drop("is_valid")
df_fact_gold_reviews = df_validation_fact_reviews.filter(F.col("is_valid") == True).drop("is_valid", "validation_errors")

# Export to Data Lake or Local Repository

if project_config.IS_GCP:

    # Bronze
    df_bronze.write.mode("overwrite").parquet(
        f"{project_config.BRONZE}/df_bronze"
    )

    # Silver
    df_fact_silver_reviews.write.mode("overwrite").parquet(
        f"{project_config.SILVER}/fact_reviews"
    )

    df_dim_silver_attractions.write.mode("overwrite").parquet(
        f"{project_config.SILVER}/dim_attractions"
    )

    # Gold
    df_fact_gold_reviews.write.mode("overwrite").parquet(
        f"{project_config.GOLD}/fact_reviews"
    )

    df_dim_gold_attractions.write.mode("overwrite").parquet(
        f"{project_config.GOLD}/dim_attractions"
    )

    # Invalid Bucket
    df_fact_invalid_reviews.write.mode("overwrite").parquet(
        f"{project_config.INVALID}/fact_reviews_invalid"
    )

    df_dim_invalid_attractions.write.mode("overwrite").parquet(
        f"{project_config.INVALID}/dim_invalid"
    )
else:
    # Bronze
    df_bronze.toPandas().to_csv(f"{project_config.BRONZE}/df_bronze.csv")

    # Silver
    df_fact_silver_reviews.toPandas().to_csv(f"{project_config.SILVER}/fact_reviews.csv")
    df_dim_silver_attractions.toPandas().to_csv(f"{project_config.SILVER}/dim_attractions.csv")
    
    # Gold
    df_fact_gold_reviews.toPandas().to_csv(f"{project_config.GOLD}/fact_reviews.csv")
    df_dim_gold_attractions.toPandas().to_csv(f"{project_config.GOLD}/dim_attrractions.csv")

    # Invalid Bucket
    df_fact_invalid_reviews.toPandas().to_csv(f"{project_config.INVALID}/fact_reviews_invalid.csv")
    df_dim_invalid_attractions.toPandas().to_csv(f"{project_config.INVALID}/dim_invalid.csv")

spark.stop()