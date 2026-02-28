# Databricks Python notebook
# Transform Silver -> Gold dimensional table: swapi_demo.gold.dim_characters

from pyspark.sql.functions import col, when, lit, current_timestamp, expr

# Ensure gold schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS swapi_demo.gold")

# Read silver source
src = 'swapi_demo.silver.characters_cleaned'
try:
    df = spark.table(src)
    print(f"Reading source table: {src}")
except Exception as e:
    raise RuntimeError(f"Required silver table not found: {src}")

# Compute timeline: BBY -> negative, ABY -> positive
df_gold = df.withColumn('birth_year_timeline', expr("CASE WHEN birth_era = 'BBY' THEN -birth_year_value WHEN birth_era = 'ABY' THEN birth_year_value ELSE NULL END"))

# Height category
df_gold = df_gold.withColumn('height_category', when(col('height_cm').isNull(), None)
                                         .when(col('height_cm') < 120, 'Short')
                                         .when((col('height_cm') >= 120) & (col('height_cm') <= 190), 'Medium')
                                         .when(col('height_cm') > 190, 'Tall')
                                         .otherwise(None))

# Mass category
df_gold = df_gold.withColumn('mass_category', when(col('mass_kg').isNull(), None)
                                        .when(col('mass_kg') < 50, 'Light')
                                        .when((col('mass_kg') >= 50) & (col('mass_kg') <= 100), 'Medium')
                                        .when(col('mass_kg') > 100, 'Heavy')
                                        .otherwise(None))

# Droid indicator: name contains '-' or any digit
df_gold = df_gold.withColumn('is_droid', (col('character_name').rlike('-')) | (col('character_name').rlike('[0-9]')))

# Add record_created_timestamp
df_gold = df_gold.withColumn('record_created_timestamp', current_timestamp())

# Select columns in required order for the gold table
gold_select = df_gold.select(
    col('character_name'),
    col('height_cm'),
    col('mass_kg'),
    col('birth_year_timeline'),
    col('height_category'),
    col('mass_category'),
    col('is_droid'),
    col('homeworld_id'),
    col('record_created_timestamp')
)

# Create target table with identity column if not exists
create_ddl = '''
CREATE TABLE IF NOT EXISTS swapi_demo.gold.dim_characters (
  character_id BIGINT GENERATED ALWAYS AS IDENTITY,
  character_name STRING,
  height_cm INT,
  mass_kg DOUBLE,
  birth_year_timeline DOUBLE,
  height_category STRING,
  mass_category STRING,
  is_droid BOOLEAN,
  homeworld_id INT,
  record_created_timestamp TIMESTAMP
) USING DELTA
'''

spark.sql(create_ddl)

# Insert overwrite into gold table using DataFrame write (avoids arity mismatch with identity column)
gold_select.write.format("delta").mode("overwrite").option("mergeSchema", "false").saveAsTable("swapi_demo.gold.dim_characters")

print(f"Wrote {spark.table('swapi_demo.gold.dim_characters').count()} rows into swapi_demo.gold.dim_characters")
