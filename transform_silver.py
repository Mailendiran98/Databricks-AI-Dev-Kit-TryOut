# Databricks Python notebook
# Transform Bronze/raw data into swapi_demo.silver.characters_cleaned

from pyspark.sql.functions import col, regexp_extract, trim, when, regexp_replace, current_timestamp, lit

# Ensure silver schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS swapi_demo.silver")

# Prefer raw_data as primary source (bronze was incorrect); fall back to bronze only if needed
src_table = "swapi_demo.raw_data.characters"
try:
    df = spark.table(src_table)
    print(f"Reading source table: {src_table}")
except Exception:
    # If raw_data isn't present, try the older bronze location
    src_table = "swapi_demo.bronze.characters"
    df = spark.table(src_table)
    print(f"Raw_data not found; falling back to: {src_table}")

# Normalise and clean
df_clean = df.select(
    trim(col('name')).alias('character_name'),
    trim(col('height')).alias('height_raw'),
    trim(col('mass')).alias('mass_raw'),
    trim(col('birth_year')).alias('birth_year_raw'),
    trim(col('homeworld')).alias('homeworld_raw')
)

# Replace 'unknown' with NULL and remove commas
df_clean = df_clean.withColumn('height_raw', when((col('height_raw').isNull()) | (col('height_raw') == '') | (col('height_raw').rlike('(?i)^unknown$')), None).otherwise(regexp_replace(col('height_raw'), ',', '')))

df_clean = df_clean.withColumn('mass_raw', when((col('mass_raw').isNull()) | (col('mass_raw') == '') | (col('mass_raw').rlike('(?i)^unknown$')), None).otherwise(regexp_replace(col('mass_raw'), ',', '')))

# Parse birth year numeric and era
df_clean = df_clean.withColumn('birth_year_value', when((col('birth_year_raw').isNull()) | (col('birth_year_raw').rlike('(?i)^unknown$')), None).otherwise(regexp_extract(col('birth_year_raw'), '([0-9]+\\.?[0-9]*)', 1).cast('double')))

df_clean = df_clean.withColumn('birth_era', when((col('birth_year_raw').isNull()) | (col('birth_year_raw').rlike('(?i)^unknown$')), None).otherwise(regexp_extract(col('birth_year_raw'), '([A-Za-z]+)', 1)))

# Parse homeworld id from URL like https://swapi.dev/api/planets/28/
df_clean = df_clean.withColumn('homeworld_id', when((col('homeworld_raw').isNull()) | (col('homeworld_raw') == ''), None).otherwise(regexp_extract(col('homeworld_raw'), '/planets/([0-9]+)/', 1).cast('int')))

# Cast types for height and mass
from pyspark.sql.functions import col

df_clean = df_clean.withColumn('height_cm', when(col('height_raw').isNull(), None).otherwise(col('height_raw').cast('int')))

df_clean = df_clean.withColumn('mass_kg', when(col('mass_raw').isNull(), None).otherwise(col('mass_raw').cast('double')))

# Add metadata
df_clean = df_clean.withColumn('source_system', lit('SWAPI'))

df_clean = df_clean.withColumn('ingestion_timestamp', current_timestamp())

# Select final columns in required order
final = df_clean.select(
    'character_name',
    'height_cm',
    'mass_kg',
    'birth_year_value',
    'birth_era',
    'homeworld_id',
    'source_system',
    'ingestion_timestamp'
)

# Write out as Delta table (overwrite to keep silver aligned with latest ingest)
final.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('swapi_demo.silver.characters_cleaned')

print(f"Wrote {final.count()} rows into swapi_demo.silver.characters_cleaned")
