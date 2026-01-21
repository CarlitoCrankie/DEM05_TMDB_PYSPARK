"""
Data Cleaning Module
Handles all data transformation - NO API calls or analysis here
"""

import logging
import json
import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, size, array_join, to_date, expr, lit
from pyspark.sql.types import StringType

# Add parent directory to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent))

from new_config import CLEANING_CONFIG

logger = logging.getLogger(__name__)


class MovieDataCleaner:
    """Handles all data cleaning and transformation using PySpark"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = CLEANING_CONFIG
        
    def load_from_json(self, movies_data: list) -> DataFrame:
        """Convert raw API data to PySpark DataFrame"""
        logger.info("Creating initial DataFrame from raw data...")
        
        json_strings = [json.dumps(movie) for movie in movies_data]
        rdd = self.spark.sparkContext.parallelize(json_strings)
        df = self.spark.read.json(rdd)
        
        logger.info(f"Initial DataFrame: {df.count()} rows × {len(df.columns)} columns")
        return df
    
    def drop_irrelevant_columns(self, df: DataFrame) -> DataFrame:
        """Step 1: Drop unnecessary columns"""
        cols_to_drop = self.config['columns_to_drop']
        existing_cols = [c for c in cols_to_drop if c in df.columns]
        logger.info(f"Dropping {len(existing_cols)} irrelevant columns")
        return df.drop(*existing_cols)
    
    def extract_nested_fields(self, df: DataFrame) -> DataFrame:
        """Step 2: Extract and flatten nested columns"""
        logger.info("Extracting nested JSON fields...")
        
        # belongs_to_collection - struct with 'name' field
        if 'belongs_to_collection' in df.columns:
            df = df.withColumn(
                'collection_name',
                when(
                    col('belongs_to_collection').isNotNull(),
                    col('belongs_to_collection.name')
                ).otherwise(lit(None).cast(StringType()))
            ).drop('belongs_to_collection').withColumnRenamed('collection_name', 'belongs_to_collection')
        
        # genres - array of structs with 'name' field
        if 'genres' in df.columns:
            df = df.withColumn(
                'genres_str',
                when(
                    (col('genres').isNotNull()) & (size(col('genres')) > 0),
                    expr("concat_ws('|', transform(genres, x -> x.name))")
                ).otherwise(lit(None).cast(StringType()))
            ).drop('genres').withColumnRenamed('genres_str', 'genres')
        
        # spoken_languages - array of structs with 'english_name' field
        if 'spoken_languages' in df.columns:
            df = df.withColumn(
                'spoken_languages_str',
                when(
                    (col('spoken_languages').isNotNull()) & (size(col('spoken_languages')) > 0),
                    expr("concat_ws('|', transform(spoken_languages, x -> x.english_name))")
                ).otherwise(lit(None).cast(StringType()))
            ).drop('spoken_languages').withColumnRenamed('spoken_languages_str', 'spoken_languages')
        
        # production_countries - array of structs with 'name' field
        if 'production_countries' in df.columns:
            df = df.withColumn(
                'production_countries_str',
                when(
                    (col('production_countries').isNotNull()) & (size(col('production_countries')) > 0),
                    expr("concat_ws('|', transform(production_countries, x -> x.name))")
                ).otherwise(lit(None).cast(StringType()))
            ).drop('production_countries').withColumnRenamed('production_countries_str', 'production_countries')
        
        # production_companies - array of structs with 'name' field
        if 'production_companies' in df.columns:
            df = df.withColumn(
                'production_companies_str',
                when(
                    (col('production_companies').isNotNull()) & (size(col('production_companies')) > 0),
                    expr("concat_ws('|', transform(production_companies, x -> x.name))")
                ).otherwise(lit(None).cast(StringType()))
            ).drop('production_companies').withColumnRenamed('production_companies_str', 'production_companies')
        
        # cast - array of structs with 'name' field
        if 'cast' in df.columns:
            # Get cast size first
            df = df.withColumn(
                'cast_size',
                when(col('cast').isNotNull(), size(col('cast'))).otherwise(lit(0))
            )
            
            # Extract cast names
            df = df.withColumn(
                'cast_str',
                when(
                    (col('cast').isNotNull()) & (size(col('cast')) > 0),
                    expr("concat_ws('|', transform(cast, x -> x.name))")
                ).otherwise(lit(None).cast(StringType()))
            ).drop('cast').withColumnRenamed('cast_str', 'cast')
        
        # crew - extract director and crew_size
        if 'crew' in df.columns:
            # Get crew size
            df = df.withColumn(
                'crew_size',
                when(col('crew').isNotNull(), size(col('crew'))).otherwise(lit(0))
            )
            
            # Extract director(s)
            df = df.withColumn(
                'director',
                when(
                    (col('crew').isNotNull()) & (size(col('crew')) > 0),
                    expr("concat_ws('|', transform(filter(crew, x -> x.job = 'Director'), x -> x.name))")
                ).otherwise(lit(None).cast(StringType()))
            ).drop('crew')
        
        # origin_country - array of strings
        if 'origin_country' in df.columns:
            df = df.withColumn(
                'origin_country_str',
                when(
                    (col('origin_country').isNotNull()) & (size(col('origin_country')) > 0),
                    array_join(col('origin_country'), '|')
                ).otherwise(lit(None).cast(StringType()))
            ).drop('origin_country').withColumnRenamed('origin_country_str', 'origin_country')
        
        # Debug: Show sample after extraction
        logger.info("Sample data after nested field extraction:")
        df.select('title', 'genres', 'belongs_to_collection', 'director').show(3, truncate=50)
        
        return df
    
    def convert_datatypes(self, df: DataFrame) -> DataFrame:
        """Step 3: Convert columns to proper data types"""
        logger.info("Converting data types...")
        
        df = (df
              .withColumn('budget', col('budget').cast('double'))
              .withColumn('revenue', col('revenue').cast('double'))
              .withColumn('id', col('id').cast('int'))
              .withColumn('popularity', col('popularity').cast('double'))
              .withColumn('vote_count', col('vote_count').cast('int'))
              .withColumn('vote_average', col('vote_average').cast('double'))
              .withColumn('runtime', col('runtime').cast('int'))
              .withColumn('release_date', to_date(col('release_date')))
        )
        
        return df
    
    def handle_missing_and_invalid(self, df: DataFrame) -> DataFrame:
        """Step 4: Handle missing and incorrect values"""
        logger.info("Handling missing and invalid values...")
        
        # Replace 0 with null for budget, revenue, runtime
        df = (df
              .withColumn('budget', when(col('budget') == 0, None).otherwise(col('budget')))
              .withColumn('revenue', when(col('revenue') == 0, None).otherwise(col('revenue')))
              .withColumn('runtime', when(col('runtime') == 0, None).otherwise(col('runtime')))
        )
        
        # Convert to millions USD
        df = (df
              .withColumn('budget_musd', col('budget') / 1_000_000)
              .withColumn('revenue_musd', col('revenue') / 1_000_000)
              .drop('budget', 'revenue')
        )
        
        # Handle vote_average for 0 vote_count
        df = df.withColumn(
            'vote_average',
            when(col('vote_count') == 0, None).otherwise(col('vote_average'))
        )
        
        # Replace placeholder text with null
        placeholders = self.config['null_placeholders']
        df = (df
              .withColumn('overview', 
                         when(col('overview').isin(placeholders), None).otherwise(col('overview')))
              .withColumn('tagline',
                         when(col('tagline').isin(placeholders), None).otherwise(col('tagline')))
        )
        
        # Replace empty strings with null for string columns
        string_cols = ['genres', 'belongs_to_collection', 'director', 'cast',
                       'production_companies', 'production_countries', 'spoken_languages']
        for col_name in string_cols:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(col(col_name) == '', None).otherwise(col(col_name))
                )
        
        return df
    
    def remove_duplicates_and_filter(self, df: DataFrame) -> DataFrame:
        """Step 5: Remove duplicates and apply filters"""
        logger.info("Removing duplicates and filtering...")
        
        # Remove duplicates by id
        initial_count = df.count()
        df = df.dropDuplicates(['id'])
        logger.info(f"Removed {initial_count - df.count()} duplicate rows")
        
        # Drop rows with null id or title
        df = df.filter(col('id').isNotNull() & col('title').isNotNull())
        
        # Filter for Released movies only
        if 'status' in df.columns:
            df = df.filter(col('status') == 'Released').drop('status')
        
        # Keep rows with at least min_non_null_values non-null values
        min_non_null = self.config['min_non_null_values']
        df = self._filter_by_non_null_count(df, min_non_null)
        
        return df
    
    def _filter_by_non_null_count(self, df: DataFrame, min_non_null: int) -> DataFrame:
        """Filter rows based on minimum non-null column count"""
        non_null_expr = sum([
            when(col(c).isNotNull(), 1).otherwise(0) 
            for c in df.columns
        ])
        
        df_filtered = df.filter(non_null_expr >= min_non_null)
        logger.info(f"Kept rows with >= {min_non_null} non-null values")
        return df_filtered
    
    def reorder_columns(self, df: DataFrame) -> DataFrame:
        """Step 6: Reorder columns to match specification"""
        column_order = self.config['column_order']
        
        # Select only existing columns in specified order
        existing_cols = [c for c in column_order if c in df.columns]
        logger.info(f"Reordering to {len(existing_cols)} final columns")
        return df.select(existing_cols)
    
    def clean_pipeline(self, movies_data: list) -> DataFrame:
        """Execute complete cleaning pipeline"""
        logger.info("="*60)
        logger.info("Starting Data Cleaning Pipeline")
        logger.info("="*60)
        
        df = self.load_from_json(movies_data)
        df = self.drop_irrelevant_columns(df)
        df = self.extract_nested_fields(df)
        df = self.convert_datatypes(df)
        df = self.handle_missing_and_invalid(df)
        df = self.remove_duplicates_and_filter(df)
        df = self.reorder_columns(df)
        
        logger.info(f"Cleaning complete: {df.count()} rows × {len(df.columns)} columns")
        logger.info("="*60)
        
        return df
