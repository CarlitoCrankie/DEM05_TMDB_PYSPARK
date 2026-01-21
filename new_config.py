"""
Configuration file for TMDB Spark Pipeline
All settings and configurations are centralized here
"""

import os
from pathlib import Path

# LOAD ENVIRONMENT VARIABLES FROM .env FILE
def load_env_file():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent / '.env'
    
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    os.environ.setdefault(key, value)


# Load .env file
load_env_file()

# API CONFIGURATION
# TMDB API Key - can be overridden by environment variable
TMDB_API_KEY = os.environ.get('TMDB_API_KEY')

TMDB_BASE_URL = "https://api.themoviedb.org/3"

# Movie IDs to extract
MOVIE_IDS = [0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 
             168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445, 
             321612, 260513]

# SPARK CONFIGURATION
SPARK_CONFIG = {
    'app_name': 'TMDB Movie Analysis',
    'driver_memory': os.environ.get('SPARK_DRIVER_MEMORY', '2g'),
    'executor_memory': os.environ.get('SPARK_EXECUTOR_MEMORY', '2g'),
    'shuffle_partitions': '8',
    'default_parallelism': '8',
    'arrow_enabled': 'false',
    'driver_host': 'localhost',
    'driver_bind_address': '0.0.0.0',
}

# PATH CONFIGURATION
PATHS = {
    'raw_data': 'data/raw',
    'processed_data': 'data/processed',
    'analysis_data': 'data/analysis',
    'visualizations': 'visualizations',
    'logs': 'logs',
}

# File names
FILES = {
    'raw_json': 'movies_raw.json',
    'clean_parquet': 'movies_clean.parquet',
    'log_file': 'pipeline.log',
}

# LOGGING CONFIGURATION
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_to_file': True,
    'log_to_console': True,
}

# DATA CLEANING CONFIGURATION
CLEANING_CONFIG = {
    # Columns to drop during cleaning
    'columns_to_drop': ['adult', 'imdb_id', 'original_title', 'video', 'homepage'],
    
    # Placeholder values to treat as null
    'null_placeholders': ['No Data', '', 'N/A', 'no data', 'NA'],
    
    # Minimum non-null values required per row
    'min_non_null_values': 10,
    
    # Final column order
    'column_order': [
        'id', 'title', 'tagline', 'release_date', 'genres',
        'belongs_to_collection', 'original_language', 'budget_musd',
        'revenue_musd', 'production_companies', 'production_countries',
        'vote_count', 'vote_average', 'popularity', 'runtime',
        'overview', 'spoken_languages', 'poster_path', 'cast',
        'cast_size', 'director', 'crew_size'
    ],
}

# ANALYSIS CONFIGURATION
ANALYSIS_CONFIG = {
    # Minimum budget (in millions) for ROI calculations
    'min_budget_for_roi': 10,
    
    # Minimum vote count for rating rankings
    'min_votes_for_rating': 10,
    
    # Number of top results to return
    'top_n_results': 10,
}

# VISUALIZATION CONFIGURATION
VISUALIZATION_CONFIG = {
    'dpi': 150,
    'figure_size': (12, 7),
    'style': 'whitegrid',
    'colors': {
        'primary': '#2ecc71',
        'secondary': '#3498db',
        'accent': '#e74c3c',
        'warning': '#f39c12',
    },
}

# API REQUEST CONFIGURATION
API_CONFIG = {
    'max_retries': 3,
    'retry_delay': 2,
    'request_timeout': 10,
    'rate_limit_delay': 0.25,
}
