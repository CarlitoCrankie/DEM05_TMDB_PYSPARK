"""
scripts/pipeline.py
Main Pipeline Orchestrator - Coordinates all modules
"""

import logging
from pyspark.sql import SparkSession
from pathlib import Path
import os
import sys
import importlib.util

# Import all modules (make sure they're in same directory)
from data_extraction import TMDBExtractor
from data_transformations import MovieDataCleaner
from analysis import MovieAnalyzer
from visualizations import MovieVisualizer

# Load config dynamically
config_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'new_config.py')
)
spec = importlib.util.spec_from_file_location("new_config", config_path)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
TMDB_API_KEY_CONFIG = config.TMDB_API_KEY
MOVIE_IDS_CONFIG = config.MOVIE_IDS

# Set Python executable for Spark on Windows
python_exec = sys.executable
os.environ['PYSPARK_PYTHON'] = python_exec
os.environ['PYSPARK_DRIVER_PYTHON'] = python_exec

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TMDBPipeline:
    
    def __init__(self, TMDB_API_KEY: str, movie_ids: list):
        self.api_key = TMDB_API_KEY
        self.movie_ids = movie_ids
        
        # Initialize Spark with optimized configuration
        self.spark = SparkSession.builder \
            .appName("TMDB Movie Analysis") \
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g")) \
            .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")) \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .getOrCreate()

        
        logger.info(f"Spark Session Created: {self.spark.version}")
        
        # Initialize all modules
        self.extractor = TMDBExtractor(self.api_key)
        self.cleaner = MovieDataCleaner(self.spark)
        self.analyzer = MovieAnalyzer()
        self.visualizer = MovieVisualizer()
        
        # Setup directories
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories"""
        dirs = ['data/raw', 'data/processed', 'data/analysis', 'visualizations']
        for d in dirs:
            Path(d).mkdir(parents=True, exist_ok=True)
    
    def run_extraction(self):
        """Step 1: Extract data from API"""
        logger.info("="*70)
        logger.info("STEP 1: DATA EXTRACTION FROM TMDB API")
        logger.info("="*70)
        
        raw_data = self.extractor.extract_movies(
            self.movie_ids,
            save_path='data/raw/movies_raw.json'
        )
        
        return raw_data
    
    def run_cleaning(self, raw_data):
        """Step 2: Clean and transform data"""
        logger.info("="*70)
        logger.info("STEP 2: DATA CLEANING AND TRANSFORMATION")
        logger.info("="*70)
        
        df_clean = self.cleaner.clean_pipeline(raw_data)
        
        # Cache for multiple operations ahead
        df_clean.cache()
        logger.info("DataFrame cached for performance optimization")
        
        # Save to parquet (columnar format - optimized for analytics)
        df_clean.write.mode('overwrite').parquet('data/processed/movies_clean.parquet')
        logger.info("Cleaned data saved to Parquet format")
        
        return df_clean
    
    def run_analysis(self, df_clean):
        """Step 3: Perform KPI analysis"""
        logger.info("="*70)
        logger.info("STEP 3: KPI ANALYSIS AND CALCULATIONS")
        logger.info("="*70)
        
        # Get all rankings
        rankings = self.analyzer.get_all_rankings(df_clean)
        
        # Log sample results
        logger.info("\nSample Rankings:")
        logger.info("-" * 50)
        for name, df in rankings.items():
            logger.info(f"\n{name.upper()}:")
            df.show(5, truncate=False)
        
        # Advanced searches
        searches = self.analyzer.advanced_searches(df_clean)
        logger.info("\nAdvanced Search Results:")
        logger.info("-" * 50)
        for name, df in searches.items():
            logger.info(f"\n{name.upper()}:")
            df.show(truncate=False)
        
        # Franchise analysis
        franchise_comp = self.analyzer.franchise_vs_standalone(df_clean)
        logger.info("\nFranchise vs Standalone Comparison:")
        logger.info("-" * 50)
        franchise_comp.show(truncate=False)
        
        # Top franchises
        top_franchises = self.analyzer.top_franchises(df_clean, top_n=10)
        logger.info("\nTop 10 Franchises:")
        logger.info("-" * 50)
        top_franchises.show(truncate=False)
        
        # Top directors
        top_directors = self.analyzer.top_directors(df_clean, top_n=10)
        logger.info("\nTop 10 Directors:")
        logger.info("-" * 50)
        top_directors.show(truncate=False)
        
        # Save analysis results
        self.save_analysis_results(rankings, searches, franchise_comp, 
                                   top_franchises, top_directors)
        
        return {
            'rankings': rankings,
            'searches': searches,
            'franchise_comp': franchise_comp,
            'top_franchises': top_franchises,
            'top_directors': top_directors
        }
    
    def run_visualization(self, df_clean, analysis_results):
        """Step 4: Generate visualizations"""
        logger.info("="*70)
        logger.info("STEP 4: DATA VISUALIZATION")
        logger.info("="*70)
        
        self.visualizer.generate_all_visualizations(
            df_clean,
            analysis_results['franchise_comp'],
            analysis_results['top_franchises']
        )
    
    def save_analysis_results(self, rankings, searches, franchise_comp, 
                             top_franchises, top_directors):
        """Save analysis results to disk"""
        logger.info("Saving analysis results...")
        
        # Save rankings
        for name, df in rankings.items():
            df.write.mode('overwrite').parquet(f'data/analysis/ranking_{name}.parquet')
        
        # Save other analyses
        franchise_comp.write.mode('overwrite').parquet('data/analysis/franchise_comparison.parquet')
        top_franchises.write.mode('overwrite').parquet('data/analysis/top_franchises.parquet')
        top_directors.write.mode('overwrite').parquet('data/analysis/top_directors.parquet')
        
        logger.info("All analysis results saved")
    
    def run_full_pipeline(self):
        """Execute complete pipeline end-to-end"""
        try:
            logger.info("\n" + "="*70)
            logger.info(" TMDB MOVIE ANALYSIS PIPELINE - PYSPARK IMPLEMENTATION")
            logger.info("="*70 + "\n")
            
            # Step 1: Extract
            raw_data = self.run_extraction()
            
            # Step 2: Clean
            df_clean = self.run_cleaning(raw_data)
            
            # Step 3: Analyze
            analysis_results = self.run_analysis(df_clean)
            
            # Step 4: Visualize
            self.run_visualization(df_clean, analysis_results)
            
            logger.info("\n" + "="*70)
            logger.info(" PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("="*70)
            logger.info("\nOutputs:")
            logger.info("  - Raw data: data/raw/movies_raw.json")
            logger.info("  - Cleaned data: data/processed/movies_clean.parquet")
            logger.info("  - Analysis results: data/analysis/")
            logger.info("  - Visualizations: visualizations/")
            logger.info("="*70 + "\n")
            
            return df_clean, analysis_results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            raise
        
        finally:
            # Clean up
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Entry point for command-line execution"""
    
    # Configuration - Load from new_config.py
    API_KEY = TMDB_API_KEY_CONFIG
    MOVIE_IDS = MOVIE_IDS_CONFIG
    
    # Run pipeline
    pipeline = TMDBPipeline(API_KEY, MOVIE_IDS)
    df_clean, analysis_results = pipeline.run_full_pipeline()
    
    return df_clean, analysis_results


if __name__ == "__main__":
    main()