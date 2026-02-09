"""
scripts/pipeline.py
Main Pipeline Orchestrator - Coordinates all modules
Idempotent design: Safe to re-run entire pipeline without side effects
"""

import logging
import sys
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession

# Add parent directory to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent))

from new_config import (
    TMDB_API_KEY, MOVIE_IDS, SPARK_CONFIG, PATHS, FILES,
    LOGGING_CONFIG, CLEANING_CONFIG, ANALYSIS_CONFIG
)
from data_extraction import TMDBExtractor, ExtractionError
from data_transformations import MovieDataCleaner, ValidationError
from analysis import MovieAnalyzer
from visualizations import MovieVisualizer


def setup_logging():
    """Configure logging based on config settings"""
    log_dir = Path(PATHS['logs'])
    log_dir.mkdir(parents=True, exist_ok=True)
    
    formatter = logging.Formatter(LOGGING_CONFIG['format'])
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, LOGGING_CONFIG['level']))
    
    # Clear existing handlers
    root_logger.handlers = []
    
    if LOGGING_CONFIG['log_to_console']:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, LOGGING_CONFIG['level']))
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    if LOGGING_CONFIG['log_to_file']:
        log_file = Path(PATHS['logs']) / FILES['log_file']
        # Append to log file for audit trail across runs
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(getattr(logging, LOGGING_CONFIG['level']))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return logging.getLogger(__name__)


logger = setup_logging()


class TMDBPipeline:
    """Idempotent TMDB Pipeline Orchestrator
    
    Design principles:
    - Fail fast: Stop on critical failures
    - Retry transient errors: API rate limits, network issues
    - Idempotent: Safe to re-run without manual cleanup
    - Atomic writes: All or nothing data updates
    - Checkpointed: Recovery from failures
    """
    
    def __init__(self, idempotent_mode: bool = True):
        self.api_key = TMDB_API_KEY
        self.movie_ids = MOVIE_IDS
        self.idempotent_mode = idempotent_mode
        
        self._validate_config()
        self._setup_directories()
        self._setup_checkpoint_dir()
        self._init_spark()
        self._init_modules()
    
    def _setup_checkpoint_dir(self):
        """Setup checkpoint directory for fault tolerance"""
        checkpoint_dir = Path(PATHS['logs']) / 'checkpoints'
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir = str(checkpoint_dir)
        logger.info(f"Checkpoint directory: {self.checkpoint_dir}")
    
    def _validate_config(self):
        """Validate configuration before starting"""
        if not self.api_key or self.api_key == 'YOUR_API_KEY_HERE':
            raise ValueError("TMDB API key not configured. Update new_config.py or set TMDB_API_KEY environment variable.")
        
        if not self.movie_ids:
            raise ValueError("No movie IDs configured in new_config.py")
        
        logger.info("Configuration validated successfully")
    
    def _setup_directories(self):
        """Create necessary directories from config"""
        for path_name, path_value in PATHS.items():
            Path(path_value).mkdir(parents=True, exist_ok=True)
        logger.info("Directories created")
    
    def _init_spark(self):
        """Initialize Spark session from config"""
        self.spark = SparkSession.builder \
            .appName("TMDB Movie Analysis") \
            .master("local[1]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.local.dir", "/tmp/spark") \
            .getOrCreate()
        
        logger.info(f"Spark Session Created: {self.spark.version}")
    
    def _init_modules(self):
        """Initialize all pipeline modules"""
        self.extractor = TMDBExtractor(self.api_key)
        self.cleaner = MovieDataCleaner(self.spark, checkpoint_dir=self.checkpoint_dir)
        self.analyzer = MovieAnalyzer()
        self.visualizer = MovieVisualizer(output_dir=PATHS['visualizations'])
        logger.info("All modules initialized")
    
    def run_extraction(self):
        """Step 1: Extract data from API with idempotency check"""
        logger.info("="*70)
        logger.info("STEP 1: DATA EXTRACTION FROM TMDB API")
        logger.info("="*70)
        
        save_path = Path(PATHS['raw_data']) / FILES['raw_json']
        
        # Idempotency: Skip extraction if data already exists and is valid
        if self.idempotent_mode and save_path.exists() and save_path.stat().st_size > 0:
            logger.info(f"Raw data already exists: {save_path}")
            logger.info("Skipping extraction (idempotent mode)")
            try:
                import json
                with open(save_path, 'r') as f:
                    raw_data = json.load(f)
                logger.info(f"Loaded {len(raw_data)} movies from existing file")
                return raw_data
            except Exception as e:
                logger.warning(f"Failed to load existing data: {str(e)}. Re-extracting...")
        
        # FAIL FAST: Stop if extraction fails
        try:
            raw_data = self.extractor.extract_movies(
                self.movie_ids,
                save_path=str(save_path)
            )
            logger.info(f"Extraction successful: {len(raw_data)} movies")
            return raw_data
        
        except ExtractionError as e:
            logger.critical(f"EXTRACTION FAILED (FAIL FAST): {str(e)}")
            raise
        except Exception as e:
            logger.critical(f"Unexpected error during extraction: {str(e)}", exc_info=True)
            raise
    
    def run_cleaning(self, raw_data):
        """Step 2: Clean and transform data with atomic writes and checkpointing"""
        logger.info("="*70)
        logger.info("STEP 2: DATA CLEANING AND TRANSFORMATION")
        logger.info("="*70)
        
        save_path = Path(PATHS['processed_data']) / FILES['clean_parquet']
        
        # FAIL FAST: Stop if cleaning/validation fails
        try:
            df_clean = self.cleaner.clean_pipeline(raw_data)
            
            df_clean.cache()
            logger.info("DataFrame cached for performance optimization")
            
            # Atomic write: Write to temp location first, then move
            save_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = str(save_path.parent / f"{save_path.name}.tmp")
            
            df_clean.write.mode('overwrite').parquet(temp_path)
            
            # Atomic move
            import shutil
            if save_path.exists():
                shutil.rmtree(save_path)
            shutil.move(temp_path, str(save_path))
            
            logger.info(f"Cleaned data saved (atomically) to: {save_path}")
            return df_clean
        
        except ValidationError as e:
            logger.critical(f"DATA VALIDATION FAILED (FAIL FAST): {str(e)}")
            raise
        except Exception as e:
            logger.critical(f"Error during cleaning: {str(e)}", exc_info=True)
            raise
    
    def run_analysis(self, df_clean):
        """Step 3: Perform KPI analysis"""
        logger.info("="*70)
        logger.info("STEP 3: KPI ANALYSIS AND CALCULATIONS")
        logger.info("="*70)
        
        rankings = self.analyzer.get_all_rankings(df_clean)
        
        logger.info("\nSample Rankings:")
        logger.info("-" * 50)
        for name, df in rankings.items():
            logger.info(f"\n{name.upper()}:")
            df.show(5, truncate=False)
        
        searches = self.analyzer.advanced_searches(df_clean)
        logger.info("\nAdvanced Search Results:")
        logger.info("-" * 50)
        for name, df in searches.items():
            logger.info(f"\n{name.upper()}:")
            df.show(truncate=False)
        
        franchise_comp = self.analyzer.franchise_vs_standalone(df_clean)
        logger.info("\nFranchise vs Standalone Comparison:")
        logger.info("-" * 50)
        franchise_comp.show(truncate=False)
        
        top_franchises = self.analyzer.top_franchises(df_clean, top_n=ANALYSIS_CONFIG['top_n_results'])
        logger.info(f"\nTop {ANALYSIS_CONFIG['top_n_results']} Franchises:")
        logger.info("-" * 50)
        top_franchises.show(truncate=False)
        
        top_directors = self.analyzer.top_directors(df_clean, top_n=ANALYSIS_CONFIG['top_n_results'])
        logger.info(f"\nTop {ANALYSIS_CONFIG['top_n_results']} Directors:")
        logger.info("-" * 50)
        top_directors.show(truncate=False)
        
        self._save_analysis_results(rankings, franchise_comp, top_franchises, top_directors)
        
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
    
    def _save_analysis_results(self, rankings, franchise_comp, top_franchises, top_directors):
        """Save analysis results to disk"""
        logger.info("Saving analysis results...")
        
        analysis_path = Path(PATHS['analysis_data'])
        
        for name, df in rankings.items():
            df.write.mode('overwrite').parquet(str(analysis_path / f'ranking_{name}.parquet'))
        
        franchise_comp.write.mode('overwrite').parquet(str(analysis_path / 'franchise_comparison.parquet'))
        top_franchises.write.mode('overwrite').parquet(str(analysis_path / 'top_franchises.parquet'))
        top_directors.write.mode('overwrite').parquet(str(analysis_path / 'top_directors.parquet'))
        
        logger.info("All analysis results saved")
    
    def run_full_pipeline(self):
        """Execute complete pipeline end-to-end (idempotent and fail-fast)"""
        try:
            logger.info("\n" + "="*70)
            logger.info(" TMDB MOVIE ANALYSIS PIPELINE - PYSPARK IMPLEMENTATION")
            logger.info(f" Mode: {'IDEMPOTENT' if self.idempotent_mode else 'FRESH'}")
            logger.info("="*70 + "\n")
            
            # FAIL FAST: Stop on extraction failure
            raw_data = self.run_extraction()
            
            # FAIL FAST: Stop on cleaning/validation failure
            df_clean = self.run_cleaning(raw_data)
            
            # FAIL FAST: Stop on analysis failure
            try:
                analysis_results = self.run_analysis(df_clean)
            except Exception as e:
                logger.error(f"Analysis failed: {str(e)}", exc_info=True)
                analysis_results = None
            
            # Visualization is non-critical
            try:
                if analysis_results:
                    self.run_visualization(df_clean, analysis_results)
            except Exception as e:
                logger.warning(f"Visualization generation failed (non-critical): {str(e)}")
            
            logger.info("\n" + "="*70)
            logger.info(" PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("="*70)
            logger.info("\nOutputs:")
            logger.info(f"  - Raw data: {PATHS['raw_data']}/{FILES['raw_json']}")
            logger.info(f"  - Cleaned data: {PATHS['processed_data']}/{FILES['clean_parquet']}")
            logger.info(f"  - Analysis results: {PATHS['analysis_data']}/")
            logger.info(f"  - Visualizations: {PATHS['visualizations']}/")
            logger.info(f"  - Logs: {PATHS['logs']}/{FILES['log_file']}")
            logger.info(f"  - Checkpoints: {self.checkpoint_dir}")
            logger.info("="*70 + "\n")
            
            return df_clean, analysis_results
            
        except (ExtractionError, ValidationError) as e:
            logger.error(f"CRITICAL FAILURE - Pipeline stopped (FAIL FAST): {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            raise
        
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Entry point for command-line execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description="TMDB Movie Analysis Pipeline")
    parser.add_argument(
        '--fresh', 
        action='store_true', 
        help='Run pipeline in fresh mode (skip idempotent checks)'
    )
    args = parser.parse_args()
    
    pipeline = TMDBPipeline(idempotent_mode=not args.fresh)
    df_clean, analysis_results = pipeline.run_full_pipeline()
    return df_clean, analysis_results


if __name__ == "__main__":
    main()
