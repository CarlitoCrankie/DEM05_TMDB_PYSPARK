"""
Scripts/data_extraction.py
Data Extraction Module - PySpark Version
Handles all TMDB API interactions - NO data processing here
"""

import logging
import requests
import json
import os
import importlib.util
from typing import List, Dict, Optional, Set
from functools import wraps
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config():
    """Load config, handling both local and Docker environments"""
    # Try environment variable first (Docker)
    api_key = os.environ.get('TMDB_API_KEY')
    if api_key:
        return api_key
    
    # Fall back to config file
    config_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'new_config.py')
    )
    
    if os.path.exists(config_path):
        spec = importlib.util.spec_from_file_location("new_config", config_path)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        return config.TMDB_API_KEY
    
    return None


TMDB_API_KEY = load_config()


class TMDBExtractor:
    """Handles TMDB API data extraction only"""
    
    def __init__(self, api_key: str = None, base_url: str = "https://api.themoviedb.org/3"):
        self.api_key = api_key if api_key else TMDB_API_KEY
        self.base_url = base_url
        self.session = requests.Session()
        self._extracted_ids: Set[int] = set()
        
        if not self.api_key or self.api_key in ("YOUR_API_KEY_HERE", "API_KEY_HERE"):
            logger.error("API key not configured! Set TMDB_API_KEY environment variable or update new_config.py")
            raise ValueError("Invalid API key")
    
    @staticmethod
    def retry_on_failure(max_retries: int = 3, delay: int = 2):
        """Decorator for API retry logic"""
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                for attempt in range(max_retries):
                    try:
                        return func(self, *args, **kwargs)
                    except requests.RequestException as e:
                        if attempt == max_retries - 1:
                            logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                            return None
                        logger.warning(f"Attempt {attempt + 1} failed. Retrying in {delay}s...")
                        time.sleep(delay)
                return None
            return wrapper
        return decorator
    
    @retry_on_failure(max_retries=3, delay=2)
    def fetch_movie_with_credits(self, movie_id: int) -> Optional[Dict]:
        """
        Fetch movie details + credits in single API call.
        Returns None for invalid movie IDs (e.g., 0).
        """
        if movie_id <= 0:
            logger.warning(f"Invalid movie ID: {movie_id}")
            return None
        
        endpoint = f"{self.base_url}/movie/{movie_id}"
        params = {
            'api_key': self.api_key,
            'append_to_response': 'credits'
        }
        
        response = self.session.get(endpoint, params=params, timeout=10)
        
        # Handle 404 for non-existent movies
        if response.status_code == 404:
            logger.warning(f"Movie ID {movie_id} not found in TMDB")
            return None
        
        response.raise_for_status()
        data = response.json()
        
        # Flatten credits into main response
        if 'credits' in data:
            data['cast'] = data['credits'].get('cast', [])
            data['crew'] = data['credits'].get('crew', [])
            del data['credits']
        
        return data
    
    def load_existing_data(self, save_path: str) -> List[Dict]:
        """Load previously extracted data to avoid re-fetching"""
        if os.path.exists(save_path):
            try:
                with open(save_path, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
                    logger.info(f"Loaded {len(existing)} existing movies from {save_path}")
                    return existing
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load existing data: {e}")
        return []
    
    def extract_movies(
        self, 
        movie_ids: List[int], 
        save_path: Optional[str] = None,
        skip_existing: bool = True
    ) -> List[Dict]:
        """
        Extract movies from API.
        
        Args:
            movie_ids: List of TMDB movie IDs to fetch
            save_path: Path to save JSON output
            skip_existing: If True and save_path exists, skip already-fetched movies
        
        Returns:
            List of movie data dictionaries
        """
        # Deduplicate input IDs
        unique_ids = list(dict.fromkeys(movie_ids))  # Preserves order
        if len(unique_ids) < len(movie_ids):
            logger.info(f"Removed {len(movie_ids) - len(unique_ids)} duplicate IDs from input")
        
        # Filter out invalid IDs
        valid_ids = [mid for mid in unique_ids if mid > 0]
        if len(valid_ids) < len(unique_ids):
            logger.info(f"Filtered out {len(unique_ids) - len(valid_ids)} invalid IDs (<=0)")
        
        # Load existing data if available
        movies = []
        existing_movie_ids: Set[int] = set()
        
        if skip_existing and save_path:
            existing_movies = self.load_existing_data(save_path)
            for movie in existing_movies:
                movie_id = movie.get('id')
                if movie_id and movie_id not in existing_movie_ids:
                    movies.append(movie)
                    existing_movie_ids.add(movie_id)
        
        # Determine which IDs still need fetching
        ids_to_fetch = [mid for mid in valid_ids if mid not in existing_movie_ids]
        
        if not ids_to_fetch:
            logger.info("All requested movies already extracted")
            return movies
        
        logger.info(f"Fetching {len(ids_to_fetch)} new movies ({len(existing_movie_ids)} already cached)")
        logger.info(f"Using API key: {self.api_key[:8]}...")
        
        for idx, movie_id in enumerate(ids_to_fetch, 1):
            logger.info(f"Fetching movie {idx}/{len(ids_to_fetch)}: ID {movie_id}")
            movie_data = self.fetch_movie_with_credits(movie_id)
            
            if movie_data:
                fetched_id = movie_data.get('id')
                # Double-check we don't have this movie already
                if fetched_id not in existing_movie_ids:
                    movies.append(movie_data)
                    existing_movie_ids.add(fetched_id)
                    logger.info(f"  Got: {movie_data.get('title', 'Unknown')}")
                else:
                    logger.info(f"  Skipped duplicate: {movie_data.get('title', 'Unknown')}")
            else:
                logger.warning(f"  No data returned for ID {movie_id}")
            
            time.sleep(0.25)
        
        logger.info(f"Total movies: {len(movies)}")
        
        if save_path:
            self._save_raw_data(movies, save_path)
        
        return movies
    
    def _save_raw_data(self, movies: List[Dict], save_path: str):
        """Save raw API response to JSON"""
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(movies, f, indent=2, ensure_ascii=False)
        logger.info(f"Raw data saved to: {save_path}")


def test_api_connection():
    """Test if API key is working"""
    print("Testing TMDB API connection...")
    
    try:
        extractor = TMDBExtractor()
        test_movie = extractor.fetch_movie_with_credits(299534)
        
        if test_movie:
            print(f"API connection successful!")
            print(f"  Test movie: {test_movie.get('title')}")
            return True
        else:
            print("API test failed - no data returned")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False


if __name__ == "__main__":
    test_api_connection()
