"""
Scripts/data_extraction.py
Data Extraction Module - Handles all TMDB API interactions
"""

import logging
import requests
import json
import sys
import tempfile
from pathlib import Path
from typing import List, Dict, Optional, Set
from functools import wraps
import time

# Add parent directory to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent))

from new_config import TMDB_API_KEY, TMDB_BASE_URL, API_CONFIG

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Custom exception for extraction failures"""
    pass


class TMDBExtractor:
    """Handles TMDB API data extraction"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key if api_key else TMDB_API_KEY
        self.base_url = TMDB_BASE_URL
        self.session = requests.Session()
        self._extracted_ids: Set[int] = set()
        
        if not self.api_key or self.api_key == 'YOUR_API_KEY_HERE':
            logger.error("API key not configured!")
            raise ValueError("Invalid API key. Update new_config.py or set TMDB_API_KEY environment variable.")
    
    @staticmethod
    def retry_on_failure(max_retries: int = None, delay: int = None):
        """Decorator for API retry logic"""
        max_retries = max_retries or API_CONFIG['max_retries']
        delay = delay or API_CONFIG['retry_delay']
        
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
    
    @retry_on_failure()
    def fetch_movie_with_credits(self, movie_id: int) -> Optional[Dict]:
        """Fetch movie details + credits in single API call"""
        if movie_id <= 0:
            logger.warning(f"Invalid movie ID: {movie_id}")
            return None
        
        endpoint = f"{self.base_url}/movie/{movie_id}"
        params = {
            'api_key': self.api_key,
            'append_to_response': 'credits'
        }
        
        response = self.session.get(
            endpoint, 
            params=params, 
            timeout=API_CONFIG['request_timeout']
        )
        
        if response.status_code == 404:
            logger.warning(f"Movie ID {movie_id} not found in TMDB")
            return None
        
        response.raise_for_status()
        data = response.json()
        
        if 'credits' in data:
            data['cast'] = data['credits'].get('cast', [])
            data['crew'] = data['credits'].get('crew', [])
            del data['credits']
        
        return data
    
    def extract_movies(
        self, 
        movie_ids: List[int], 
        save_path: Optional[str] = None,
        skip_existing: bool = True
    ) -> List[Dict]:
        """Extract movies from API with validation and atomic writes"""
        # Validate input
        if not movie_ids:
            raise ExtractionError("No movie IDs provided for extraction")
        
        # Deduplicate and filter invalid IDs
        unique_ids = list(dict.fromkeys(movie_ids))
        valid_ids = [mid for mid in unique_ids if mid > 0]
        
        if len(valid_ids) < len(movie_ids):
            logger.info(f"Filtered {len(movie_ids) - len(valid_ids)} invalid/duplicate IDs")
        
        if not valid_ids:
            raise ExtractionError(f"No valid movie IDs after filtering from {len(movie_ids)} input IDs")
        
        logger.info(f"Starting extraction for {len(valid_ids)} movies...")
        logger.info(f"Using API key: {self.api_key[:8]}...")
        
        movies = []
        existing_ids: Set[int] = set()
        failed_ids = []
        
        for idx, movie_id in enumerate(valid_ids, 1):
            if movie_id in existing_ids:
                continue
                
            logger.info(f"Fetching movie {idx}/{len(valid_ids)}: ID {movie_id}")
            try:
                movie_data = self.fetch_movie_with_credits(movie_id)
                
                if movie_data:
                    fetched_id = movie_data.get('id')
                    if fetched_id is None:
                        logger.warning(f"  Extracted data missing 'id' field")
                        failed_ids.append(movie_id)
                        continue
                    
                    if fetched_id not in existing_ids:
                        movies.append(movie_data)
                        existing_ids.add(fetched_id)
                        logger.info(f"  Got: {movie_data.get('title', 'Unknown')}")
                else:
                    logger.warning(f"  No data returned for ID {movie_id}")
                    failed_ids.append(movie_id)
            except Exception as e:
                logger.warning(f"  Error fetching ID {movie_id}: {str(e)}")
                failed_ids.append(movie_id)
            
            time.sleep(API_CONFIG['rate_limit_delay'])
        
        success_rate = len(movies) / len(valid_ids) * 100
        logger.info(f"Successfully extracted {len(movies)}/{len(valid_ids)} movies ({success_rate:.1f}%)")
        
        if not movies:
            raise ExtractionError(f"Failed to extract any movies. Failed IDs: {failed_ids}")
        
        if failed_ids:
            logger.warning(f"Failed to extract {len(failed_ids)} movies: {failed_ids}")
        
        if save_path:
            self._save_raw_data_atomic(movies, save_path)
        
        return movies
    
    def _save_raw_data_atomic(self, movies: List[Dict], save_path: str):
        """Save raw API response to JSON with atomic write (temp file + rename)"""
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temporary file first (atomic write pattern)
        temp_dir = Path(save_path).parent
        with tempfile.NamedTemporaryFile(
            mode='w', 
            dir=temp_dir, 
            encoding='utf-8', 
            delete=False, 
            suffix='.tmp'
        ) as tmp:
            try:
                json.dump(movies, tmp, indent=2, ensure_ascii=False)
                tmp.flush()
                tmp_path = tmp.name
            except Exception as e:
                logger.error(f"Failed to write to temporary file: {str(e)}")
                raise ExtractionError(f"Atomic write failed: {str(e)}")
        
        # Atomic rename
        try:
            Path(tmp_path).replace(save_path)
            logger.info(f"Raw data saved to: {save_path}")
        except Exception as e:
            Path(tmp_path).unlink(missing_ok=True)
            logger.error(f"Failed to rename temp file to {save_path}: {str(e)}")
            raise ExtractionError(f"Atomic write finalization failed: {str(e)}")
    
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
