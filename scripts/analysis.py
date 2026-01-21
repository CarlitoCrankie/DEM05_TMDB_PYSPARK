"""
Analysis Module
Handles all KPI calculations and business logic - NO data cleaning here
"""

import logging
import sys
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, desc, asc, count, avg, sum as spark_sum,
    when, round as spark_round
)
from typing import Dict

# Add parent directory to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent))

from new_config import ANALYSIS_CONFIG

logger = logging.getLogger(__name__)


class MovieAnalyzer:
    """Implements all KPI calculations and analytical queries"""
    
    def __init__(self):
        self.config = ANALYSIS_CONFIG
    
    def add_calculated_metrics(self, df: DataFrame) -> DataFrame:
        """Add derived metrics for analysis"""
        logger.info("Adding calculated metrics...")
        
        df = df.withColumn(
            'profit',
            when(
                col('revenue_musd').isNotNull() & col('budget_musd').isNotNull(),
                col('revenue_musd') - col('budget_musd')
            ).otherwise(None)
        )
        
        df = df.withColumn(
            'roi',
            when(
                col('revenue_musd').isNotNull() & 
                col('budget_musd').isNotNull() & 
                (col('budget_musd') > 0),
                col('revenue_musd') / col('budget_musd')
            ).otherwise(None)
        )
        
        return df
    
    def rank_by_metric(self, df: DataFrame, metric: str, 
                       ascending: bool = False, 
                       filter_expr=None,
                       top_n: int = None) -> DataFrame:
        """Generic ranking function for all ranking operations"""
        top_n = top_n or self.config['top_n_results']
        result = df
        
        # Filter out nulls for the metric being ranked
        result = result.filter(col(metric).isNotNull())
        
        # Apply additional filter if provided
        if filter_expr is not None:
            result = result.filter(filter_expr)
        
        # Sort and select top N
        order_col = asc(metric) if ascending else desc(metric)
        result = result.orderBy(order_col).limit(top_n)
        
        # Select relevant columns
        return result.select('title', metric, 'release_date', 'genres')
    
    def get_all_rankings(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Calculate all movie rankings as per requirements"""
        logger.info("Calculating movie rankings...")
        
        df = self.add_calculated_metrics(df)
        min_budget = self.config['min_budget_for_roi']
        min_votes = self.config['min_votes_for_rating']
        
        rankings = {
            'highest_revenue': self.rank_by_metric(df, 'revenue_musd'),
            
            'highest_budget': self.rank_by_metric(df, 'budget_musd'),
            
            'highest_profit': self.rank_by_metric(df, 'profit'),
            
            'lowest_profit': self.rank_by_metric(df, 'profit', ascending=True),
            
            'highest_roi': self.rank_by_metric(
                df, 'roi',
                filter_expr=(col('budget_musd').isNotNull() & (col('budget_musd') >= min_budget))
            ),
            
            'lowest_roi': self.rank_by_metric(
                df, 'roi',
                ascending=True,
                filter_expr=(col('budget_musd').isNotNull() & (col('budget_musd') >= min_budget))
            ),
            
            'most_voted': self.rank_by_metric(df, 'vote_count'),
            
            'highest_rated': self.rank_by_metric(
                df, 'vote_average',
                filter_expr=(col('vote_count').isNotNull() & (col('vote_count') >= min_votes))
            ),
            
            'lowest_rated': self.rank_by_metric(
                df, 'vote_average',
                ascending=True,
                filter_expr=(col('vote_count').isNotNull() & (col('vote_count') >= min_votes))
            ),
            
            'most_popular': self.rank_by_metric(df, 'popularity')
        }
        
        logger.info(f"Calculated {len(rankings)} different rankings")
        return rankings
    
    def advanced_searches(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Execute advanced search queries"""
        logger.info("Executing advanced search queries...")
        
        searches = {}
        
        # Search 1: Best-rated Sci-Fi Action with Bruce Willis
        bruce_willis_filter = (
            col('genres').isNotNull() &
            col('cast').isNotNull() &
            col('genres').contains('Science Fiction') &
            col('genres').contains('Action') &
            col('cast').contains('Bruce Willis')
        )
        
        searches['bruce_willis_scifi_action'] = (
            df.filter(bruce_willis_filter)
            .orderBy(desc('vote_average'))
            .select('title', 'vote_average', 'genres', 'cast')
        )
        
        # Search 2: Uma Thurman + Quentin Tarantino (shortest to longest)
        thurman_tarantino_filter = (
            col('cast').isNotNull() &
            col('director').isNotNull() &
            col('cast').contains('Uma Thurman') &
            col('director').contains('Quentin Tarantino')
        )
        
        searches['thurman_tarantino'] = (
            df.filter(thurman_tarantino_filter)
            .orderBy(asc('runtime'))
            .select('title', 'runtime', 'director', 'cast')
        )
        
        logger.info(f"Completed {len(searches)} advanced searches")
        return searches
    
    def franchise_vs_standalone(self, df: DataFrame) -> DataFrame:
        """Compare franchise vs standalone movie performance"""
        logger.info("Analyzing franchise vs standalone performance...")
        
        df = self.add_calculated_metrics(df)
        
        df_categorized = df.withColumn(
            'category',
            when(col('belongs_to_collection').isNotNull(), 'Franchise')
            .otherwise('Standalone')
        )
        
        results = df_categorized.groupBy('category').agg(
            count('*').alias('movie_count'),
            spark_round(avg('revenue_musd'), 2).alias('mean_revenue'),
            spark_round(avg('budget_musd'), 2).alias('mean_budget'),
            spark_round(avg('profit'), 2).alias('mean_profit'),
            spark_round(avg('roi'), 2).alias('median_roi'),
            spark_round(avg('popularity'), 2).alias('mean_popularity'),
            spark_round(avg('vote_average'), 2).alias('mean_rating')
        )
        
        return results
    
    def top_franchises(self, df: DataFrame, top_n: int = None) -> DataFrame:
        """Analyze most successful franchises"""
        top_n = top_n or self.config['top_n_results']
        logger.info(f"Analyzing top {top_n} franchises...")
        
        df = self.add_calculated_metrics(df)
        
        franchise_df = df.filter(col('belongs_to_collection').isNotNull())
        
        results = franchise_df.groupBy('belongs_to_collection').agg(
            count('*').alias('total_movies'),
            spark_round(spark_sum('budget_musd'), 2).alias('total_budget'),
            spark_round(avg('budget_musd'), 2).alias('mean_budget'),
            spark_round(spark_sum('revenue_musd'), 2).alias('total_revenue'),
            spark_round(avg('revenue_musd'), 2).alias('mean_revenue'),
            spark_round(avg('vote_average'), 2).alias('mean_rating')
        ).orderBy(desc('total_revenue')).limit(top_n)
        
        return results
    
    def top_directors(self, df: DataFrame, top_n: int = None) -> DataFrame:
        """Analyze most successful directors"""
        top_n = top_n or self.config['top_n_results']
        logger.info(f"Analyzing top {top_n} directors...")
        
        df = self.add_calculated_metrics(df)
        
        director_df = df.filter(
            col('director').isNotNull() & 
            (col('director') != '')
        )
        
        results = director_df.groupBy('director').agg(
            count('*').alias('total_movies'),
            spark_round(spark_sum('revenue_musd'), 2).alias('total_revenue'),
            spark_round(avg('revenue_musd'), 2).alias('avg_revenue'),
            spark_round(avg('vote_average'), 2).alias('mean_rating')
        ).orderBy(desc('total_revenue')).limit(top_n)
        
        return results
    
    def genre_analysis(self, df: DataFrame) -> DataFrame:
        """Analyze performance by genre"""
        logger.info("Analyzing genre performance...")
        
        df = self.add_calculated_metrics(df)
        
        # Filter movies with genres
        genre_df = df.filter(col('genres').isNotNull())
        
        results = genre_df.groupBy('genres').agg(
            count('*').alias('movie_count'),
            spark_round(avg('revenue_musd'), 2).alias('avg_revenue'),
            spark_round(avg('budget_musd'), 2).alias('avg_budget'),
            spark_round(avg('vote_average'), 2).alias('avg_rating'),
            spark_round(avg('popularity'), 2).alias('avg_popularity')
        ).orderBy(desc('avg_revenue')).limit(self.config['top_n_results'])
        
        return results
