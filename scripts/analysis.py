"""
Analysis Module
Handles all KPI calculations and business logic - NO data cleaning here
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, desc, asc, count, mean, sum as spark_sum, 
    expr, when, lit
)
from typing import Dict

logger = logging.getLogger(__name__)

class MovieAnalyzer:
    """Implements all KPI calculations and analytical queries"""
    
    def __init__(self):
        pass
    
    def add_calculated_metrics(self, df: DataFrame) -> DataFrame:
        """Add derived metrics for analysis"""
        logger.info("Adding calculated metrics...")
        
        df = (df
              .withColumn('profit', col('revenue_musd') - col('budget_musd'))
              .withColumn('roi', col('revenue_musd') / col('budget_musd'))
        )
        
        return df
    
    def rank_by_metric(self, df: DataFrame, metric: str, 
                       ascending: bool = False, 
                       filter_expr = None,
                       top_n: int = 10) -> DataFrame:
        """
        Generic ranking UDF
        Reusable function for all ranking operations
        """
        result = df
        
        # Apply filter if provided
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
        
        rankings = {
            'highest_revenue': self.rank_by_metric(df, 'revenue_musd'),
            
            'highest_budget': self.rank_by_metric(df, 'budget_musd'),
            
            'highest_profit': self.rank_by_metric(df, 'profit'),
            
            'lowest_profit': self.rank_by_metric(df, 'profit', ascending=True),
            
            'highest_roi': self.rank_by_metric(
                df, 'roi', 
                filter_expr=(col('budget_musd') >= 10)
            ),
            
            'lowest_roi': self.rank_by_metric(
                df, 'roi', 
                ascending=True,
                filter_expr=(col('budget_musd') >= 10)
            ),
            
            'most_voted': self.rank_by_metric(df, 'vote_count'),
            
            'highest_rated': self.rank_by_metric(
                df, 'vote_average',
                filter_expr=(col('vote_count') >= 10)
            ),
            
            'lowest_rated': self.rank_by_metric(
                df, 'vote_average',
                ascending=True,
                filter_expr=(col('vote_count') >= 10)
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
        searches['bruce_willis_scifi_action'] = (
            df.filter(
                col('genres').contains('Science Fiction') &
                col('genres').contains('Action') &
                col('cast').contains('Bruce Willis')
            )
            .orderBy(desc('vote_average'))
            .select('title', 'vote_average', 'genres', 'cast')
        )
        
        # Search 2: Uma Thurman + Quentin Tarantino (shortest to longest)
        searches['thurman_tarantino'] = (
            df.filter(
                col('cast').contains('Uma Thurman') &
                col('director').contains('Quentin Tarantino')
            )
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
            mean('revenue_musd').alias('mean_revenue'),
            expr('percentile_approx(roi, 0.5)').alias('median_roi'),
            mean('budget_musd').alias('mean_budget'),
            mean('popularity').alias('mean_popularity'),
            mean('vote_average').alias('mean_rating')
        )
        
        return results
    
    def top_franchises(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """Analyze most successful franchises"""
        logger.info("Analyzing top franchises...")
        
        df = self.add_calculated_metrics(df)
        
        franchise_df = df.filter(col('belongs_to_collection').isNotNull())
        
        results = franchise_df.groupBy('belongs_to_collection').agg(
            count('*').alias('total_movies'),
            spark_sum('budget_musd').alias('total_budget'),
            mean('budget_musd').alias('mean_budget'),
            spark_sum('revenue_musd').alias('total_revenue'),
            mean('revenue_musd').alias('mean_revenue'),
            mean('vote_average').alias('mean_rating')
        ).orderBy(desc('total_revenue')).limit(top_n)
        
        return results
    
    def top_directors(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """Analyze most successful directors"""
        logger.info("Analyzing top directors...")
        
        df = self.add_calculated_metrics(df)
        
        director_df = df.filter(col('director').isNotNull())
        
        results = director_df.groupBy('director').agg(
            count('*').alias('total_movies'),
            spark_sum('revenue_musd').alias('total_revenue'),
            mean('vote_average').alias('mean_rating')
        ).orderBy(desc('total_revenue')).limit(top_n)
        
        return results

