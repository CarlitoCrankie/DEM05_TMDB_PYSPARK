"""
Visualization Module
Handles all plotting - NO data processing or analysis here
"""

import logging
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pathlib import Path

logger = logging.getLogger(__name__)

sns.set_style("whitegrid")
plt.rcParams['figure.dpi'] = 150
plt.rcParams['figure.figsize'] = (12, 7)


class MovieVisualizer:
    """Handles all data visualizations"""
    
    def __init__(self, output_dir: str = 'visualizations'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Visualizations will be saved to: {self.output_dir}")
    
    def _add_roi_column(self, df: DataFrame) -> DataFrame:
        """Add ROI column if it doesn't exist"""
        if 'roi' not in df.columns:
            df = df.withColumn(
                'roi',
                when(
                    col('budget_musd').isNotNull() & 
                    col('revenue_musd').isNotNull() & 
                    (col('budget_musd') > 0),
                    col('revenue_musd') / col('budget_musd')
                ).otherwise(None)
            )
        return df
    
    def plot_revenue_vs_budget(self, df: DataFrame):
        """Plot revenue vs budget scatter"""
        logger.info("Creating Revenue vs Budget visualization...")
        
        pdf = (df
               .select('budget_musd', 'revenue_musd', 'title')
               .filter(col('budget_musd').isNotNull() & col('revenue_musd').isNotNull())
               .toPandas())
        
        if pdf.empty:
            logger.warning("No data available for revenue vs budget plot")
            return
        
        plt.figure(figsize=(12, 7))
        plt.scatter(pdf['budget_musd'], pdf['revenue_musd'], 
                   alpha=0.6, s=100, edgecolors='black', linewidth=0.5)
        
        plt.xlabel('Budget (Million USD)', fontsize=12, fontweight='bold')
        plt.ylabel('Revenue (Million USD)', fontsize=12, fontweight='bold')
        plt.title('Movie Revenue vs Budget Analysis', fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3)
        
        max_val = max(pdf['budget_musd'].max(), pdf['revenue_musd'].max())
        if max_val > 0:
            plt.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Break-even')
            plt.legend()
        
        plt.tight_layout()
        save_path = self.output_dir / 'revenue_vs_budget.png'
        plt.savefig(save_path, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {save_path}")
    
    def plot_roi_distribution(self, df: DataFrame):
        """Plot ROI distribution"""
        logger.info("Creating ROI distribution visualization...")
        
        # Add ROI column if missing
        df = self._add_roi_column(df)
        
        pdf = (df
               .select('roi')
               .filter(col('roi').isNotNull() & (col('roi') > 0) & (col('roi') < 100))
               .toPandas())
        
        if pdf.empty or len(pdf) < 2:
            logger.warning("Not enough data for ROI distribution plot")
            return
        
        plt.figure(figsize=(12, 6))
        plt.hist(pdf['roi'], bins=min(30, len(pdf)), edgecolor='black', alpha=0.7)
        plt.xlabel('ROI (Revenue / Budget)', fontsize=12, fontweight='bold')
        plt.ylabel('Frequency', fontsize=12, fontweight='bold')
        plt.title('Distribution of Movie ROI', fontsize=14, fontweight='bold')
        
        median_roi = pdf['roi'].median()
        plt.axvline(median_roi, color='r', linestyle='--', 
                   label=f'Median: {median_roi:.2f}')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        save_path = self.output_dir / 'roi_distribution.png'
        plt.savefig(save_path, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {save_path}")
    
    def plot_franchise_comparison(self, franchise_df: DataFrame):
        """Plot franchise vs standalone comparison"""
        logger.info("Creating franchise comparison visualization...")
        
        pdf = franchise_df.toPandas()
        
        if pdf.empty:
            logger.warning("No data available for franchise comparison plot")
            return
        
        # Find available metric columns
        possible_metrics = {
            'avg_revenue_musd': 'Avg Revenue (M USD)',
            'avg_budget_musd': 'Avg Budget (M USD)', 
            'avg_popularity': 'Avg Popularity',
            'avg_rating': 'Avg Rating',
            'mean_revenue': 'Mean Revenue (M USD)',
            'mean_budget': 'Mean Budget (M USD)',
            'mean_popularity': 'Mean Popularity',
            'mean_rating': 'Mean Rating'
        }
        
        available_metrics = []
        available_titles = []
        for metric, title in possible_metrics.items():
            if metric in pdf.columns:
                available_metrics.append(metric)
                available_titles.append(title)
        
        if not available_metrics:
            logger.warning("No metrics available for franchise comparison")
            return
        
        n_metrics = len(available_metrics)
        n_cols = min(2, n_metrics)
        n_rows = (n_metrics + 1) // 2
        
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(7 * n_cols, 5 * n_rows))
        if n_metrics == 1:
            axes = [axes]
        elif n_metrics > 1:
            axes = axes.ravel()
        
        colors = ['#2ecc71', '#3498db']
        
        for idx, (metric, title) in enumerate(zip(available_metrics, available_titles)):
            ax = axes[idx]
            bars = ax.bar(pdf['category'], pdf[metric], 
                         color=colors[:len(pdf)], edgecolor='black', linewidth=1.5)
            ax.set_title(title, fontsize=12, fontweight='bold')
            ax.set_ylabel('Value', fontsize=10)
            ax.grid(True, alpha=0.3, axis='y')
            
            for i, v in enumerate(pdf[metric]):
                if pd.notna(v):
                    ax.text(i, v, f'{v:.2f}', ha='center', va='bottom', fontsize=9)
        
        for idx in range(len(available_metrics), len(axes)):
            axes[idx].set_visible(False)
        
        plt.suptitle('Franchise vs Standalone Movie Performance', 
                    fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        save_path = self.output_dir / 'franchise_comparison.png'
        plt.savefig(save_path, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {save_path}")
    
    def plot_top_franchises(self, top_franchises_df: DataFrame):
        """Plot top franchises by revenue"""
        logger.info("Creating top franchises visualization...")
        
        pdf = top_franchises_df.toPandas()
        
        if pdf.empty:
            logger.warning("No data available for top franchises plot")
            return
        
        # Find revenue and rating columns
        revenue_col = None
        rating_col = None
        
        for col_name in ['total_revenue_musd', 'total_revenue']:
            if col_name in pdf.columns:
                revenue_col = col_name
                break
        
        for col_name in ['avg_rating', 'mean_rating']:
            if col_name in pdf.columns:
                rating_col = col_name
                break
        
        if not revenue_col and not rating_col:
            logger.warning("No revenue or rating columns found")
            return
        
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        
        if revenue_col and revenue_col in pdf.columns:
            axes[0].barh(pdf['belongs_to_collection'], pdf[revenue_col], 
                    color='#e74c3c', edgecolor='black')
            axes[0].set_xlabel('Total Revenue (M USD)', fontsize=11, fontweight='bold')
            axes[0].set_title('Top Franchises by Total Revenue', fontsize=12, fontweight='bold')
            axes[0].grid(True, alpha=0.3, axis='x')
        else:
            axes[0].text(0.5, 0.5, 'No revenue data', ha='center', va='center')
        
        if rating_col and rating_col in pdf.columns:
            axes[1].barh(pdf['belongs_to_collection'], pdf[rating_col], 
                    color='#f39c12', edgecolor='black')
            axes[1].set_xlabel('Average Rating', fontsize=11, fontweight='bold')
            axes[1].set_title('Top Franchises by Average Rating', fontsize=12, fontweight='bold')
            axes[1].grid(True, alpha=0.3, axis='x')
        else:
            axes[1].text(0.5, 0.5, 'No rating data', ha='center', va='center')
        
        plt.tight_layout()
        save_path = self.output_dir / 'top_franchises.png'
        plt.savefig(save_path, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {save_path}")
    
    def plot_popularity_vs_rating(self, df: DataFrame):
        """Plot popularity vs rating scatter"""
        logger.info("Creating Popularity vs Rating visualization...")
        
        pdf = (df
               .select('popularity', 'vote_average', 'title')
               .filter(col('popularity').isNotNull() & col('vote_average').isNotNull())
               .toPandas())
        
        if pdf.empty:
            logger.warning("No data available for popularity vs rating plot")
            return
        
        plt.figure(figsize=(12, 7))
        scatter = plt.scatter(pdf['vote_average'], pdf['popularity'], 
                            alpha=0.6, s=100, c=pdf['vote_average'],
                            cmap='viridis', edgecolors='black', linewidth=0.5)
        
        plt.xlabel('Average Rating', fontsize=12, fontweight='bold')
        plt.ylabel('Popularity Score', fontsize=12, fontweight='bold')
        plt.title('Movie Popularity vs Rating', fontsize=14, fontweight='bold')
        plt.colorbar(scatter, label='Rating')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        save_path = self.output_dir / 'popularity_vs_rating.png'
        plt.savefig(save_path, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {save_path}")
    
    def generate_all_visualizations(self, df: DataFrame, 
                                    franchise_comp: DataFrame,
                                    top_franchises: DataFrame):
        """Generate all visualizations"""
        logger.info("="*60)
        logger.info("Generating All Visualizations")
        logger.info("="*60)
        
        try:
            self.plot_revenue_vs_budget(df)
        except Exception as e:
            logger.error(f"Failed to create revenue vs budget plot: {e}")
        
        try:
            self.plot_roi_distribution(df)
        except Exception as e:
            logger.error(f"Failed to create ROI distribution plot: {e}")
        
        try:
            self.plot_popularity_vs_rating(df)
        except Exception as e:
            logger.error(f"Failed to create popularity vs rating plot: {e}")
        
        try:
            self.plot_franchise_comparison(franchise_comp)
        except Exception as e:
            logger.error(f"Failed to create franchise comparison plot: {e}")
        
        try:
            self.plot_top_franchises(top_franchises)
        except Exception as e:
            logger.error(f"Failed to create top franchises plot: {e}")
        
        logger.info("="*60)
        logger.info("Visualization generation complete!")
        logger.info("="*60)
