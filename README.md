# TMDB Movie Analysis Pipeline

## Executive Summary

This project implements a data engineering pipeline for extracting, transforming, and analyzing movie data from The Movie Database (TMDB) API. Built with Apache Spark (PySpark) and containerized using Docker, the pipeline processes movie metadata to generate business insights and visualizations.

## Business Objective

The pipeline answers key business questions about the film industry:

- Which movies generate the highest revenue and profit?
- What is the return on investment (ROI) for different budget levels?
- How do franchise films perform compared to standalone releases?
- Which directors consistently deliver successful films?
- What factors correlate with audience ratings and popularity?

## Technical Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   EXTRACTION    │────▶│ TRANSFORMATION  │────▶│    ANALYSIS     │────▶│ VISUALIZATION   │
│   (TMDB API)    │     │   (PySpark)     │     │   (PySpark)     │     │  (Matplotlib)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │                       │
        ▼                       ▼                       ▼                       ▼
   movies_raw.json      movies_clean.parquet    analysis/*.parquet    visualizations/*.png
```

## Technology Stack

| Component | Technology |
|-----------|------------|
| Data Processing | Apache Spark 3.5.0 (PySpark) |
| Runtime | Python 3.11 |
| Containerization | Docker |
| Data Source | TMDB REST API |
| Storage Format | JSON (raw), Parquet (processed) |
| Visualization | Matplotlib, Seaborn |

## Project Structure

```
TMDB_SPARK/
├── data/
│   ├── raw/                  # Raw JSON from API
│   ├── processed/            # Cleaned Parquet files
│   └── analysis/             # Analysis results
├── logs/                     # Pipeline execution logs
├── scripts/
│   ├── pipeline.py           # Main orchestrator
│   ├── data_extraction.py    # API integration
│   ├── data_transformations.py # Data cleaning
│   ├── analysis.py           # KPI calculations
│   └── visualizations.py     # Chart generation
├── visualizations/           # Generated charts
├── new_config.py             # Centralized configuration
├── docker-compose.yml        # Container orchestration
├── Dockerfile                # Container definition
├── requirements.txt          # Python dependencies
└── .env                      # API credentials (not committed)
```

## Pipeline Stages

### Stage 1: Data Extraction

- Connects to TMDB API with retry logic and rate limiting
- Fetches movie details including cast, crew, and metadata
- Handles API errors gracefully with configurable retries
- Saves raw JSON for reproducibility

### Stage 2: Data Transformation

- Flattens nested JSON structures (genres, cast, crew, production companies)
- Extracts director information from crew data
- Converts budget and revenue to millions USD
- Handles missing values and data quality issues
- Outputs optimized Parquet format for analytics

### Stage 3: Analysis

- Calculates derived metrics (profit, ROI)
- Generates rankings across multiple dimensions
- Compares franchise vs standalone performance
- Identifies top-performing directors and franchises
- Saves results in Parquet format for downstream use

### Stage 4: Visualization

- Revenue vs Budget scatter plot with break-even line
- ROI distribution histogram
- Popularity vs Rating correlation
- Franchise vs Standalone comparison charts
- Top franchises by revenue and rating
- Genre distribution analysis

## Key Metrics Calculated

| Metric | Description |
|--------|-------------|
| Revenue | Total box office earnings (millions USD) |
| Budget | Production cost (millions USD) |
| Profit | Revenue minus Budget |
| ROI | Revenue divided by Budget |
| Vote Average | Audience rating (0-10 scale) |
| Popularity | TMDB popularity score |

## Configuration

All settings are centralized in `new_config.py`:

- API credentials (loaded from .env file)
- Spark cluster settings
- File paths and naming conventions
- Analysis parameters (minimum votes, budget thresholds)
- Visualization styling

## Prerequisites

- Docker Desktop installed and running
- TMDB API key (free registration at themoviedb.org)
- Minimum 8GB RAM recommended
- 5GB free disk space

## Quick Start

1. Clone the repository

2. Create environment file:
```
echo "TMDB_API_KEY=your_api_key_here" > .env
```

3. Build the Docker image:
```
docker-compose build
```

4. Run the pipeline:
```
docker-compose up pipeline
```

5. View outputs:
```
ls -la data/analysis/
ls -la visualizations/
cat logs/pipeline.log
```

## Output Files

### Data Files

| File | Description |
|------|-------------|
| `data/raw/movies_raw.json` | Raw API response |
| `data/processed/movies_clean.parquet` | Cleaned dataset |
| `data/analysis/ranking_*.parquet` | Various rankings |
| `data/analysis/franchise_comparison.parquet` | Franchise analysis |
| `data/analysis/top_directors.parquet` | Director performance |

### Visualizations

| File | Description |
|------|-------------|
| `revenue_vs_budget.png` | Budget-revenue relationship |
| `roi_distribution.png` | ROI histogram |
| `popularity_vs_rating.png` | Rating-popularity correlation |
| `franchise_comparison.png` | Franchise vs standalone metrics |
| `top_franchises.png` | Top franchise performance |
| `genre_distribution.png` | Genre frequency analysis |

## Interactive Analysis

Launch Jupyter Notebook for ad-hoc analysis:

```
docker-compose up spark-app
```

Access at: http://localhost:8888

Spark UI available at: http://localhost:4040 (during job execution)

## Logging

Pipeline execution is logged to both console and file:

- Log file: `logs/pipeline.log`
- Log level: INFO (configurable)
- Includes timestamps, module names, and execution details

## Error Handling

- API failures trigger automatic retries with exponential backoff
- Invalid movie IDs are skipped with warnings
- Missing data fields are handled gracefully
- All errors are logged with full stack traces

## Performance Considerations

- Spark configured with 2GB driver and executor memory (adjustable)
- DataFrame caching enabled for repeated operations
- Parquet format used for efficient columnar storage
- Shuffle partitions optimized for small datasets

## Extending the Pipeline

### Adding New Movies

Update `MOVIE_IDS` list in `new_config.py`:

```python
MOVIE_IDS = [
    299534,  # Avengers: Endgame
    # Add new movie IDs here
]
```

### Adding New Analysis

1. Add method to `scripts/analysis.py`
2. Call from `run_analysis()` in `scripts/pipeline.py`
3. Save results in `_save_analysis_results()`

### Adding New Visualizations

1. Add method to `scripts/visualizations.py`
2. Call from `generate_all_visualizations()`
