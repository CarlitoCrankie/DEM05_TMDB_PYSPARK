# TMDB Movie Analysis Pipeline

## Executive Summary

This project implements a robust, production-grade data engineering pipeline for extracting, transforming, and analyzing movie data from The Movie Database (TMDB) API. Built with Apache Spark (PySpark) and containerized using Docker, the pipeline features fail-fast error handling, idempotent execution, checkpointing for fault tolerance, explicit schema validation, and atomic writes for data integrity.

## Business Objective

The pipeline answers key business questions about the film industry:

- Which movies generate the highest revenue and profit?
- What is the return on investment (ROI) for different budget levels?
- How do franchise films perform compared to standalone releases?
- Which directors consistently deliver successful films?
- What factors correlate with audience ratings and popularity?

## Technical Architecture

![System Architecture Diagram](data/System_Architecture.png)

**Architecture Overview:**

The pipeline follows a modular design with clear separation of concerns:

```
API → Extraction → Transformation → Analysis → Visualization
     (Retries)  (Validation)    (KPIs)     (Charts)
                (Checkpoints)
```

For detailed architecture and diagrams, see [docs/project_overview.md](docs/project_overview.md) and [docs/diagrams/](docs/diagrams/)

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

- **Idempotent**: Skips extraction if valid data already exists (use `--fresh` to force re-extraction)
- **Retry Logic**: Automatic retries for transient errors (API rate limits, network issues)
- **Rate Limiting**: Respects TMDB API rate limits with configurable delays
- **Atomic Writes**: Uses temp files and atomic rename for data integrity
- **Validation**: Ensures all extracted records have required fields (id, title)
- **Detailed Logging**: Full audit trail of extraction attempts and failures

### Stage 2: Data Transformation

- **Explicit Schema**: All fields defined with proper types upfront
- **Checkpointing**: Intermediate checkpoints for fault recovery at each transformation step
- **Nested Field Extraction**: Flattens JSON structures (genres, cast, crew, production companies)
- **Data Validation**: Validates schema and data quality (FAIL FAST on validation errors)
- **Type Conversion**: Ensures proper data types (integers, doubles, dates)
- **Null Handling**: Strategic null replacement and empty string cleanup
- **Deduplication**: Removes duplicates and filters released movies only

### Stage 3: Analysis

- **Derived Metrics**: Profit, ROI, budget breakdowns
- **Multiple Rankings**: By revenue, rating, popularity, profit
- **Franchise Analysis**: Compares franchise vs standalone performance
- **Director Performance**: Identifies top-performing directors
- **Non-Critical**: Failures don't stop pipeline, logged as warnings

### Stage 4: Visualization

- **Non-Critical**: Generated if analysis succeeds, skipped if not
- **Charts Generated**: Revenue vs Budget, ROI distribution, Rating-Popularity correlation, Franchise comparison, Top franchises, Genre distribution

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

- **API Credentials**: Loaded from `.env` file (not committed to version control)
- **Spark Settings**: Driver/executor memory, shuffle partitions, timeouts
- **File Paths**: Data directories, logs, visualization output
- **Analysis Parameters**: Minimum votes, budget thresholds, top-N results
- **Pipeline Behavior**: Logging levels, checkpoint directory, validation rules
- **Visualization Styling**: Plot colors, figure sizes, label formatting

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

4. Run the pipeline (idempotent mode by default):
```
docker-compose up pipeline
```

   Or run with fresh extraction:
```
docker-compose run --rm pipeline python scripts/pipeline.py --fresh
```

5. Run the jupyter notebook:
```
docker-compose up spark-app
```
6. View outputs:
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

## Logging & Checkpointing

**Pipeline Logs**:
- Log file: `logs/pipeline.log` (appended across runs for audit trail)
- Log level: INFO (configurable)
- Includes timestamps, module names, and execution details
- Full stack traces for all errors

**Checkpointing**:
- Checkpoint directory: `logs/checkpoints/`
- Automatic checkpoints at each transformation stage
- Enables recovery from failures without re-running entire pipeline
- Safe for re-run (idempotent design)

## Error Handling & Resilience

**Fail Fast Strategy**:
- Critical failures (extraction, validation) stop pipeline immediately
- Non-critical failures (visualization) logged as warnings, pipeline continues
- Full stack traces to `logs/pipeline.log`

**Retry Logic**:
- API failures: Automatic retries with configurable backoff
- Transient errors: Network timeouts, rate limits handled gracefully
- Invalid IDs: Skipped with warnings, processing continues

**Data Integrity**:
- Atomic writes: All or nothing file operations (temp file + rename)
- Schema validation: Explicit schema enforced on output
- Data quality checks: Null key validation, row count verification

## Performance Considerations

- **Spark Configuration**: 4GB driver/executor memory (adjustable in docker-compose.yml)
- **DataFrame Caching**: In-memory caching for repeated operations
- **Parquet Format**: Efficient columnar storage for analysis datasets
- **Partitioning**: Shuffle partitions optimized for cluster size
- **Checkpointing**: Staged checkpoints prevent re-computation on recovery

## Extending the Pipeline

### Adding New Movies

Update `MOVIE_IDS` list in `new_config.py`:

```python
MOVIE_IDS = [
    299534,  # Avengers: Endgame
    # Add new movie IDs here
]
```

Pipeline will skip existing data on re-run (idempotent). Use `--fresh` flag to re-extract.

### Adding New Analysis

1. Add method to `scripts/analysis.py`
2. Call from `run_analysis()` in `scripts/pipeline.py`
3. Save results in `_save_analysis_results()`
4. Failures in analysis are non-critical (logged, pipeline continues)

### Adding New Validations

1. Add validation method to `MovieDataCleaner` class
2. Call from `clean_pipeline()` after transformations
3. Raise `ValidationError` to trigger FAIL FAST behavior
4. Add recovery strategy in exception handler if needed

### Adding New Visualizations

1. Add method to `scripts/visualizations.py`
2. Call from `generate_all_visualizations()`
3. Non-critical stage - failures logged as warnings
