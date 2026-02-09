# System Architecture

## Pipeline Flowchart

```
                    ┌─────────────┐
                    │  TMDB API   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────────────┐
                    │  STAGE 1: EXTRACT   │
                    │  ─────────────────  │
                    │ • Fetch movie data  │
                    │ • Retry logic       │
                    │ • Rate limiting     │
                    │ • Atomic writes     │
                    └──────┬──────────────┘
                           │
                        Raw JSON
                        (idempotent)
                           │
                    ┌──────▼──────────────────┐
                    │ STAGE 2: TRANSFORM      │
                    │ ───────────────────────  │
                    │ • Explicit schema       │
                    │ • Nested extraction     │
                    │ • Type conversion       │
                    │ • Validation (FAIL FAST)│
                    │ • Checkpointing         │
                    │ • Deduplication         │
                    └──────┬──────────────────┘
                           │
                        Parquet Clean
                       (validated)
                           │
                    ┌──────▼──────────────┐
                    │ STAGE 3: ANALYZE    │
                    │ ──────────────────  │
                    │ • Calculate KPIs    │
                    │ • Rankings          │
                    │ • Franchise compare │
                    │ • Top performers    │
                    └──────┬──────────────┘
                           │
                    Analysis Results
                    (Parquet format)
                           │
                    ┌──────▼──────────────┐
                    │ STAGE 4: VISUALIZE  │
                    │ ──────────────────  │
                    │ • Revenue charts    │
                    │ • ROI analysis      │
                    │ • Comparisons       │
                    │ • Genre breakdown   │
                    └──────┬──────────────┘
                           │
                        PNG/SVG Outputs
```

## Module Architecture

```
pipeline.py (Orchestrator)
├── data_extraction.py (TMDBExtractor)
│   ├── retry_on_failure() - Decorator for transient errors
│   ├── fetch_movie_with_credits() - Single movie fetch
│   ├── extract_movies() - Batch extraction with validation
│   └── _save_raw_data_atomic() - Atomic file writes
│
├── data_transformations.py (MovieDataCleaner)
│   ├── MOVIE_SCHEMA - Explicit schema definition
│   ├── load_from_json() - DataFrame creation
│   ├── extract_nested_fields() - JSON flattening
│   ├── convert_datatypes() - Type enforcement
│   ├── handle_missing_and_invalid() - Data quality
│   ├── remove_duplicates_and_filter() - Deduplication
│   ├── validate_schema() - FAIL FAST validation
│   ├── validate_data_quality() - Quality checks
│   ├── checkpoint() - Fault recovery
│   └── clean_pipeline() - End-to-end transformation
│
├── analysis.py (MovieAnalyzer)
│   ├── get_all_rankings() - Multi-dimension rankings
│   ├── advanced_searches() - Complex queries
│   ├── franchise_vs_standalone() - Comparative analysis
│   ├── top_franchises() - Top performers
│   └── top_directors() - Director performance
│
└── visualizations.py (MovieVisualizer)
    ├── generate_all_visualizations() - Master function
    ├── plot_revenue_vs_budget() - Budget analysis
    ├── plot_roi_distribution() - ROI histogram
    └── (More visualization methods)
```

## Error Handling Strategy

```
Pipeline Execution
│
├─ EXTRACTION STEP
│  ├─ Failure → ExtractionError → FAIL FAST ✗
│  └─ Success → Continue
│
├─ TRANSFORMATION STEP
│  ├─ Schema Validation Failure → ValidationError → FAIL FAST ✗
│  ├─ Data Quality Error → ValidationError → FAIL FAST ✗
│  └─ Success → Continue
│
├─ ANALYSIS STEP
│  ├─ Failure → Log warning, Continue with None results
│  └─ Success → Continue
│
└─ VISUALIZATION STEP
   ├─ Failure → Log warning, Pipeline completes ✓
   └─ Success → Pipeline completes ✓
```

## Idempotency Strategy

```
First Run:
  Extract → Transform → Analyze → Visualize → Success

Second Run (Idempotent Mode):
  Skip Extract (file exists) → Transform → Analyze → Visualize → Success

Second Run (Fresh Mode --fresh):
  Force Extract → Transform → Analyze → Visualize → Success

Failure Recovery:
  Extract → Transform [Checkpoint 1] → [Checkpoint 2] → [Checkpoint 3]
           → Failed at Analysis
  Re-run  → Skip Extract → Skip Transform (from checkpoint) → Analyze → Success
```

## Checkpointing Strategy

```
Clean Pipeline Execution:

Step 1: load_from_json()
   ↓
Step 2: drop_irrelevant_columns()
   ↓ [CHECKPOINT]
Step 3: extract_nested_fields()
   ↓ [CHECKPOINT]
Step 4: convert_datatypes()
   ↓ [CHECKPOINT]
Step 5: handle_missing_and_invalid()
   ↓ [CHECKPOINT]
Step 6: remove_duplicates_and_filter()
   ↓ [CHECKPOINT]
Step 7: reorder_columns()
   ↓
VALIDATION (validate_schema, validate_data_quality)
   ↓
Output: Clean Parquet
```

Checkpoints stored in: `logs/checkpoints/`

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                          │
│                    TMDB API (REST)                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ JSON Response
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   EXTRACTION LAYER                          │
│  • Retry decorator (transient errors)                       │
│  • Atomic writes (temp + rename)                            │
│  • Validation (id, title required)                          │
│  • Rate limiting + deduplication                            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ movies_raw.json
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   TRANSFORMATION LAYER                      │
│  • Explicit schema enforcement                              │
│  • Nested field flattening                                  │
│  • Type conversions + null handling                          │
│  • Schema validation (FAIL FAST)                            │
│  • Data quality checks (FAIL FAST)                          │
│  • Checkpointing at each stage                              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ movies_clean.parquet
                       │
┌──────────────────────┬──────────────────────────────────────┐
│                      │                                      │
│ ANALYSIS LAYER       │                 CACHING LAYER       │
│ • KPI calculations   │                 • DataFrame cache   │
│ • Rankings           │                 • Reuse across      │
│ • Comparisons        │                   analysis ops      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Analysis Results (Parquet)
                       │
┌──────────────────────▼──────────────────────────────────────┐
│               VISUALIZATION LAYER (Non-Critical)            │
│  • Revenue vs Budget scatter plot                           │
│  • ROI distribution histogram                               │
│  • Rating vs Popularity scatter                             │
│  • Franchise comparison charts                              │
│  • Genre analysis                                           │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ PNG/SVG Charts
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                     OUTPUT LAYER                            │
│  • Visualizations: visualizations/                          │
│  • Data: data/processed/, data/analysis/                    │
│  • Logs: logs/pipeline.log                                  │
│  • Checkpoints: logs/checkpoints/                           │
└─────────────────────────────────────────────────────────────┘
```

## Resilience Features

### Fail Fast
- **Critical**: Extraction and transformation failures stop pipeline
- **Non-Critical**: Analysis and visualization warnings logged, pipeline continues
- **Result**: Clean failure with full stack trace, no partial data

### Retry Logic
- **Transient Errors**: Network timeouts, API rate limits (retry 3x with backoff)
- **Permanent Errors**: Invalid API key, movie not found (logged and skipped)
- **Result**: Robust handling of temporary API issues

### Idempotency
- **Extraction**: Skipped if data file exists and valid
- **Transformation**: Can be re-run without side effects (overwrites outputs)
- **Analysis**: Deterministic (same input = same output always)
- **Result**: Safe to re-run entire pipeline multiple times

### Atomic Writes
- **Pattern**: Write to temp file → Verify → Atomic rename
- **Benefit**: No partial/corrupted data on failure
- **Files**: Raw JSON, analysis results, visualizations

### Checkpointing
- **Purpose**: Enable recovery from failures mid-pipeline
- **Granularity**: After each transformation stage
- **Location**: `logs/checkpoints/`
- **Benefit**: Re-run from checkpoint, not from start
