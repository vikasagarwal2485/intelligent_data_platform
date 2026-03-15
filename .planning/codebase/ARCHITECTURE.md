# Architecture

**Analysis Date:** 2026-03-15

## Pattern Overview

**Overall:** Layered Data Pipeline Architecture with ML Model Training

**Key Characteristics:**
- Multi-layer data processing (Bronze → Silver → Gold) using Apache Iceberg
- Feature engineering pipeline from event-based data into temporal features
- Modular ML training with multi-algorithm comparison pattern
- Spark-based distributed processing with local fallback for testing
- Time-windowed feature calculation (30-day observation vs 30-day historical)

## Layers

**Data Ingestion Layer:**
- Purpose: Capture raw CDR events from Kafka and ingested into Iceberg Bronze tables
- Location: Kafka topics (`cdr-raw-events`, `cdr-dlq`) → S3 Bronze layer
- Contains: Raw call detail records, recharge events, data usage events
- Depends on: Kafka, S3, Kafka Connect
- Used by: Flink processing layer for data transformation

**Stream Processing Layer:**
- Purpose: Transform raw events into clean, normalized records with schema validation
- Location: Apache Flink jobs (referenced in `lakehouse_prototype_setup.md`)
- Contains: Event validation, schema transformation, enrichment
- Depends on: Kafka (source), Iceberg (sink)
- Used by: Silver layer for analytics-ready data

**Silver Layer (Iceberg):**
- Purpose: Clean, normalized, analytics-ready data
- Location: `iceberg_catalog.silver.*` tables in S3
- Contains: 5 normalized tables—`agreements`, `products`, `voice_usage_events`, `data_usage_events`, `recharge_events`
- Depends on: Flink stream processing
- Used by: Churn feature engineering pipeline

**Feature Engineering Layer:**
- Purpose: Transform Silver layer event data into ML-ready features through temporal aggregation
- Location: `churn_rate_prediction.py` classes `SilverLayerDataLoader` and `ChurnFeatureEngineering`
- Contains: Feature extraction, temporal windowing, drop metric calculation, derived feature creation
- Depends on: Silver layer Iceberg tables via Spark
- Used by: Model training layer

**Model Training Layer:**
- Purpose: Train and select optimal churn prediction models
- Location: `churn_rate_prediction.py` class `ChurnModelTrainer` and `churn_prediction_demo.py` class `ChurnModelTrainer`
- Contains: 4 ML algorithms (Logistic Regression, Random Forest, XGBoost, LightGBM), imbalance handling (SMOTE), model evaluation and comparison
- Depends on: Feature engineering output (Spark DataFrame or pandas DataFrame)
- Used by: Batch inference pipeline and serving layer

**Gold/Serving Layer:**
- Purpose: Store predictions and model artifacts for downstream consumption
- Location: Model artifacts in `models/churn/` directory; predictions written to S3 Gold layer
- Contains: Trained model pickle files, feature scalers, feature name mappings, prediction DataFrames
- Depends on: Model training outputs
- Used by: Retention campaign systems, real-time scoring APIs

## Data Flow

**Batch Training Pipeline:**

1. **Data Loading** → `SilverLayerDataLoader.load_agreements()`, `load_products()`, `load_voice_usage()`, `load_data_usage()`, `load_recharge_events()`
   - Read from Iceberg Silver tables filtered by date range
   - Extract nested JSON attributes from Iceberg structured columns
   - Return Spark DataFrames per entity type

2. **Temporal Windowing** → `ChurnFeatureEngineering.create_temporal_windows()`
   - Define two time periods: recent (30 days) and historical (30 days prior)
   - Used for comparative feature calculation

3. **Feature Aggregation** → `ChurnFeatureEngineering.create_voice_features()`, `create_data_features()`, `create_recharge_features()`, `create_product_features()`
   - Aggregate events by service_id and time window
   - Calculate counts, sums, averages, distinct values for each feature type
   - Window functions for chronological analysis (roaming patterns, daytime/nighttime calls, active days)

4. **Drop Metric Calculation** → `ChurnFeatureEngineering.calculate_drop_metrics()`
   - Compare recent vs historical period for key indicators
   - Calculate percentage change for: outgoing calls, voice duration, data usage, recharge amount, recharge frequency, voice activity, data activity

5. **Derived Features** → `ChurnFeatureEngineering.create_derived_features()`
   - Compute engagement score, usage intensity, service diversity, revenue per recharge metrics
   - Create composite behavioral indicators

6. **Feature Merging** → `ChurnFeatureEngineering.create_all_features()`
   - Join all feature sets on service_id using outer joins
   - Fill nulls with 0 (handles new customers or inactive services)
   - Output: Single feature DataFrame with 70+ columns

7. **Label Creation** → `ChurnModelTrainer.create_churn_labels()`
   - Apply business rules: outgoing_calls_drop_pct > 70% OR recharge_amount_drop_pct > 60% OR data_usage_drop_pct > 70% OR other conditions
   - Add churned (0/1) column to feature DataFrame

8. **Data Preparation** → `ChurnModelTrainer.prepare_training_data()`
   - Convert Spark DataFrame to pandas for sklearn
   - Filter feature columns (exclude service_id, churned)
   - Handle missing values with fillna(0)

9. **Train/Test Split** → `ChurnModelTrainer.split_data()`
   - Stratified split (80/20 by default) maintaining churn ratio

10. **Imbalance Handling** → `ChurnModelTrainer.handle_imbalance()`
    - Apply SMOTE with k_neighbors=min(5, minority_samples-1)
    - Resample to 50% of majority class for balanced training

11. **Feature Scaling** → `ChurnModelTrainer.scale_features()`
    - StandardScaler fit on training data
    - Applied to Logistic Regression model only (tree-based models use unscaled)

12. **Model Training** → `ChurnModelTrainer.train_logistic_regression()`, `train_random_forest()`, `train_xgboost()`, `train_lightgbm()`
    - Each model fitted on X_train, evaluated on X_test
    - Metrics collected: F1, Precision, Recall, ROC-AUC

13. **Model Selection** → `ChurnModelTrainer.select_best_model()`
    - Sort models by F1 score (primary metric)
    - Select highest F1 model as best_model

14. **Evaluation** → `ChurnModelTrainer.evaluate_model()`
    - Detailed classification report, confusion matrix, ROC-AUC on test set
    - Generate visualizations: confusion matrix heatmap, ROC curve, feature importance barplot

15. **Artifact Persistence** → `ChurnModelTrainer.save_model()`
    - Save model pickle, scaler pickle, feature names JSON, model metadata JSON
    - Location: `models/churn/` directory

**Demo Testing Pipeline (churn_prediction_demo.py):**

1. **Sample Data Loading** → `SampleDataLoader.load_json_file()`, extract_*_data() methods
   - Load and parse sample JSON from `data_samples/` directory
   - Extract fields from JSON structure

2. **Synthetic Data Generation** → `SampleDataLoader.generate_sample_dataset()`
   - Create 1000 synthetic customer records
   - Simulate three customer types: active (70%), declining (20%), churning (10%)
   - Generate realistic feature distributions for each type

3. **Feature Preparation** → Same as batch pipeline steps 8-9

4. **Model Training & Selection** → Same as batch pipeline steps 10-13

5. **Visualization & Saving** → Same as batch pipeline steps 14-15

**State Management:**

- **Spark Session State**: Initialized once per job with Iceberg catalog configuration. Reused across all data loading and feature engineering operations.
- **Feature DataFrame State**: Accumulates progressively through joins (`recent_voice.join(recent_data).join(...)`)
- **Model State**: Best performing model persisted as pickle file; scaler and feature names preserved for reproducibility
- **Training Metadata**: Model performance across all 4 algorithms stored in JSON for model selection auditing

## Key Abstractions

**SilverLayerDataLoader:**
- Purpose: Encapsulate Iceberg table access and schema extraction
- Examples: `churn_rate_prediction.py` lines 43-154
- Pattern: Class-based loader with separate method per entity type (agreements, products, voice_usage, data_usage, recharge_events). Each method: reads Iceberg table, filters by date, extracts nested columns with F.col() Spark SQL syntax, returns DataFrame

**ChurnFeatureEngineering:**
- Purpose: Implement feature transformation pipeline from raw events to model-ready features
- Examples: `churn_rate_prediction.py` lines 157-491
- Pattern: Temporal window factory method (`create_temporal_windows`), feature builder methods per data type (`create_voice_features`, `create_data_features`, etc.), orchestrator method (`create_all_features`) chains all operations

**ChurnModelTrainer:**
- Purpose: Manage model lifecycle from data prep through persistence
- Examples: `churn_rate_prediction.py` lines 494-989 (production) and `churn_prediction_demo.py` lines 299-518 (demo)
- Pattern: State-holding class (`self.models`, `self.best_model`, `self.scaler`, `self.feature_names`). Separate method per model type for training. Unified evaluation and visualization methods. `train_pipeline()` orchestrates complete workflow

**SampleDataLoader (Demo):**
- Purpose: Simulate Silver layer data using sample JSON structures and synthetic generation
- Examples: `churn_prediction_demo.py` lines 24-296
- Pattern: Data extractor methods match `SilverLayerDataLoader` field names; synthetic generator creates realistic customer profiles with controllable behavior patterns (active/declining/churning)

## Entry Points

**Production Entry Point:**
- Location: `churn_rate_prediction.py` function `main()` (lines 992-1046)
- Triggers: Manual execution via `spark-submit` or Airflow DAG scheduler
- Responsibilities: Initialize Spark session with Iceberg catalog, instantiate ChurnFeatureEngineering, call `create_all_features()`, instantiate ChurnModelTrainer, call `train_pipeline()`, manage Spark session lifecycle (stop on exit)

**Demo Entry Point:**
- Location: `churn_prediction_demo.py` function `main()` (lines 521-575)
- Triggers: Manual execution via `python` command, or called from `test_churn_model.py`
- Responsibilities: Load sample data files, generate synthetic dataset, train models on local data, save artifacts, display demo metrics

**Validation Entry Point:**
- Location: `test_churn_model.py` function `main()` (lines 84-113)
- Triggers: Manual execution for pre-deployment validation
- Responsibilities: Check Python dependencies, verify sample data files exist, run quick integration test with SampleDataLoader, report success/failure

## Error Handling

**Strategy:** Exception propagation with informative logging

**Patterns:**

**File/Data Loading Errors:**
- `test_churn_model.py` checks_dependencies(): Catches ImportError for missing packages; prints missing list and installation command
- `test_churn_model.py` check_data_files(): Verifies file existence; returns boolean rather than throwing
- `churn_prediction_demo.py` load_json_file(): Returns None on missing file with warning print
- `churn_rate_prediction.py` assumes Spark tables exist; no explicit error handling for missing tables (fails on read)

**Model Training Errors:**
- `ChurnModelTrainer.handle_imbalance()`: Checks minority class sample count; skips SMOTE if n_samples < 6; prints warning
- `ChurnFeatureEngineering.create_*_features()`: Uses F.when(condition, value).otherwise(fallback) for null-safe calculations (e.g., division by zero protection via .otherwise(0))

**State Validation:**
- `ChurnModelTrainer.prepare_training_data()`: Prints dataset shape and churn rate for validation
- `SampleDataLoader.generate_sample_dataset()`: Prints generated dataset shape and churn distribution
- No explicit assertions; relies on print statements for operator visibility

**Spark-Specific Errors:**
- `.fillna(0)` applied globally after feature joins to handle outer join nulls
- No explicit Spark error handling; assumes successful reads from Iceberg catalog

## Cross-Cutting Concerns

**Logging:**
- Approach: Print statements with formatted output (uses print() for progress updates, section headers with "="*60 separators, summary statistics)
- When: Major workflow milestones (feature engineering start, model training start, best model selection), data shape validation, artifact save confirmations
- Example: `print(f"✓ Feature engineering complete! Total features: {len(features_df.columns) - 1}")`

**Validation:**
- Approach: Runtime assertions via print/return status; data distribution checks
- When: After data loading (print shape), after churn label creation (print churn rate), after train/test split (print per-set churn rates), after model training (print F1/AUC per model)
- Example: `print(f"Churn rate: {features_df['churned'].mean()*100:.2f}%")`

**Authentication:**
- Approach: Implicit via Spark/Iceberg configuration; assumes IAM credentials available in environment
- Configuration: `spark.sql.catalog.iceberg_catalog.warehouse = s3://telco-lakehouse/silver` (S3 path requires AWS credentials)
- No explicit credential handling in code

**Feature Scaling:**
- Applied: Logistic Regression only (requires normalized input)
- Not applied: Random Forest, XGBoost, LightGBM (tree-based models are scale-invariant)
- Implementation: StandardScaler fit on training set, transformed on test set (prevents data leakage)

**Class Imbalance:**
- Addressed via: SMOTE (synthetic oversampling) on training data, class_weight='balanced' in model hyperparameters
- Details: SMOTE resamples minority class to 50% of majority; k_neighbors adapted to minority sample size

**Model Reproducibility:**
- Random seeds: random_state=42 set in all model instantiations and train_test_split
- Artifact storage: Feature names, scaler, model saved together in models/churn/ for inference consistency

---

*Architecture analysis: 2026-03-15*
