# Codebase Structure

**Analysis Date:** 2026-03-15

## Directory Layout

```
lakehouse/
├── churn_rate_prediction.py        # Production ML pipeline with Spark
├── churn_prediction_demo.py        # Local demo with synthetic data
├── test_churn_model.py             # Validation script for dependencies
├── CHURN_MODEL_README.md           # Complete model documentation
├── IMPLEMENTATION_SUMMARY.md       # Implementation summary and deployment guide
├── lakehouse_prototype_setup.md    # Lakehouse infrastructure setup guide
├── lakehouse_prototyping.md        # Architecture documentation and design decisions
├── requirements_churn_model.txt    # Python package dependencies
└── data_samples/                   # Sample JSON data files
    ├── agreements.json             # Sample agreement record schema
    ├── products.json               # Sample product record schema
    ├── voice_usage_events.json     # Sample voice event schema
    ├── data_usage_events.json      # Sample data event schema
    └── recharge_events.json        # Sample recharge event schema
```

## Directory Purposes

**Root Directory:**
- Purpose: Container for all churn prediction model code, documentation, and sample data
- Contains: Python scripts, markdown documentation, sample data directory
- Key files: Main executables (`churn_rate_prediction.py`, `churn_prediction_demo.py`, `test_churn_model.py`)

**data_samples/:**
- Purpose: Store example JSON data structures that reflect the Silver layer Iceberg table schemas
- Contains: One JSON file per entity type (agreements, products, voice events, data events, recharge events)
- Key files: All five sample JSON files are used by `churn_prediction_demo.py` SampleDataLoader for structure validation and synthetic data generation
- Special: These files define the expected schema for production Silver layer tables

## Key File Locations

**Entry Points:**

- `churn_rate_prediction.py`: Main production script. Entry via `spark-submit` command. Initializes Spark, orchestrates feature engineering, trains models, saves artifacts.
  - Size: 1050 lines
  - Key classes: SilverLayerDataLoader, ChurnFeatureEngineering, ChurnModelTrainer
  - Key function: `main()` at line 992

- `churn_prediction_demo.py`: Demo script for local testing. Entry via `python` command. Generates synthetic data, trains models, creates visualizations.
  - Size: 576 lines
  - Key classes: SampleDataLoader, ChurnModelTrainer
  - Key function: `main()` at line 521

- `test_churn_model.py`: Validation script. Entry via `python` command. Checks dependencies and data files.
  - Size: 117 lines
  - Key functions: check_dependencies(), check_data_files(), test_demo(), main()
  - Located at: Root level

**Configuration:**

- `requirements_churn_model.txt`: Lists Python package dependencies (pandas, numpy, scikit-learn, xgboost, matplotlib, seaborn, pyspark)
- No config files: Spark/Iceberg configuration hardcoded in `main()` function of `churn_rate_prediction.py` (lines 1001-1007)
- Hyperparameters: Defined inline in model instantiation methods (e.g., RandomForestClassifier at line 641-649)

**Core Logic:**

- `churn_rate_prediction.py` lines 43-154: `SilverLayerDataLoader` class — Data extraction from Iceberg Silver tables
- `churn_rate_prediction.py` lines 157-491: `ChurnFeatureEngineering` class — Feature transformation pipeline
- `churn_rate_prediction.py` lines 494-989: `ChurnModelTrainer` class — Model training and evaluation
- `churn_prediction_demo.py` lines 24-296: `SampleDataLoader` class — Demo data handling
- `churn_prediction_demo.py` lines 299-518: `ChurnModelTrainer` class — Demo model training

**Testing:**

- `test_churn_model.py` lines 9-27: Dependency validation (check_dependencies function)
- `test_churn_model.py` lines 29-55: Data file validation (check_data_files function)
- `test_churn_model.py` lines 57-82: Integration test (test_demo function)

**Documentation:**

- `CHURN_MODEL_README.md`: Model architecture, data sources, features, training results, deployment instructions (126+ lines)
- `IMPLEMENTATION_SUMMARY.md`: Implementation overview, feature list, model performance, integration with lakehouse (363+ lines)
- `lakehouse_prototype_setup.md`: Infrastructure setup steps for Kafka, Flink, Iceberg, S3 (500+ lines)
- `lakehouse_prototyping.md`: High-level architecture design and data flow documentation

## Naming Conventions

**Files:**

- Production scripts: lowercase with underscore separators (`churn_rate_prediction.py`, `churn_prediction_demo.py`)
- Test/validation: `test_` prefix (`test_churn_model.py`)
- Documentation: CamelCase uppercase (`CHURN_MODEL_README.md`, `IMPLEMENTATION_SUMMARY.md`)
- Config: Lowercase with `.txt` extension (`requirements_churn_model.txt`)
- Sample data: snake_case with entity type (`voice_usage_events.json`, `data_usage_events.json`)

**Directories:**

- Lowercase with underscore for multi-word names (`data_samples/`)
- Output directory created at runtime: `models/churn/` (hardcoded in ChurnModelTrainer.__init__)

**Python Classes:**

- PascalCase: `SilverLayerDataLoader`, `ChurnFeatureEngineering`, `ChurnModelTrainer`, `SampleDataLoader`

**Python Functions:**

- snake_case: `load_agreements()`, `create_voice_features()`, `train_random_forest()`, `prepare_training_data()`
- Private: No underscore prefix convention used; all methods public

**Python Variables:**

- snake_case: `feature_cols`, `X_train`, `y_test`, `recent_start`, `historical_end`
- Constants: All-caps not used; no configuration constants defined

**Feature Names:**

- Temporal prefix: `recent_` and `historical_` for period-comparative features (e.g., `recent_outgoing_calls`, `historical_voice_duration`)
- Drop metric suffix: `_drop_pct` for percentage change features (e.g., `outgoing_calls_drop_pct`, `recharge_amount_drop_pct`)
- Derived metric patterns: `{adjective}_{noun}` (e.g., `engagement_score`, `service_usage_diversity`, `revenue_per_recharge`)

## Where to Add New Code

**New Feature:**

- Primary code: Add feature engineering logic to `churn_rate_prediction.py` in `ChurnFeatureEngineering` class
  - If aggregating existing table data: Add method like `create_{entity}_features()` (line 183-289 pattern)
  - If calculating derived metrics: Add method like `create_derived_features()` (line 356-412 pattern)
  - If calculating drop metrics: Extend `calculate_drop_metrics()` (line 291-354)
  - Register new feature in `create_all_features()` orchestrator method (line 414-491) by adding join operation

- Tests: Add synthetic data generation logic to `churn_prediction_demo.py` in `SampleDataLoader.generate_sample_dataset()` method (lines 157-296)
  - Generate synthetic feature values matching realistic distributions
  - Ensure new feature is included in data dictionary at lines 241-289

**New Component/Module:**

- Implementation: Create new Python file at root level with class/function structure matching existing patterns
  - Follow naming convention: `{purpose}_{entity}.py` (e.g., `feature_selector_voice.py`)
  - Import at top of `churn_rate_prediction.py` or `churn_prediction_demo.py`
  - Integrate into data flow by calling from `ChurnFeatureEngineering` or relevant class

- Integration: Add import statement and instantiation in appropriate entry point (`main()` function)

**Utilities:**

- Shared helpers: Add to existing class as static methods or module-level functions
  - Example: Add to `ChurnFeatureEngineering` class for feature-related utilities
  - Example: Add to `ChurnModelTrainer` class for model-related utilities
  - Do not create separate utils file unless functions are used across multiple modules

**New Model Algorithm:**

- Implementation: Add new method to `ChurnModelTrainer` class following the pattern of `train_random_forest()` (lines 634-675)
  - Method signature: `def train_{model_name}(self, X_train, y_train, X_test, y_test):`
  - Body structure: instantiate model, fit, predict, calculate metrics, store in self.models dict
  - Return: None (model stored in self.models)

- Registration: Add model training call in `train_pipeline()` method (lines 942-989) before `select_best_model()` call
  - Add to both production and demo versions

## Special Directories

**models/churn/:**
- Purpose: Store trained model artifacts and visualizations
- Generated: Yes (created by ChurnModelTrainer.save_model() at line 902)
- Committed: No (typically excluded via .gitignore)
- Contents after training:
  - `churn_model.pkl` — Serialized trained model
  - `scaler.pkl` — Feature StandardScaler object
  - `feature_names.json` — List of 70+ feature names for reproducibility
  - `model_metadata.json` — Model type, feature count, performance metrics, creation timestamp
  - `confusion_matrix.png` — Confusion matrix heatmap visualization
  - `roc_curve.png` — ROC curve with AUC metric
  - `feature_importance.png` — Top 20 feature importances bar chart

**data_samples/:**
- Purpose: Reference JSON samples defining Silver layer table schemas
- Generated: No (committed with repo)
- Committed: Yes (part of source control for demo reproducibility)
- Contents: Five JSON files, each with single record example structure for one entity type

## Production vs Demo Differences

**Production Script (churn_rate_prediction.py):**
- Data source: Iceberg Silver layer tables via Spark
- Data loader: `SilverLayerDataLoader` class with Spark DataFrame operations
- Scale: Distributed processing on entire dataset
- Feature engineering: Real temporal windows from actual dates in data
- Label creation: Business rule-based churn definition
- Demo data: None used

**Demo Script (churn_prediction_demo.py):**
- Data source: Local sample JSON files + synthetic generation
- Data loader: `SampleDataLoader` class with JSON parsing and pandas
- Scale: Single machine, 1000 synthetic customers
- Feature engineering: Same feature definitions but applied to synthetic data
- Label creation: Simulated customer types (active/declining/churning) mapped to labels
- Sample data: Uses `data_samples/*.json` for structure validation

**Key Code Sharing:**
- Both use `ChurnModelTrainer` class for model training (one shared class, instantiated in both scripts)
- Feature definitions are parallel but not shared (production uses Spark SQL, demo uses pandas)
- Model evaluation and artifact saving use identical methods

---

*Structure analysis: 2026-03-15*
