# Technology Stack

**Analysis Date:** 2026-03-15

## Languages

**Primary:**
- Python 3.x - ML model training, feature engineering, demo scripts, testing
- SQL - Spark DataFrame operations and Iceberg table queries

**Secondary:**
- Bash - Script execution and deployment (spark-submit commands)

## Runtime

**Environment:**
- Python 3.x interpreter (local execution)
- Apache Spark 3.4+ (production pipeline execution)
- Java JVM (required for Spark)

**Package Manager:**
- pip - Python package management
- Lockfile: `requirements_churn_model.txt` (present, pinned versions)

## Frameworks

**Core ML:**
- scikit-learn 1.3.0 - Machine learning algorithms (Logistic Regression, Random Forest, Gradient Boosting)
- xgboost 2.0.0 - Gradient boosting classifier (primary production model)
- lightgbm 4.1.0 - Fast gradient boosting alternative
- imbalanced-learn 0.11.0 - SMOTE for handling class imbalance

**Data Processing:**
- pandas 2.1.0 - DataFrames for feature engineering and analysis
- numpy 1.24.0 - Numerical computations
- pyspark 3.4.1 - Distributed data processing with Spark

**Visualization:**
- matplotlib 3.7.0 - Plot generation (ROC curves, confusion matrices)
- seaborn 0.12.0 - Statistical data visualization

**Model Persistence:**
- joblib 1.3.0 - Serialization of trained models, scalers, and metadata

**Testing:**
- pytest (implicit usage in test_churn_model.py)

## Key Dependencies

**Critical:**
- scikit-learn 1.3.0 - Foundation for ML model training and evaluation
- xgboost 2.0.0 - Best performing model in pipeline (F1 ~0.86)
- pyspark 3.4.1 - Enables processing of large-scale Iceberg tables
- pandas 2.1.0 - Feature engineering and data transformation
- joblib 1.3.0 - Model artifact serialization for production deployment

**Data Processing:**
- numpy 1.24.0 - Numerical operations and array handling
- imbalanced-learn 0.11.0 - SMOTE resampling for 70%/20%/10% class distribution

**Visualization & Analysis:**
- matplotlib 3.7.0 - Feature importance, ROC curves, confusion matrices
- seaborn 0.12.0 - Enhanced statistical plots

## Configuration

**Environment:**
- Spark configuration in code: `churn_rate_prediction.py` lines 1001-1007
- Iceberg catalog configured as "hadoop" type
- S3 warehouse path: `s3://telco-lakehouse/silver`
- No `.env` files detected; configuration hardcoded in scripts

**Build:**
- No build configuration files (.toml, .cfg) present
- Direct Python script execution without compilation

**Spark Configuration (Production):**
```
spark.sql.catalog.iceberg_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type = hadoop
spark.sql.catalog.iceberg_catalog.warehouse = s3://telco-lakehouse/silver
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

## Platform Requirements

**Development:**
- Python 3.x with pip
- 4GB+ RAM for local demo execution
- Approximately 500MB disk for model artifacts and visualizations

**Production:**
- Apache Spark 3.4+ cluster
- Iceberg 1.4.3+ support
- S3 bucket access (aws credentials)
- Network connectivity to S3 and Kafka (inferred from lakehouse architecture)

**Production Runtime Dependencies:**
```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  --driver-memory 8g \
  --executor-memory 16g \
  churn_rate_prediction.py
```

## Testing Dependencies

**Testing Framework:**
- Built-in Python assertions (test_churn_model.py lines 9-27)
- No external test framework detected (pytest/unittest imports absent)

**Test Validation:**
- Dependency check script: `test_churn_model.py`
- Sample data validation for 5 JSON files
- Integration test: loads and parses `agreements.json` (line 66)

## Version Pinning Strategy

All critical dependencies are pinned to specific versions in `requirements_churn_model.txt`:
- Data processing: pandas==2.1.0, numpy==1.24.0
- ML: scikit-learn==1.3.0, xgboost==2.0.0, lightgbm==4.1.0
- Visualization: matplotlib==3.7.0, seaborn==0.12.0

Optional dependencies (commented out, not required):
- mlflow==2.8.0 (experiment tracking - future)
- shap==0.43.0 (model explainability - future)
- flask==2.3.0 (REST API - future)
- psycopg2-binary==2.9.7 (PostgreSQL/TimescaleDB - future)
- sqlalchemy==2.0.21 (ORM - future)

---

*Stack analysis: 2026-03-15*
