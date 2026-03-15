# Codebase Concerns

**Analysis Date:** 2026-03-15

## Tech Debt

### Array Indexing Without Bounds Checking

**Issue:** Unsafe array indexing on nested fields that may have variable lengths.

**Files:**
- `churn_rate_prediction.py` lines 101, 124
- `churn_prediction_demo.py` lines 85, 115

**Details:**
```python
# Line 101 in churn_rate_prediction.py
F.col("attributes.used_service_units")[0]["amount"].alias("duration_seconds")

# Line 124 in churn_rate_prediction.py
F.col("attributes.used_service_units")[0]["amount"].alias("data_kilobytes")

# Line 85 in churn_prediction_demo.py
duration = attrs['used-service-units'][0].get('amount', 0)
```

**Impact:** If `used_service_units` or `used-service-units` arrays are empty, indexing `[0]` will fail with IndexError. No validation occurs before accessing array elements. In Spark, this will cause job failures. In local code, it crashes.

**Fix approach:**
- Use PySpark array functions like `element_at()` with null coalescing
- Add defensive checks: `F.when(F.size(F.col("used_service_units")) > 0, ...)`
- Handle null/empty cases explicitly with fallback values


### Synthetic Churn Label Generation in Production Code

**Issue:** Model training in production uses hardcoded business rules to generate churn labels instead of actual churn data.

**Files:**
- `churn_rate_prediction.py` lines 504-529 (`create_churn_labels` method)

**Details:**
```python
# Line 507 - Explicit admission this should be replaced
"""In production, this should be replaced with actual churn data"""

churn_conditions = (
    (F.col("outgoing_calls_drop_pct") > 70) |  # 70% threshold is hardcoded
    (F.col("recharge_amount_drop_pct") > 60) |  # 60% threshold is hardcoded
    (F.col("data_usage_drop_pct") > 70) |       # 70% threshold is hardcoded
    # ...more hardcoded thresholds
)
```

**Impact:**
- Model is trained to predict synthetic labels, not actual churn
- Thresholds (70%, 60%) are arbitrary and untested
- Production predictions will show correlation with input features, not actual customer churn
- False sense of model performance - metrics look good but don't reflect business reality
- No ground truth available to validate model quality

**Fix approach:**
- Integrate actual churn labels from customer database (cancellations, disconnections)
- Move threshold values to configuration file, not hardcoded in code
- Add mechanism to accept external labels at training time
- Implement feedback loop to track actual vs predicted churn
- Business rules should inform feature engineering, not label creation


### Warning Suppression Without Context

**Issue:** All warnings filtered indiscriminately without understanding the underlying issues.

**Files:**
- `churn_rate_prediction.py` line 40
- `churn_prediction_demo.py` line 21

**Details:**
```python
warnings.filterwarnings('ignore')
```

**Impact:**
- Masks potential data issues, deprecation warnings, and version compatibility problems
- May hide future breakage when dependencies update
- sklearn, xgboost, lightgbm might emit warnings about data types or usage patterns
- Difficult to diagnose problems in production

**Fix approach:**
- Replace blanket suppression with specific warning filters
- Only suppress known, non-critical warnings with reasoning
- Example: `warnings.filterwarnings('ignore', category=DeprecationWarning, module='lightgbm')`
- Document why each warning is suppressed
- Periodically review and remove suppressions as libraries update


## Known Bugs

### Type Confusion in JSON Extraction - List vs String Values

**Issue:** Sample JSON data has inconsistent field types. Some fields are arrays, others are single values, causing extraction failures.

**Files:**
- `churn_prediction_demo.py` lines 52-56 (agreements extraction)
- `churn_prediction_demo.py` lines 69-71 (products extraction)

**Details:**
```python
# Line 52-53 - field might be list OR string
'agreement_type': attrs.get('agreement-type', [None])[0] if isinstance(attrs.get('agreement-type'), list) else attrs.get('agreement-type'),
'business_line_type': attrs.get('business-line-type', [None])[0] if isinstance(attrs.get('business-line-type'), list) else attrs.get('business-line-type'),

# Sample data (agreements.json line 6-7) shows lists:
"agreement-type": ["device", "subscription"],
"business-line-type": ["mobile", "broadband", ...],
```

**Trigger:** If API changes return field as string instead of list, extraction will fail. If Iceberg schema has different types in different rows, Spark job fails.

**Workaround:** Current code has type checking, but verbose and fragile. Better schema definition needed upstream.

**Fix approach:**
- Define schema validation at data ingestion point
- Use PySpark StructType with union types or consistent array types
- Add data quality checks before feature engineering
- Document expected field types in data contract


### Hardcoded Spark Configuration Path

**Issue:** S3 path is hardcoded and specific to one environment.

**Files:**
- `churn_rate_prediction.py` line 1005

**Details:**
```python
.config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://telco-lakehouse/silver")
```

**Impact:**
- Not portable across dev/staging/production
- Will fail if S3 bucket or path structure changes
- Requires code modification to run in different environment
- No environment variable substitution

**Fix approach:**
- Move to environment variables or config file
- Example: `os.environ.get('ICEBERG_WAREHOUSE', 's3://telco-lakehouse/silver')`
- Add config validation in main() before Spark initialization


## Security Considerations

### No Input Validation on Iceberg Catalog Configuration

**Issue:** Catalog and warehouse paths passed without validation to Spark config.

**Files:**
- `churn_rate_prediction.py` lines 1001-1007

**Details:**
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://telco-lakehouse/silver") \
    # No validation of paths
```

**Risk:** Malicious S3 path injection could redirect data reads to unauthorized buckets if configuration comes from untrusted sources.

**Current mitigation:** Hardcoded values reduce risk, but if variables are introduced, validation needed.

**Recommendations:**
- Validate S3 paths match expected pattern: `^s3://[a-z0-9-]+/[a-z0-9-/]+$`
- Whitelist allowed bucket names
- Use IAM role-based access with least privilege instead of credentials


### Model Serialization Without Integrity Check

**Issue:** Pickled models saved without signatures or checksums.

**Files:**
- `churn_rate_prediction.py` lines 905-906
- `churn_prediction_demo.py` lines 506-507

**Details:**
```python
model_path = f"{self.output_dir}/{model_name}.pkl"
joblib.dump(self.best_model, model_path)
```

**Risk:**
- Pickled objects can execute arbitrary code on deserialization
- No way to verify model hasn't been tampered with
- Loading untrusted .pkl files is a security vulnerability

**Fix approach:**
- Use ONNX format for model export instead of pickle
- Add cryptographic signatures to saved models
- Version control model artifacts and track changes
- Implement model validation on load


## Performance Bottlenecks

### Full DataFrame Conversion to Pandas

**Issue:** Entire Spark DataFrame converted to pandas in memory for training.

**Files:**
- `churn_rate_prediction.py` lines 534-535

**Details:**
```python
print("Converting Spark DataFrame to pandas...")
features_df = features_spark_df.toPandas()
```

**Problem:** Pandas holds entire dataset in memory. With millions of customers, this causes:
- Out of memory errors
- Slow performance (Spark → Driver → Pandas → sklearn pipeline)
- Serialization overhead

**Example impact:**
- 1M customers × 70 features × 8 bytes = ~560 MB
- Real enterprise data: 10M+ customers = 5.6 GB (exceeds typical driver memory)

**Improvement path:**
- Use spark-sklearn bridges like MLlib for distributed training
- Implement mini-batch training
- Use sample-based training for model selection, full data for final training
- For linear models, use Spark's native MLlib.classification


### Inefficient Feature Joins with Outer Joins

**Issue:** Multiple outer joins without deduplication can create data explosion.

**Files:**
- `churn_rate_prediction.py` lines 472-478

**Details:**
```python
features_df = recent_voice \
    .join(recent_data, on="service_id", how="outer") \
    .join(recent_recharge, on="service_id", how="outer") \
    .join(historical_voice, on="service_id", how="outer") \
    .join(historical_data, on="service_id", how="outer") \
    .join(historical_recharge, on="service_id", how="outer") \
    .join(product_features, on="service_id", how="outer")
```

**Impact:**
- If service_id appears multiple times in any aggregation result (data quality issue), cartesian product occurs
- 7 outer joins with skew = exponential data growth
- Slow execution and potential memory issues

**Fix approach:**
- Ensure aggregations produce unique service_id rows (GROUP BY service_id alone)
- Use inner joins where business logic allows
- Add deduplication and validation between joins
- Monitor join cardinality during execution


### No Index on service_id in Sample Data

**Issue:** In `churn_prediction_demo.py`, synthetic data generator creates sequential IDs without validation.

**Files:**
- `churn_prediction_demo.py` lines 165

**Details:**
```python
service_ids = [f"59598492{str(i).zfill(4)}" for i in range(num_customers)]
```

**Impact:**
- Only 10,000 unique IDs possible (59598492 + 4-digit number)
- Duplicate service_ids if num_customers > 10,000 causes silent data duplication
- Features are aggregated per service_id, so duplicates break meaning

**Fix approach:**
- Use UUID or larger key space: `str(uuid.uuid4())`
- Add uniqueness validation: `assert len(set(service_ids)) == len(service_ids)`
- Document ID generation scheme


## Fragile Areas

### Feature Engineering Pipeline - No Error Recovery

**Issue:** Feature engineering creates 7 intermediate DataFrames with no error handling or logging.

**Files:**
- `churn_rate_prediction.py` lines 414-491

**Details:**
```python
# 7 separate df loading calls, any can fail silently or with cryptic Spark errors
agreements_df = self.loader.load_agreements(...)  # Line 422
products_df = self.loader.load_products(...)      # Line 426
voice_df = self.loader.load_voice_usage(...)      # Line 430
data_df = self.loader.load_data_usage(...)        # Line 434
recharge_df = self.loader.load_recharge_events(...)  # Line 438
# Then 6 feature creation calls
# Then 6 aggregations
# Then 6 joins
```

**Why fragile:**
- If any data load returns empty DF, subsequent joins produce empty results (silent failure)
- If schema doesn't match (e.g., column renamed), job fails mid-pipeline with unclear error
- No validation that required columns exist before processing
- No data quality checks (e.g., non-null service_id)

**Safe modification:**
1. Add schema validation before processing each DataFrame
2. Check row counts and warn if unexpectedly low/zero
3. Add column existence checks: `assert "service_id" in df.columns`
4. Use try-catch blocks around feature creation with informative logging
5. Save intermediate results for debugging

**Test coverage gaps:**
- No tests for empty input data
- No tests for missing columns
- No tests for null values in key fields
- No integration tests of full pipeline


### Model Selection Logic - No Validation

**Issue:** Best model selected by single metric without considering other factors.

**Files:**
- `churn_rate_prediction.py` lines 771-797

**Details:**
```python
best_model_name = results_df.iloc[0]['Model']  # Line 792
self.best_model = self.models[best_model_name]['model']
```

**Why fragile:**
- F1 score alone doesn't ensure model quality (could have high F1 on biased test set)
- No consideration of precision/recall tradeoff for business use case
- If F1 scores are tied (rare but possible), arbitrary first result selected
- LightGBM with 0.899 F1 selected over XGBoost with 0.898 F1, but XGBoost may be more interpretable
- No model stability analysis - high variance model not flagged

**Safe modification:**
1. Define selection criteria explicitly: `max(f1_score, roc_auc, precision)`
2. Add tie-breaking logic: if F1 tied, prefer highest recall (for churn use case)
3. Implement model confidence score based on validation curve
4. Consider complexity vs performance tradeoff


### Hardcoded Thresholds Throughout

**Issue:** Business thresholds hardcoded in multiple places without traceability.

**Files:**
- `churn_rate_prediction.py` lines 512-521 (churn label thresholds)
- `churn_prediction_demo.py` lines 172-176, 188-191 (synthetic data customer type probabilities)
- `churn_rate_prediction.py` line 572-577 (SMOTE minority class threshold)

**Details:**
```python
# Line 512-521: Hardcoded business thresholds
(F.col("outgoing_calls_drop_pct") > 70) |      # Why 70%?
(F.col("recharge_amount_drop_pct") > 60) |     # Why 60%?
(F.col("data_usage_drop_pct") > 70) |          # Why 70%?
(F.col("recent_voice_active_days") < 3) & ...# Why 3 days?
(F.col("recent_days_since_last_recharge") > 20) # Why 20 days?

# Line 171: Customer type distribution
p=[0.7, 0.2, 0.1]  # Why these ratios?
```

**Impact:**
- Changing thresholds requires code modification and full retest
- No business justification documented
- Inconsistent thresholds (60% vs 70%) without clear reasoning
- Hard to A/B test different threshold values

**Fix approach:**
- Move all thresholds to config file (YAML/JSON)
- Add business rationale comments explaining each threshold
- Implement threshold optimization based on validation metrics
- Make thresholds tunable without code changes


## Test Coverage Gaps

### No Tests for Spark DataFrame Operations

**Issue:** Spark data loading and transformation code has zero test coverage.

**Files:**
- `churn_rate_prediction.py` lines 43-154 (SilverLayerDataLoader)
- `churn_rate_prediction.py` lines 157-491 (ChurnFeatureEngineering)

**What's not tested:**
- What happens if Silver layer tables don't exist
- What happens if columns have different types than expected
- What happens if date ranges have no data
- What happens if nested fields are missing or null
- Correctness of aggregations (GROUP BY logic)
- Window function calculations
- Feature scaling and normalization

**Risk:**
- Tests only run on demo (small, synthetic data)
- Real production data with edge cases causes silent failures
- Aggregation bugs not caught (wrong OVER partitions, missing ORDER BY)

**Priority:** High - these are critical paths that must work reliably


### Synthetic Data Not Representative

**Issue:** `churn_prediction_demo.py` generates unrealistic data patterns.

**Files:**
- `churn_prediction_demo.py` lines 157-289

**Problems:**
- Churn label perfectly separates based on thresholds: "churning" customers have 0-10 recent calls, "active" have 20-100
  - Real churn is messier with overlapping patterns
- No temporal autocorrelation (customer behavior changes gradually, not randomly)
- No account for seasonal patterns (holidays, weekends)
- Perfect customer segmentation (70%, 20%, 10%) unrealistic
- Feature distributions don't match expected real-world distributions

**Impact:**
- Model trained on synthetic data shows inflated performance (F1=0.86)
- Production model with real data will perform worse
- Overfitting to synthetic patterns

**Fix approach:**
- Generate synthetic data with overlapping patterns
- Add temporal dependencies between consecutive periods
- Use distribution fitting from sample data if available
- Add noise and outliers to synthetic features
- Validate synthetic data distribution matches business expectations


### Demo Script Lacks End-to-End Assertions

**Issue:** `test_churn_model.py` validates setup but not model functionality.

**Files:**
- `test_churn_model.py` lines 57-82

**What's tested:**
- Dependencies installed
- Data files exist
- Can load JSON

**What's NOT tested:**
- Model trains without error
- Model produces valid predictions
- Feature ranges are reasonable
- Model artifact files created with correct format
- Predictions are between 0-1 (probability)

**Fix approach:**
- Add assertions for model training success
- Validate prediction output format
- Check model file sizes reasonable (not 0 bytes)
- Verify feature importance sums to expected value


## Scaling Limits

### Memory Limit: Pandas Conversion at 1-2M Customers

**Current capacity:** Successfully handles ~1M customer records (560 MB)

**Limit:** Breaks at ~5-10M customers due to driver memory exhaustion

**Scaling path:**
- Use distributed ML training instead of pandas conversion
- Implement mini-batch training with sklearn_spark
- Use PySpark MLlib for random forest/gradient boosting
- Stream features directly to model without collecting


### No Partitioning Strategy for Large Datasets

**Issue:** Feature engineering code assumes all data fits in worker memory.

**Files:**
- `churn_rate_prediction.py` lines 414-491

**Problem:** Joins without partition hints may shuffle entire dataset:
- 10B events × 7 joins = massive shuffle operations
- No caching of intermediate DataFrames
- No partitioning strategy defined

**Improvement path:**
- Partition voice/data/recharge events by service_id before joins
- Cache intermediate DataFrames if used multiple times
- Add repartition hints for join optimization
- Use bucketing for efficient joins


### Feature Store Not Implemented

**Current approach:** Features computed fresh for each training run

**Bottleneck:** Re-computing 70 features from raw events every training cycle:
- Takes 30+ minutes for 1B events
- Wasteful if only training weekly but need daily predictions
- No feature versioning or lineage

**Scaling path:**
- Implement feature store (Feast, Tecton, or custom)
- Materialize features daily, reuse for multiple models
- Enable point-in-time lookups for backtesting
- Track feature version and compute lineage


## Dependencies at Risk

### imbalanced-learn SMOTE with Fixed k_neighbors

**Issue:** SMOTE configuration not adaptive to minority class size.

**Files:**
- `churn_rate_prediction.py` lines 576-578

**Details:**
```python
k_neighbors = min(5, minority_samples - 1)
smote = SMOTE(sampling_strategy=0.5, random_state=42, k_neighbors=k_neighbors)
```

**Risk:**
- If minority class has < 6 samples, k_neighbors = 0, SMOTE fails
- Fixed 0.5 sampling_strategy may not be optimal
- No cross-validation of SMOTE parameters

**Recommendation:**
- Implement fallback to random undersampling if minority < 6
- Make sampling_strategy configurable
- Document SMOTE decision rationale


### Multiple Incompatible Model Backends

**Issue:** 4 different ML frameworks with different behavior and APIs.

**Files:**
- sklearn (Logistic Regression, Random Forest)
- xgboost (XGBoost)
- lightgbm (LightGBM)

**Risk:**
- Different input requirements (scaled vs unscaled features)
- Different random_state parameter naming
- Different interpretability (coefficients vs feature_importances_)
- If one library breaks, need to maintain fallback

**Recommendation:**
- Standardize on single framework, or
- Use ONNX for model standardization
- Implement unified prediction interface


## Missing Critical Features

### No Feature Validation or Sanity Checks

**Problem:** Features computed without checking for:
- NaN/Infinity values
- Out-of-range percentages (>100%)
- Logical inconsistencies (recent < historical always)

**Files:** `churn_rate_prediction.py` lines 183-412

**Example bug potential:**
```python
# Line 296-301: outgoing_calls_drop_pct calculated
# If recent_outgoing_calls > historical_outgoing_calls, result is negative
# But line 270 in demo caps drop at 0 with max(0, ...)
# Production code doesn't cap - could create invalid features
```

**Recommendation:**
- Add data quality assertions post-aggregation
- Log feature distributions (min, max, mean, nulls)
- Add bounds checking: `assert df.select(F.max("outgoing_calls_drop_pct")).collect()[0][0] <= 100`


### No Model Explainability or Interpretability

**Problem:** Can't explain why model predicts churn for specific customer.

**Impact:**
- Can't defend model to business stakeholders
- Can't debug model failures
- Can't identify biased predictions

**Fix approach:**
- Add SHAP value calculation for top predictions
- Implement LIME for local interpretability
- Save feature importance per prediction
- Add audit log of all predictions with confidence scores


### No Production Monitoring or Alerting

**Problem:** No way to detect:
- Model performance degradation over time
- Data drift (feature distributions changing)
- Prediction distribution shifts
- Failed predictions or invalid inputs

**Recommendation:**
- Implement monitoring dashboard tracking key metrics
- Set up alerts for F1 score < threshold
- Monitor feature distributions vs baseline
- Log all predictions for analysis


---

**Concerns Summary:**

| Category | Count | Severity | Priority |
|----------|-------|----------|----------|
| Tech Debt | 3 | High | 1 |
| Known Bugs | 3 | Medium | 2 |
| Security | 2 | Medium | 3 |
| Performance | 4 | Medium | 2 |
| Fragile Areas | 4 | High | 1 |
| Test Gaps | 3 | High | 1 |
| Scaling Limits | 3 | Medium | 4 |
| Dependencies | 2 | Low | 5 |
| Missing Features | 3 | Medium | 3 |

**Total Issues Identified:** 27

*Concerns audit: 2026-03-15*
