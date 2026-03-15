# Coding Conventions

**Analysis Date:** 2026-03-15

## Naming Patterns

**Files:**
- Lowercase with underscores: `churn_prediction_demo.py`, `churn_rate_prediction.py`, `test_churn_model.py`
- Descriptive names indicating purpose and domain

**Classes:**
- PascalCase: `SampleDataLoader`, `ChurnModelTrainer`, `ChurnFeatureEngineering`, `SilverLayerDataLoader`
- Suffixes indicate purpose: `*Loader`, `*Trainer`, `*Engineering`
- One main class per logical module, with related classes grouped in same file

**Functions:**
- snake_case: `load_json_file()`, `extract_agreements_data()`, `generate_sample_dataset()`, `train_models()`, `prepare_features()`
- Verb-first pattern for action methods: `load_*`, `extract_*`, `create_*`, `train_*`, `prepare_*`, `split_*`, `scale_*`
- Underscore-prefixed for helper/internal functions: `_` prefix not used; all methods are public or documented

**Variables:**
- snake_case: `data_dir`, `output_dir`, `feature_cols`, `X_train`, `y_test`, `recent_start`, `historical_end`
- Descriptive multi-word names for clarity
- Standard abbreviations: `X`, `y` for features and labels; `df` for DataFrames; `sm` for SMOTE; `cm` for confusion matrix
- Numeric suffixes for split variants: `X_train`, `X_test`, `X_train_scaled`, `X_train_balanced`

**Constants:**
- UPPERCASE with underscores: `OBSERVATION_DAYS=30`, `COMPARISON_DAYS=30`
- Numeric defaults in function signatures: `test_size=0.25`, `num_customers=1000`, `top_n=15`

## Code Style

**Formatting:**
- Python 3.10+ code style (no explicit formatter detected)
- Consistent indentation: 4 spaces
- Line length: generally ~80-100 characters, allows longer lines for readability
- Spacing: single blank line between methods; double blank line between classes
- String formatting: f-strings throughout (lines 34, 293, 327, 450, etc.)

**Linting:**
- No explicit linting config detected
- PEP 8 implicit compliance observed
- Warnings suppressed globally: `warnings.filterwarnings('ignore')` in both demo and production files

## Import Organization

**Order:**
1. Standard library imports: `os`, `sys`, `json`, `warnings`, `datetime`
2. Third-party data processing: `pandas`, `numpy`
3. Third-party ML libraries: `sklearn.*`, `xgboost`, `lightgbm`, `imblearn`
4. Visualization: `matplotlib.pyplot`, `seaborn`
5. Persistence: `joblib`
6. Apache Spark (production): `pyspark.sql`, `pyspark.sql.functions`, `pyspark.sql.types`

**Path Aliases:**
- Not used; relative imports from same directory preferred

**Example from `churn_rate_prediction.py` (lines 14-40):**
```python
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# ML Libraries
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score,
    roc_curve, precision_recall_curve, f1_score, precision_score, recall_score
)
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
import xgboost as xgb
import lightgbm as lgb
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import json
import warnings
warnings.filterwarnings('ignore')
```

## Error Handling

**Patterns:**
- Defensive null checks before processing: `if not agreement_json:` (lines 43, 61, 76, 106, 135 in demo)
- File existence checks: `if not os.path.exists(filepath):` (test_churn_model.py lines 40, 47)
- Try-except for critical import validation: `try: __import__(package) except ImportError:` (test_churn_model.py lines 15-17)
- Try-except for demo execution: `try: from churn_prediction_demo import SampleDataLoader except Exception as e:` (test_churn_model.py lines 61-82)
- Warning messages for data issues: `print(f"Warning: {filepath} not found")` rather than raising exceptions
- Graceful degradation with SMOTE sampling: `if minority_samples < 6: print(f"Warning: ..."); return X_train, y_train` (churn_rate_prediction.py lines 572-574)

**No custom exception classes:** Uses built-in exceptions and prints warnings

## Logging

**Framework:** `print()` statements only (no logging module)

**Patterns:**
- Progress output: `print("Generating synthetic dataset...")`, `print("Loading agreements from Silver layer")`
- Checkmark prefix for success: `print(f"✓ Generated dataset: {df.shape}")`, `print(f"✓ Model saved: {model_path}")`
- Cross/X prefix for errors: `print("❌ Missing packages:", ', '.join(missing))`
- Formatted section headers: `print("\n" + "="*60)` with centered titles
- Indented metadata output: `print(f"  Features: {len(feature_cols)}")`, `print(f"  Train: {X_train.shape[0]} samples")`
- Metric reporting: `print(f"F1: {rf_f1:.4f}, AUC: {rf_auc:.4f}")`
- No debug/info/error levels; print everything to stdout

**Example from `churn_prediction_demo.py` (lines 292-294):**
```python
print(f"✓ Generated dataset: {df.shape}")
print(f"  Churn rate: {df['churned'].mean()*100:.2f}%")
print(f"  Churned: {df['churned'].sum()}, Active: {(1-df['churned']).sum()}")
```

## Comments

**When to Comment:**
- Section headers for major feature groups: `# Voice features`, `# Data features`, `# Recharge features`, `# Product features`
- Business logic explanation: `# Simulate different customer behaviors`, `# Calculate drop percentages (KEY CHURN INDICATORS)`
- Customer type classifications: `# Active customers`, `# Declining customers (potential churners)`, `# Churning customers`
- Implementation notes for complex calculations: comments appear before calculation blocks

**Docstring/TSDoc:**
- Module-level docstrings describe purpose (lines 2-11 in both demo and production files)
- Class docstrings: Single-line summary in triple quotes, e.g., `"""Load and process sample JSON data"""`
- Method docstrings: Single-line summary only, e.g., `"""Load a single JSON file"""`
- No parameter documentation; signatures are self-documenting
- No return type documentation; return values evident from context

**Example from `churn_prediction_demo.py` (lines 24-31):**
```python
class SampleDataLoader:
    """Load and process sample JSON data"""

    def __init__(self, data_dir="data_samples"):
        self.data_dir = data_dir

    def load_json_file(self, filename):
        """Load a single JSON file"""
```

## Function Design

**Size:** Methods range from 3-40 lines; larger methods tend to be data aggregation with repetitive Spark operations

**Parameters:**
- Minimal parameters: 1-4 typical for instance methods
- Default parameters for optional config: `data_dir="data_samples"`, `test_size=0.25`, `num_customers=1000`, `output_dir="models/churn"`, `top_n=15`
- No *args/**kwargs usage; all parameters explicit

**Return Values:**
- Explicit returns: DataFrames, tuples, dicts
- Tuple returns for multi-value: `(X_train, X_test, y_train, y_test)`, `(X_train_balanced, y_train_balanced)`
- Dict returns for feature windows: `{'recent_start': ..., 'recent_end': ..., 'historical_start': ..., 'historical_end': ...}`
- Side effects: Methods that plot (save files) and train (modify self.models) return reference object
- No None returns except where data missing (null check patterns)

## Module Design

**Exports:**
- Classes are main exports: `SampleDataLoader`, `ChurnModelTrainer`, `ChurnFeatureEngineering`, `SilverLayerDataLoader`
- Entry point: `if __name__ == "__main__": main()` at end of files (churn_prediction_demo.py line 574, test_churn_model.py line 116, churn_rate_prediction.py line 1050+)
- Test validation: test_churn_model.py provides standalone testing without requiring full model

**Barrel Files:** Not used; single-responsibility files

**Instance State Management:**
- Classes maintain mutable state: `self.models = {}`, `self.best_model = None`, `self.scaler = None`, `self.feature_names = None`
- State accumulated across method calls: `train_models()` populates `self.models`, then `select_best_model()` updates `self.best_model`
- File I/O side effects in save methods: `save_model()` creates directory and writes files

---

*Convention analysis: 2026-03-15*
