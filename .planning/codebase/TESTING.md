# Testing Patterns

**Analysis Date:** 2026-03-15

## Test Framework

**Runner:**
- Custom pytest/unittest-style validation script
- No formal test framework (pytest, unittest, nose) detected
- Manual validation through executable test scripts

**Assertion Library:**
- Print-based assertions: `print("✓ All required packages installed")`
- Boolean return values from check functions: `return True/False`
- String matching for success/failure: "❌" prefix for failures, "✓" prefix for success

**Run Commands:**
```bash
python test_churn_model.py              # Run full validation test
python churn_prediction_demo.py         # Run demo with synthetic data
spark-submit churn_rate_prediction.py   # Run production model with Spark
```

## Test File Organization

**Location:**
- Separate test file in root: `test_churn_model.py`
- Test file located at `/Users/vagarwal/Documents/Projects/MTECH/lakehouse/test_churn_model.py`
- Tests mirror production code structure

**Naming:**
- Test file: `test_churn_model.py` (standard `test_*.py` pattern)
- Test functions: `check_dependencies()`, `check_data_files()`, `test_demo()`
- Descriptive function names indicating what is tested

**Structure:**
```
test_churn_model.py
├── check_dependencies()           # Validates package imports
├── check_data_files()             # Validates data file existence
├── test_demo()                    # Validates demo execution
└── main()                         # Orchestrates all checks
```

## Test Structure

**Suite Organization:**

From `test_churn_model.py` (lines 9-83):

```python
def check_dependencies():
    """Check if required packages are installed"""
    required = ['pandas', 'numpy', 'sklearn', 'xgboost', 'matplotlib', 'seaborn']
    missing = []

    for package in required:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)

    if missing:
        print("❌ Missing packages:", ', '.join(missing))
        return False

    print("✓ All required packages installed")
    return True

def check_data_files():
    """Check if sample data files exist"""
    data_dir = "data_samples"
    required_files = [
        'agreements.json',
        'products.json',
        'voice_usage_events.json',
        'data_usage_events.json',
        'recharge_events.json'
    ]

    if not os.path.exists(data_dir):
        print(f"❌ Data directory not found: {data_dir}")
        return False

    missing = []
    for file in required_files:
        filepath = os.path.join(data_dir, file)
        if not os.path.exists(filepath):
            missing.append(file)

    if missing:
        print("❌ Missing data files:", ', '.join(missing))
        return False

    print("✓ All sample data files present")
    return True

def test_demo():
    """Run a quick test of the demo"""
    print("\nRunning quick validation test...")

    try:
        from churn_prediction_demo import SampleDataLoader
        loader = SampleDataLoader(data_dir="data_samples")
        agreement = loader.load_json_file("agreements.json")

        if agreement:
            data = loader.extract_agreements_data(agreement)
            print(f"✓ Sample data loaded successfully")
            print(f"  Service ID: {data['service_id']}")
            print(f"  Agreement Type: {data['agreement_type']}")
            print(f"  Business Line: {data['business_line_type']}")
            return True
        else:
            print("❌ Failed to load sample data")
            return False
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False
```

**Patterns:**
- **Setup:** Import and instantiate classes at start of test
- **Execution:** Call method with real data
- **Assertion:** Check return values or data properties
- **Teardown:** No explicit cleanup (no file modification)

## Mocking

**Framework:** No mocking framework (unittest.mock, pytest-mock) detected

**Patterns:** Tests use real implementations; no mocked dependencies
- Real file I/O: Tests read actual JSON files from `data_samples/`
- Real imports: Direct import of module classes
- Real model execution: test_demo() calls full data loading pipeline

**What to Mock:**
- File I/O: When testing error paths without creating test data
- Spark operations: When running tests without Spark cluster
- External APIs: If integrating with Iceberg catalogs

**What NOT to Mock:**
- Core churn calculation logic
- Feature engineering transformations
- Model training steps (use small synthetic dataset instead)

## Fixtures and Factories

**Test Data:**

From `churn_prediction_demo.py` (lines 157-296), `SampleDataLoader.generate_sample_dataset()` acts as a factory:

```python
def generate_sample_dataset(self, num_customers=1000):
    """Generate a synthetic dataset based on sample structure"""

    np.random.seed(42)

    # Generate customer IDs
    service_ids = [f"59598492{str(i).zfill(4)}" for i in range(num_customers)]

    data = []

    for service_id in service_ids:
        # Simulate different customer behaviors
        customer_type = np.random.choice(['active', 'declining', 'churning'], p=[0.7, 0.2, 0.1])

        if customer_type == 'active':
            # Active customers (high usage)
            recent_calls = np.random.randint(20, 100)
            historical_calls = np.random.randint(15, 90)
            # ... more feature generation

        elif customer_type == 'declining':
            # Declining customers (usage dropping)
            historical_calls = np.random.randint(40, 100)
            recent_calls = int(historical_calls * np.random.uniform(0.3, 0.7))
            # ... more feature generation

        else:  # churning
            # Churning customers (minimal usage)
            recent_calls = np.random.randint(0, 10)
            # ... minimal features

        data.append({...})

    df = pd.DataFrame(data)
    return df
```

**Location:**
- `SampleDataLoader` in `/Users/vagarwal/Documents/Projects/MTECH/lakehouse/churn_prediction_demo.py` (lines 24-296)
- `SilverLayerDataLoader` in `/Users/vagarwal/Documents/Projects/MTECH/lakehouse/churn_rate_prediction.py` (lines 43-154)
- Real sample files in `data_samples/` directory

**Characteristics:**
- Deterministic: `np.random.seed(42)` ensures reproducibility
- Customer segments: Active (70%), declining (20%), churning (10%)
- Feature completeness: All churn indicators included (calls, data, recharges)
- Scalable: `num_customers` parameter controls dataset size

## Coverage

**Requirements:** Not enforced; no coverage reporting detected

**View Coverage:**
```bash
# Not configured; would use:
# pytest --cov=. test_churn_model.py
# Or coverage run -m pytest test_churn_model.py
```

**Current Coverage:**
- `test_churn_model.py`: Tests dependency availability, file existence, and basic data loading (entry point validation)
- `churn_prediction_demo.py`: Includes full training pipeline execution as "demo" rather than unit test
- `churn_rate_prediction.py`: Production code; tested manually via spark-submit

## Test Types

**Unit Tests:**
- Implicit: Check individual functions like `check_dependencies()`, `check_data_files()`
- Scope: Single function validation
- Approach: Boolean return values; no assertions

**Integration Tests:**
- Implicit: `test_demo()` validates end-to-end data loading + extraction + instantiation
- Scope: Multiple components (DataLoader, JSON parsing, class instantiation)
- Approach: Real file I/O, imports actual classes

**E2E Tests:**
- `churn_prediction_demo.py` (lines 521-575): Full pipeline execution
  - Load data → Generate synthetic dataset → Train models → Evaluate → Save artifacts
  - Produces output visualizations and model files
- Entry point: `if __name__ == "__main__": main()`
- Not automated; manual execution required

## Common Patterns

**Async Testing:**
- Not applicable; no async code

**Error Testing:**

From `test_churn_model.py` (lines 40-55, 61-82):

```python
# Test missing directory
if not os.path.exists(data_dir):
    print(f"❌ Data directory not found: {data_dir}")
    return False

# Test exception handling
try:
    from churn_prediction_demo import SampleDataLoader
    loader = SampleDataLoader(data_dir="data_samples")
    agreement = loader.load_json_file("agreements.json")

    if agreement:
        # success path
        return True
    else:
        print("❌ Failed to load sample data")
        return False

except Exception as e:
    print(f"❌ Test failed: {str(e)}")
    return False
```

**Pattern:** Try-except wraps module import and execution; catch broad Exception for validation

## Test Execution Flow

**Validation Test** (`test_churn_model.py`):

1. Check dependencies (lines 9-27)
   - Try to import each required package
   - Return False if any ImportError

2. Check data files (lines 29-55)
   - Verify `data_samples/` directory exists
   - Verify all 5 required JSON files present
   - Return False if missing

3. Run demo integration test (lines 57-82)
   - Import SampleDataLoader from production code
   - Load real agreements.json file
   - Extract and display data
   - Verify output matches expected structure

4. Main orchestration (lines 84-113)
   - Run checks sequentially: `deps_ok`, then `data_ok`, then `demo_ok`
   - Exit with code 1 if any check fails
   - Print success message if all pass
   - Suggest next commands: `python churn_prediction_demo.py` or `spark-submit churn_rate_prediction.py`

## Test Invocation

**From Repository Root:**
```bash
# Prerequisite check
python test_churn_model.py

# Demo execution (local, with synthetic data)
python churn_prediction_demo.py

# Production execution (Spark cluster, with Iceberg tables)
spark-submit churn_rate_prediction.py
```

**Success Criteria:**
- Dependencies installed
- Data files present
- Sample data loads and parses correctly
- Demo training completes without exception
- Models, scalers, and metadata saved to disk

---

*Testing analysis: 2026-03-15*
