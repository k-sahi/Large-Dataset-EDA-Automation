# Large Dataset Exploratory Analysis Automation

## 1. Project Summary

This project provides a robust and automated framework for performing Exploratory Data Analysis (EDA) on datasets that are too large to fit into memory. It uses a modern, high-performance stack (DuckDB, SQL, Python) to overcome the common `MemoryError` issue encountered when using libraries like Pandas on massive files.

The core idea is to **bring the computation to the data**, not the other way around. By pre-aggregating and sampling the data with a high-speed analytical engine *before* loading it into Python, we can analyze datasets of virtually any size with minimal resource usage.

## 2. The Core Problem Solved

Attempting to load a multi-gigabyte CSV or Parquet file directly into a Pandas DataFrame (`df = pd.read_parquet(...)`) on a standard laptop or PC will often lead to:
- **Memory Errors:** The process crashes because it runs out of RAM.
- **Extreme Slowness:** Even if the data loads, every subsequent operation is painfully slow as it has to be performed on millions or billions of rows.
- **Repetitive Manual Work:** Manually writing code to generate standard plots (histograms, bar charts, correlations) for every new dataset or analytical question is tedious and time-consuming.

## 3. Our Solution: A Two-Part Strategy

This project implements a "divide and conquer" strategy:

1.  **The Heavy Lifter (DuckDB + SQL):** We use DuckDB, an in-process analytical database, to run SQL queries directly on the large data file on disk. This allows us to filter, aggregate, and sample the data with incredible speed without ever loading the entire file into memory. We only pull the small, summarized results into Python.

2.  **The Automation Engine (Python):** A Python script takes these small, manageable DataFrames and automatically generates a comprehensive EDA report, including:
    - A text summary with descriptive statistics.
    - Univariate plots (histograms, boxplots, count plots) for each column.
    - Bivariate plots (correlation heatmaps) to show relationships.
    - All plots and summaries are saved to a neatly organized output directory.

## 4. Technologies Used

- **Programming Language:** Python 3
- **Analytical Engine:** DuckDB
- **Data Manipulation:** Pandas
- **Data Visualization:** Matplotlib & Seaborn
- **File Format:** Parquet (for demonstration)

## 5. Getting Started

### Prerequisites

- Python 3.8 or newer.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd LargeDatasetEDA
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
    ```

3.  **Install the required libraries:**
    ```bash
    pip install -r requirements.txt
    ```
    *(You will need to create a `requirements.txt` file containing `duckdb`, `pandas`, `seaborn`, `matplotlib`, `faker`, and `pyarrow`)*

## 6. How to Use the Project

1.  **Generate the Sample Data:**
    First, run the data generation script to create the large `large_transactions.parquet` file that the main script will analyze.
    ```bash
    python generate_dummy_data.py
    ```

2.  **Run the Automated EDA:**
    Now, execute the main automation script. It will run the pre-defined SQL queries and generate the reports.
    ```bash
    python automated_eda_duckdb.py
    ```

3.  **Check the Output:**
    A new directory named `eda_report_duckdb` will be created. Inside, you will find all the generated summary files (`.txt`) and plots (`.png`), neatly named and organized by the analysis they represent (e.g., `duckdb_daily_sales_...`, `duckdb_category_performance_...`).

## 7. Customization for Your Own Projects

This framework is designed to be easily adapted:

-   **Use a different data file:** Simply change the `DATA_FILE` variable at the top of `automated_eda_duckdb.py`.
-   **Ask different questions:** The power of this project lies in SQL. You can write your own aggregation or sampling queries to analyze different aspects of your data. Just define your new query as a string and pass it to the `load_data_with_duckdb` function.
- 