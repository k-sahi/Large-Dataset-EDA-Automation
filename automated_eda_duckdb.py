import os
import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import io  # Needed for capturing df.info()
import numpy as np

# --- Configuration ---
# 1. Data File (must be generated first)
DATA_FILE = "large_transactions.parquet"

# 2. Output Directory for Plots
OUTPUT_DIR = "eda_report_duckdb"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_data_with_duckdb(sql_query: str, db_connection) -> pd.DataFrame:
    """Executes a SQL query using DuckDB and returns a Pandas DataFrame."""
    print(f"Executing query with DuckDB...")
    # .fetchdf() is a convenient way to get a Pandas DataFrame directly
    df = db_connection.execute(sql_query).fetchdf()
    print(f"Data loaded successfully. Shape: {df.shape}")
    return df


def automated_exploratory_analysis(df: pd.DataFrame, report_title: str):
    """
    Performs automated EDA on a given DataFrame and saves plots.
    """
    print(f"\n--- Generating EDA Report for: {report_title} ---")

    # 1. Basic Information
    print("\n1. Basic Information")
    with open(os.path.join(OUTPUT_DIR, f"{report_title}_summary.txt"), "w") as f:
        f.write(f"--- EDA Report for: {report_title} ---\n\n")
        f.write("1. Basic Information\n")
        f.write(f"Shape of the dataset: {df.shape}\n\n")

        string_buffer = io.StringIO()
        df.info(buf=string_buffer)
        f.write("Data Types and Non-Null Values:\n")
        f.write(string_buffer.getvalue() + "\n\n")

        f.write("2. Descriptive Statistics (Numerical)\n")
        f.write(df.describe(include=np.number).to_string() + "\n\n")

        # --- THIS IS THE CORRECTED SECTION ---
        f.write("3. Descriptive Statistics (Categorical)\n")
        f.write("---------------------------------------\n")
        categorical_cols_for_summary = df.select_dtypes(include=['object', 'category']).columns
        if not categorical_cols_for_summary.empty:
            f.write(df[categorical_cols_for_summary].describe().to_string() + "\n\n")
        else:
            f.write("No categorical columns found in this dataset.\n\n")
        # --- END OF CORRECTION ---

    # 2. Missing Values Analysis
    missing_values = df.isnull().sum()
    missing_values = missing_values[missing_values > 0]
    if not missing_values.empty:
        plt.figure(figsize=(12, 6))
        missing_values.plot(kind='bar')
        plt.title('Missing Values Count per Column')
        plt.savefig(os.path.join(OUTPUT_DIR, f"{report_title}_missing_values.png"))
        plt.close()

    # 3. Identify Column Types
    numerical_cols = df.select_dtypes(include=np.number).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()

    # 4. Univariate Analysis
    for col in numerical_cols:
        plt.figure(figsize=(14, 5));
        plt.subplot(1, 2, 1);
        sns.histplot(df[col], kde=True);
        plt.title(f'Histogram of {col}');
        plt.subplot(1, 2, 2);
        sns.boxplot(x=df[col]);
        plt.title(f'Boxplot of {col}');
        plt.tight_layout();
        plt.savefig(os.path.join(OUTPUT_DIR, f"{report_title}_univariate_{col}.png"));
        plt.close()
    for col in categorical_cols:
        plt.figure(figsize=(12, 7));
        top_cats = df[col].value_counts().nlargest(20).index;
        sns.countplot(y=col, data=df, order=top_cats, orient='h');
        plt.title(f'Count Plot of {col} (Top 20)');
        plt.tight_layout();
        plt.savefig(os.path.join(OUTPUT_DIR, f"{report_title}_univariate_{col}.png"));
        plt.close()

    # 5. Bivariate Analysis (Correlation Matrix)
    if len(numerical_cols) > 1:
        plt.figure(figsize=(12, 10));
        correlation_matrix = df[numerical_cols].corr();
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f");
        plt.title('Correlation Matrix');
        plt.savefig(os.path.join(OUTPUT_DIR, f"{report_title}_correlation_matrix.png"));
        plt.close()
    print(f"--- EDA Report for {report_title} Complete. Files saved to '{OUTPUT_DIR}'. ---")

if __name__ == '__main__':
    if not os.path.exists(DATA_FILE):
        print(f"Error: Data file '{DATA_FILE}' not found.")
        print("Please run 'python generate_dummy_data.py' first.")
        exit()

    # Connect to an in-memory DuckDB database
    # This is ephemeral and exists only for the duration of the script
    con = duckdb.connect(database=':memory:', read_only=False)

    # --- Define SQL queries that run DIRECTLY on the Parquet file ---

    # Query 1: Daily Sales Aggregation
    daily_sales_query = f"""
        SELECT
            CAST(transaction_date AS DATE) AS sale_date,
            SUM(quantity * price_per_item) AS total_revenue,
            COUNT(DISTINCT transaction_id) AS number_of_orders,
            AVG(quantity * price_per_item) AS average_order_value
        FROM '{DATA_FILE}'
        GROUP BY sale_date
        ORDER BY sale_date;
    """

    # Query 2: Category Performance
    category_performance_query = f"""
        SELECT
            product_category,
            store_location,
            COUNT(*) AS transactions_count,
            SUM(quantity) AS total_items_sold,
            ROUND(AVG(price_per_item), 2) as avg_item_price
        FROM '{DATA_FILE}'
        GROUP BY product_category, store_location;
    """

    # Query 3 (Optional): Sample of Raw Data
    # DuckDB's syntax for sampling is clean and efficient.
    raw_sample_query = f"""
        SELECT * FROM '{DATA_FILE}' USING SAMPLE 0.01 PERCENT; -- Sample 10,000 rows
    """

    # --- Run Analysis on the Daily Sales Aggregation ---
    df_daily_sales = load_data_with_duckdb(daily_sales_query, con)
    if 'sale_date' in df_daily_sales.columns:
        df_daily_sales['sale_date'] = pd.to_datetime(df_daily_sales['sale_date'])
    automated_exploratory_analysis(df_daily_sales, "duckdb_daily_sales")

    # --- Run Analysis on the Category Performance Aggregation ---
    df_category = load_data_with_duckdb(category_performance_query, con)
    automated_exploratory_analysis(df_category, "duckdb_category_performance")

    # --- Close the DuckDB connection ---
    con.close()