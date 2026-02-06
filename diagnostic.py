import sqlite3
import pandas as pd 
import os

print("="*70)
print("FULL DIAGNOSTIC - ETL PIPELINE")
print("="*70)


print("\n [1] CHECKING DATABASE FILE...")
db_path = 'data/warehouse/ecommerce_warehouse.db'

if os.path.exists(db_path):
    size = os.path.getsize(db_path)
    print(f"    Database exists: {db_path}")
    print(f"    Size: {size:,} bytes")
else:
    print(f"    Database NOT Found: {db_path}")
    print(" Run: python run_pipeline.py")
    exit(1)


print("\n[2] CONNECTING TO DATABASE...")
try:
    conn = sqlite3.connect(db_path)
    print(" Connection successful")
except Exception as e:
    print(f"    Connection failed: {e}")
    exit(1)


print("\n[3] LISTING TABLES...")
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"Found {len(tables)} tables")
for table in  tables:
    print(f"    - {table[0]}")



print("\n[4] CHECKING ROW COUNTS...")
for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"{table_name}: {count} rows")


print("\n[5] CHECKING fact_sales SCHEMA...")
cursor.execute("PRAGMA table_info(fact_sales);")
columns = cursor.fetchall()
print(f" fact_sales has {len(columns)} columns:")
for col in columns:
    col_id, col_name, col_type, not_null, default, pk = col
    print(f"[{col_id}]{col_name:20s}{col_type:10s}")



print("\n[6] SAMPLE DATA FROM fact_sales ...")
df = pd.read_sql_query("SELECT * FROM fact_sales LIMIT 3", conn)
print("\nFirst 3 rows:")
print(df)

print("\n\nColumn names in DataFrame:")
print(list(df.columns))

print("\nData types:")
print(df.dtypes)




print("\n[7] CHECKING FOR NULL VALUES IN KEY COLUMNS...")
null_check_query = """
    SELECT
        SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) as null_product_name,
        SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) as null_category,
        SUM(CASE WHEN total_item_price IS NULL THEN 1 ELSE 0 END) as null_total_item_price,
        SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) as null_quantity,
        SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) as null_order_status
    FROM fact_sales
"""
null_result = pd.read_sql_query(null_check_query)


print("\n[8] TESTING ANALYTICS QUERY 1 (Top Products)...")
query1 = """
    SELECT
        product_name,
        category,
        SUM(total_item_price) as total_revenue,
        SUM(quantity) as total_quantity,
        COUNT(DISTINCT order_id) as total_orders
    FROM fact_sales
    WHERE order_status = 'delivered'
    GROUP BY product_name, category
    ORDER BY total_revenue DESC
    LIMIT 5
"""
try:
    result1 = pd.read_sql_query(query1, conn)
    print(f"\n Query returned {len(result1)} rows")
    print("\nResults:")
    print(result1)

    if result1.empty:
        print("\n Query returned EMPTY result!")
        print("Checking why..")

        status_check = pd.read_sql_query(
            "SELECT DISTINCT order_status, COUNT(*) as count FROM fact_sales GROUP BY order_status",
            conn
        )
        print("\nOrder status distribution:")
        print(status_check)
except Exception as e:
    print(f"\nx Query FAILED: {e}")


print("\n[9] TESTING QUERY WITHOUT FILTER...")
query_no_filter = """
    SELECT
        product_name,
        category,
        SUM(total_item_price) as total_revenue,
        SUM(quantity) as total_quantity,
        COUNT(DISTINCT order_id) as total_orders
    FROM fact_sales
    GROUP BY product_name, category
    ORDER BY total_revenue DESC
    LIMIT 5
"""
try:
    result_no_filter = pd.read_sql_query(query_no_filter)
    print(f"\n Query returned {len(result_no_filter)} rows")
    print("\nResults (no filter):")
    print(result_no_filter)
except Exception as e:
    print(f"Query FAILED: {e}")

print("\n[10] CHECKING DATA QUALITY...")
quality_query = """
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT product_name) as unique_products,
        COUNT(DISTINCT category) as unique_categories,
        MIN(total_item_price) as min_price,
        MAX(total_item_price) as max_price,
        AVG(total_item_price) as avg_price
    FROM fact_sales
"""
quality_result = pd.read_sql_query(quality_query, conn)
print(quality_query)


conn.close()

print("\n" + "="*70)
print("DIAGNOSTIC COMPLETE")
print("="*70)
print("\nIf you see 'None' values in result above, the issue is:")
print("1. Columns have NULLs values")
print("2. Column names don't match")
print("3. Data type issues")
print("\nPlease share the output of this script forr further debugging.")
print("="*70)
