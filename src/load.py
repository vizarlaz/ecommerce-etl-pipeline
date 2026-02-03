import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, db_path):
        self.db_path = db_path

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        logger.info(f"DataLoader initialized with db_path: {db_path}")

    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def load_dataframe(self, df, table_name, if_exists='replace'):
        try:
            logger.info(f"Loading {len(df)} rows to table '{table_name}'")
            conn = self.get_connection()
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.close()
            logger.info(f"Successfully loaded data to '{table_name}'")
        except Exception as e:
            logger.error(f"Error loading data to '{table_name}':{str(e)}")
            raise
    def load_all(self, transformed_data):
        logger.info('Starting to load all data to warehouse')

        for table_name, df in transformed_data.items():
            self.load_dataframe(df, table_name, if_exists='replace')

        logger.info("All data loaded successfully to warehouse.")

    def get_table_info(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            table_info = {}
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                table_info[table_name] = count
            cursor.close()
            return table_info
        except Exception as e:
            logger.info(f"Error getting table info: {str(e)}")
            raise
    
    def execute_query(self, query):
        try:
            conn = self.get_connection()
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            logger.info(f"Error executing query: {str(e)}")
            raise
    def create_indexes(self):
        logger.info("Creating Indexes.....")

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON fact_sales(order_date)
                           """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_id ON fact_sales(customer_id)
                           """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON fact_sales(product_id)
                           """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fact_sales_category ON fact_sales(category)
                           """)
            
            conn.commit()
            conn.close()

            logger.info("Indexes created successfully")
        except Exception as e:
            logger.info(f"Error creating indexes: {str(e)}")
            raise

if __name__ == "__main__":
    from extract import DataExtractor
    from transform import DataTransformer

    extractor = DataExtractor('../data/raw')
    raw_data = extractor.extract_all()

    transformer = DataTransformer()
    transformed_data = transformer.transform_all(raw_data)

    loader = DataLoader('../data/warehouse/ecommerce_warehouse.db')
    loader.create_indexes()

    print("\n=== WAREHOUSE TABLES ===")
    table_info = loader.get_table_info()
    for table, count in table_info.items():
        print(f"{table}: {count} rows")
    
    
    print("\n=== SAMPLE QUERY: Top 5 products by Revenue ===")
    query = """
        SELECT
            product_name,
            category,
            SUM(total_item_price) as total_revenue,
            SUM(quantity) as total_quantity
        FROM fact_sales
        WHERE order_status = 'delivered'
        GROUP BY product_name, category
        ORDER BY total_revenue DESC
        LIMIT 5
    """
    result = loader.execute_query(query)
    print(result)
