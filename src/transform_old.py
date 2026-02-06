import pandas as pd
import numpy as np
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        logger.info("DataTransformer initialized")
    def transform_orders(self, df_orders):
        logger.info("Transforming orders data...")
        df = df_orders.copy()

        initial_rows = len(df)
        df = df.dropna(subset = ['order_id'])
        df['customer_id'] = df['customer_id'].fillna('UNKNOWN')
        df['order_status'].fillna('unknown', inplace=True)
        df['total_amount'] = df['total_amount'].fillna(0)

        logger.info(f"Dropped {initial_rows - len(df)} rows due to missing critical data")

        df['order_date']= pd.to_datetime(df['order_date'], errors='coerce')
        df['total_amount']= pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)

        initial_rows = len(df)
        df = df.drop_duplicates(subset=['order_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate orders")

        df['order_year'] = df['order_date'].dt.year
        df['order_month'] = df['order_date'].dt.month
        df['order_day'] = df['order_date'].dt.day
        df['order_day_name'] = df['order_date'].dt.day_name()

        df['order_status'] = df['order_status'].str.lower().str.strip()

        df = df[df['total_amount']>= 0]

        logger.info(f"Orders transformation completed: {len(df)} rows")
        return df

    def transform_customers(self, df_customers):
        logger.info("Transforming customers data...")
        df = df_customers.copy()

        df['email'].fillna('no-email@unknown.com', inplace=True)
        df = df.dropna(subset=['customer_id'])


        df['customer_name'] = df['customer_name'].str.title()
        df['city'] = df['city'].str.title()
        df['email'] = df['email'].str.lower()


        df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')

        initial_rows = len(df)
        df = df.drop_duplicates(subset=['customer_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate customers")

        df['registration_year'] = df['registration_date'].dt.year
        df['registration_month'] = df['registration_date'].dt.month

        logger.info(f"Customers transformation completed: {len(df)} rows")
        return df

    def transform_order_items(self, df_items):
        logger.info("Transforming order items data...")
        df = df_items.copy()

        df = df.dropna(subset=['order_item_id', 'order_id', 'product_id'])

        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce').fillna(0)

        initial_rows = len(df)
        df = df.drop_duplicates(subset=['order_item_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate order items")

        df['total_item_price'] = df['quantity'] * df['price_per_unit']

        df = df[df['quantity'] >= 0]
        df = df[df['price_per_unit'] >= 0]

        logger.info(f"Order items transformation completed: {len(df)} rows")
        return df

    def transform_products(self, df_products):
        logger.info("Transforming products data...")
        df = df_products.copy()

        df = df.dropna(subset=['product_id'])
        df['product_name'].fillna('Unknown Product', inplace=True)
        df['category'].fillna('Uncategorized', inplace=True)

        
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['stock'] = pd.to_numeric(df['stock'], errors='coerce').fillna(0)
        

        df['product_name'] = df['product_name'].str.title()
        df['category'] = df['category'].str.title()
        

        initial_rows = len(df)
        df = df.drop_duplicates(subset=['product_id'])
        logger.info(f"Removed {initial_rows - len(df)} duplicate products")

        logger.info(f"Products transformation completed: {len(df)} rows")
        return df

    def create_fact_sales(self, df_orders, df_order_items, df_customers, df_products):
        logger.info("Creating fact_sales table...")

        logger.info(f"Available columns in df_customers: {list(df_customers.columns)}")
        logger.info(f"Available columns in df_orders: {list(df_orders.columns)}")
        logger.info(f"Available columns in df_order_items: {list(df_order_items.columns)}")
        logger.info(f"Available columns in df_products: {list(df_products.columns)}")

       

        try:
            required_cols = ['customer_id', 'customer_name','city']
            available_cols = [col for col in required_cols if col in df_customers.columns]

            if not available_cols:
                logger.error(f"No matching columns found in df_customers!")
                logger.error(f"Required: {required_cols}")
                logger.error(f"Available: {list(df_customers.columns)}")
                raise ValueError("Cannot create fact table: missing customer columns")

            fact = df_orders.merge(
                df_customers[available_cols],
                on='customer_id',
                how= 'left'
            )
            logger.info(f"Joined with customers: {len(fact)} rows")
        except Exception as e:
            logger.error(f"error joining with customers: {str(e)}")
            raise

        
        try:
            required_cols = ['order_id', 'product_id','quantity', 'price_per_unit', 'total_item_price']
            available_cols = [col for col in required_cols if col in df_order_items.columns]

            fact = fact.merge(
                df_order_items[available_cols],
                on='order_id',
                how= 'left'
            )
            logger.info(f"Joined with order_items: {len(fact)} rows")
        except Exception as e:
            logger.error(f"error joining with order_items: {str(e)}")
            raise

        try:
            required_cols = ['product_id', 'product_name','category']
            available_cols = [col for col in required_cols if col in df_products.columns]

            fact = fact.merge(
                df_products[available_cols],
                on='product_id',
                how= 'left'
            )
            logger.info(f"Joined with products: {len(fact)} rows")
        except Exception as e:
            logger.error(f"error joining with products: {str(e)}")
            raise

        try:
            desired_columns = [
                'order_id','customer_id','customer_name','city',
                'product_id','product_name','category',
                'order_date','order_status',
                'quantity','price_per_unit','total_item_price',
                'order_year','order_month','order_day','order_day_name'
            ]

            final_columns = [col for col in desired_columns if col in fact.columns]

            logger.info(f"Final columns selected: {final_columns}")

            fact = fact[final_columns]

        except Exception as e:
            logger.error(f"Error selecting final columns: {str(e)}")
            logger.error(f"Available columns: {list(fact.columns)}")
            raise

        logger.info(f"Fact sales table created: {len(fact)} rows, {len(fact.columns)} columns")
        return fact

    def transform_all(self, raw_data):
        logger.info("Starting transformating of all data")
        transformed = {
            'orders': self.transform_orders(raw_data['orders']),
            'customers': self.transform_customers(raw_data['customers']),
            'order_items': self.transform_order_items(raw_data['order_items']),
            'products': self.transform_products(raw_data['products'])
        }

        transformed['fact_sales'] = self.create_fact_sales(
            transformed['orders'],
            transformed['order_items'],
            transformed['customers'],
            transformed['products']
        )

        logger.info("All transformations completed successfully")
        return transformed


if __name__ == "__main__":
    from extract import DataExtractor

    extractor = DataExtractor('../data/raw')
    raw_data = extractor.extract_all

    transformer = DataTransformer()
    transformed_data = transformer.transform_all(raw_data)

    print("\n=== DATA TRANSFORMATION SUMMARY ===")
    for name, df in transformed_data.items():
        print(f"\n{name.upper()}:")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        if len(df) > 0:
            print(f"  Sample data:")
            print(df.head(3))
