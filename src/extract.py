import pandas as pd
import os
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        logger.info(f"DataExtractor initialized with data_dir: {data_dir}")
    def extract_orders(self):
        try:
            file_path = os.path.join(self.data_dir, "orders.csv")
            logger.info(f"Extracting orders from {file_path}")

            df=pd.read_csv(file_path, skipinitialspace=True)
            logger.info(f"Successfully extracted {len(df)} orders")
            return df
        except Exception as e:
            logger.error(f"Failed to extract orders: {str(e)}")
            raise

    def extract_customers(self):
        try:
            file_path = os.path.join(self.data_dir, "customers.csv")
            logger.info(f"Extracting customers from {file_path}")

            df = pd.read_csv(file_path, skipinitialspace=True)
            logger.info(f"Successfully Extracted {len(df)} customers")

            return df
        except Exception as e:
            logger.error(f"Error extracting customers: {str(e)}")
            raise

    def extract_order_item(self):
        try:
            file_path = os.path.join(self.data_dir, "order_item.csv")
            logger.info(f"Extracting order item from {file_path}")

            df = pd.read_csv(file_path, skipinitialspace=True)
            logger.info(f"Successfully extracted {len(df)} order item")

            return df
        except Exception as e:
            logger.error(f"Error extracting order item: {str(e)}")
            raise

    def extract_products(self):
        try:
            file_path = os.path.join(self.data_dir, "products.csv")
            logger.info(f"Extracting products from {file_path}")

            df = pd.read_csv(file_path, skipinitialspace=True)
            logger.info(f"Successfully Extracted {len(df)} products")

            return df
        except Exception as e:
            logger.error(f"Error extracting products: {str(e)}")
            raise

    def extract_all(self):
            logger.info("Starting extraction of all data sources")
            data = {
                'orders': self.extract_orders(),
                'customers': self.extract_customers(),
                'order_items': self.extract_order_item(),
                'products': self.extract_products()
            }

            logger.info(f"Successfully Extracted all data sources")
            return data


if __name__ == "__main__":
    extractor = DataExtractor('../data/raw')
    all_data = extractor.extract_all()

    print("\n=== DATA EXTRACTION SUMMARY ===")
    for name, df in all_data.items():
        print(f"\n{name.upper()}")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Sample data:")
        print(df.head(3))
