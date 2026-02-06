import sys
import pandas as pd
sys.path.append('src')
from extract import DataExtractor
from transform import DataTransformer


extractor = DataExtractor('data/raw')
raw = extractor.extract_all()

print(f"RAW orders: {len(raw['orders'])}")
print("\nRaw orders data:")
print(raw['orders'][['order_id', 'customer_id', 'order_date','total_amount']])


transformer = DataTransformer()
transformed_orders = transformer.transform_orders(raw['orders'])

print(f"\nTRANSFORMED orders: {len(transformed_orders)} rows")
print("\nTransformed orders data:")
print(transformed_orders[['order_id','customer_id','order_date','total_amount']])

if len(raw['orders']) != len(transformed_orders):
    print(f"\n ⚠️ {len(raw['orders']) - len(transformed_orders)} rows were dropped!")

    raw_ids = set(raw['orders']['order_id'].dropna())
    transformed_ids = set(transformed_orders['order_id'])
    dropped_ids = raw_ids - transformed_ids

    print(f"Dropped order_ids: {dropped_ids}")

    print("\nDropped rows detail:")
    for oid in dropped_ids:
        row = raw['orders'][raw['orders']['order_id']]
        print(f"\n{oid}:")
        print(row.T)