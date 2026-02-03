import sys
import os 
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from load import DataLoader
import pandas as pd

def main():
    warehouse_db = os.path.join(
        os.path.dirname(__file__),
        'data',
        'warehouse',
        'ecommerce_warehouse.db'
    )

    loader = DataLoader(warehouse_db)

    print("\n" + "="*70)
    print("E-COMMERCE ANALYTICS DASHBOARD")
    print("="*70)

    print("\n 📊 TOP 5 PRODUCTS BY REVENUE")
    print("-"*70)

    query = """
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

    result = loader.execute_query(query)
    result['total_revenue'] = result['total_revenue'].apply(lambda x: f"Rp {x:,.0f}")
    print(result.to_string(index=False))


    print("\n 📊 SALES BY CATEGORY")
    print("-"*70)

    query = """
        SELECT
            category,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(quantity) as total_items,
            SUM(total_item_price) as total_revenue,
            AVG(total_item_price) AS avg_order_value
        FROM fact_sales
        WHERE order_status = 'delivered'
        GROUP BY category
        ORDER BY total_revenue DESC
    """
    result = loader.execute_query(query)
    result['total_revenue'] = result['total_revenue'].apply(lambda x: f"Rp {x:,.0f}")
    result['avg_order_value'] = result['avg_order_value'].apply(lambda x: f"Rp {x:,.0f}")
    print(result.to_string(index=False))


    print("\n 📊 SALES BY CITY")
    print("-"*70)

    query = """
        SELECT
            city,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(total_item_price) as total_revenue
        FROM fact_sales
        WHERE order_status = 'delivered'
        GROUP BY city
        ORDER BY total_revenue DESC
        LIMIT 5
    """
    result =loader.execute_query(query)
    result['total_revenue'] = result['total_revenue'].apply(lambda x: f"Rp {x:,.0f}")
    print(result.to_string(index=False))


    print("\n 📊 TOP 5 CUSTOMERS BY SPENDING")
    print("-"*70)

    query = """
        SELECT
            customer_name,
            city,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(total_item_price) as total_spent
        FROM fact_sales
        WHERE order_status = 'delivered
        GROUP BY customer_id, customer_name, city
        ORDER BY total_spent DESC
        LIMIT 5
    """

    result = loader.execute_query(query)
    result['total_spent'] = result['total_spent'].apply(lambda x: f"Rp {x:,.0f}")
    print(result.to_string(index=False))

    print("\n 📊 ORDER STATUS DISTRIBUTION")
    print("-"*70)

    query = """
        SELECT
            order_status,
            COUNT(DISTINCT order_id) as order_count,
            ROUND(COUNT(DISTINCT order_id) * 100.0 / 
            (SELECT COUNT(DISTINCT order_id) FROM fact_sales), 2) as percentage
        FROM fact_sales
        GROUP BY order_status
        ORDER BY order_count DESC
    """

    result = loader.execute_query(query)
    result['percentage'] = result['percentage'].apply(lambda x: f"{x}%")
    print(result.to_string(index=False))


    print("\n 📊 DAILY SALES TREND")
    print("-"*70)

    query = """
        SELECT
            DATE(order_date) as date,
            COUNT(DISTINCT order_id) as orders,
            SUM(total_item_price) as revenue
        FROM fact_sales
        WHERE order_status = 'delivered'
        GROUP BY DATE(order_date)
        ORDER BY date DESC
        LIMIT 10
    """

    result = loader.execute_query(query)
    result['revenue'] = result['revenue'].apply(lambda x: f"Rp {x:,.0f}")
    print(result.to_string(index=False))

    print("\n 📊 OVERALL SUMMARY METRICS")
    print("-"*70)

    query = """
        SELECT
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT customer_id) as total_customers,
            COUNT(DISTINCT product_id) as total_products,
            SUM(total_item_price) as total_revenue,
            AVG(total_item_price) as avg_order_value
        FROM fact_sales
        WHERE order_status = 'delivered'
    """

    result = loader.execute_query(query)

    print(f"Total Orders: {result['total_orders'].iloc[0]:,}")
    print(f"Total Customers: {result['total_customers'].iloc[0]:,}")
    print(f"Total Products: {result['total_products'].iloc[0]:,}")
    print(f"Total Revenue: {result['total_revenue'].iloc[0]:,.0f}")
    print(f"Average Order Value: Rp {result['avg_order_value'].iloc[0]:,.0f}")


    print("\n" + "="*70)
    print("Analytics completed successfully!")
    print("="*70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n Error: {str(e)}")
        print("Make sure you've run the ETL first: python run_pipeline.py")