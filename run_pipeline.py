import sys
import os 
import logging
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.data_quality import DataQualityChecker

log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f'etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S%")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers= [
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_etl_pipeline():
    try:
        logging.info("="*70)
        logging.info("ETL PIPELINE STARTED")
        logging.info("="*70)
        start_time = datetime.now()
        
        logging.info("\n [STEP 1/5] EXTRACTING DATA...")
        logging.info("="*70)

        raw_data_dir = os.path.join(os.path.dirname(__file__), 'data', 'raw')
        extractor = DataExtractor(raw_data_dir)
        raw_data = extractor.extract_all()

        logging.info(f"Extraction completed. Tables extracted: {list(raw_data.keys())}")

        logging.info("\n [STEP 2/5] TRANSFORMING DATA...")
        logging.info("-"*70)

        transformer = DataTransformer()
        transformed_data = transformer.transform_all(raw_data)

        logging.info(f"Transformation completed. Tables transformed: {list(transformed_data.keys())}")

        logging.info("\n [STEP 3/5] RUNNING DATA QUALITY CHECKS")
        logging.info("-"*70)

        checker = DataQualityChecker()

        checker.check_null_values(
            transformed_data['orders'],
            'orders',
            ['order_id', 'customer_id'],
            max_null_pct=5
        )
        checker.check_duplicates(transformed_data['orders'], 'orders', ['order_id'])
        checker.check_row_count(transformed_data['orders'], 'orders', min_rows=1)

        checker.check_null_values(
            transformed_data['customers'],
            'customers',
            ['customer_id'],
            max_null_pct=5
        )
        checker.check_duplicates(transformed_data['customers'], 'customers', ['customer_id'])
        
        checker.check_value_ranges(
            transformed_data['fact_sales'],
            'fact_sales',
            {
                'quantity': {'min': 0},
                'price_per_unit': {'min': 0} 
            }
        )
        checker.check_row_count(transformed_data['fact_sales'], 'fact_sales', min_rows=1)

        quality_report = checker.generate_report()
        logger.info(f"Quality checks completed: {quality_report['passed']}/{quality_report['total_checks']} passed")

        if quality_report['failed'] > 0:
            logger.warning(f" {quality_report['failed']} quality checks failed!")
            checker.print_report()

        
        logging.info("\n [STEP 4/5] LOADING DATA TO WAREHOUSE...")
        logging.info("-"*70)

        warehouse_db = os.path.join(
            os.path.dirname(__file__),
            'data',
            'warehouse',
            'ecommerce_warehouse.db'
        )

        loader = DataLoader(warehouse_db)
        loader.load_all(transformed_data)

        logger.info(f" Data Loaded to warehouse: {warehouse_db}")

        logging.info("\n [STEP 5/5] CREATING INDEXES...")
        logging.info("-"*70)

        loader.create_indexes()
        logger.info("Indexes created successfully")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "="*70)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration:.2f} seconds")


        table_info = loader.get_table_info()
        logger.info(f"\nWarehouse Tables:")
        for table, count in table_info.items():
            logger.info(f"  - {table}: {count} rows")
        
        logger.info("\n ETL pipeline execution successful!")
        logger.info(f"Log file: {log_file}")

        return True
    
    
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}", exc_info=True)
        return False
    
if __name__ == "__main__":
    success = run_etl_pipeline()

    if success:
        print("\n" + "🎉"*20)
        print("ETL PIPELINE BERHASIL DIJALANKAN!")
        print("🎉"*20)
        print("\nNext steps:")
        print("1. Check log file untuk detail execution")
        print("2. Explore data do warehouse dengan SQL queries")
        print("3. Buat Dashbouard atau visualisasi")
    else:
        print("\n ETL Pipeline Gagal. check log file untuk detail error.") 