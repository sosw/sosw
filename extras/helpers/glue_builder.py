import logging

import boto3

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("glue_operations.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)

glue_client = boto3.client('glue')


def list_of_tables():
    tables = []

    logger.info("Starting to get databases from AWS Glue")
    paginator = glue_client.get_paginator('get_databases')
    for page in paginator.paginate():
        for database in page['DatabaseList']:
            database_name = database['Name']
            logger.debug(f"Found database: {database_name}")

            table_paginator = glue_client.get_paginator('get_tables')
            for table_page in table_paginator.paginate(DatabaseName=database_name):
                for table in table_page['TableList']:
                    table_name = table['Name']
                    tables.append({
                        'Database': database_name,
                        'Table': table_name
                    })
                    logger.debug(f"Found table: {table_name} in database: {database_name}")

    logger.info("Completed getting all tables from AWS Glue")
    return tables


if __name__ == '__main__':
    logger.info("Application started")
    try:
        all_tables = list_of_tables()
        for table in all_tables:
            print(f"Database: {table['Database']}, Table: {table['Table']}")
            logger.info(f"Database: {table['Database']}, Table: {table['Table']}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
