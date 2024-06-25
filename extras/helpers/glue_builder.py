import logging

import boto3

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("glue_operations.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger()
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)

session = boto3.Session(region_name='us-east-1')
glue_client = session.client('glue')

def list_databases() -> list:
    databases =[]

    paginator = glue_client.get_paginator('get_databases')
    for page in paginator.paginate():
        for database in page['DatabaseList']:
            databases.append(database['Name'])
            logger.debug(f"Found database: {database['Name']}")

    return databases


def list_tables(database: str) -> list:
    tables = []

    table_paginator = glue_client.get_paginator('get_tables')
    for table_page in table_paginator.paginate(DatabaseName=database):
        for table in table_page['TableList']:
            tables.append(table['Name'])
            logger.debug("Found table: %s in database: %s", table['Name'], database)

    logger.info("Completed getting all tables from AWS Glue")
    return tables


if __name__ == '__main__':
    logger.info("Application started")
    if dbs := list_databases():
        for db in dbs:
            tables = list_tables(db)
