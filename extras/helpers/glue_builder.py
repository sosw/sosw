import logging

import boto3
from sosw.app import Processor as SoswProcessor


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("glue_operations.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger()
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)


class GlueBuilder(SoswProcessor):
    DEFAULT_CONFIG = {
        'init_clients': ['glue', 'dynamodb'],
    }

    glue_client: boto3.client = None
    dynamodb_client: boto3.client = None


    def list_glue_databases(self) -> list:
        databases =[]

        paginator = self.glue_client.get_paginator('get_databases')
        for page in paginator.paginate():
            for database in page['DatabaseList']:
                databases.append(database['Name'])
                logger.debug("Found database: %s", database['Name'])

        return databases


    def list_glue_tables(self, database: str) -> list:
        tables = []

        table_paginator = self.glue_client.get_paginator('get_tables')
        for table_page in table_paginator.paginate(DatabaseName=database):
            for table in table_page['TableList']:
                tables.append(table['Name'])
                logger.debug("Found table: %s in database: %s", table['Name'], database)

        logger.info("Completed getting all tables from AWS Glue")
        return tables

    def create_crawlers_for_ddbs(self):
        pass


    def get_ddb_tables(self) -> list:
        result = []
        response = self.dynamodb_client.list_tables()
        if 'TableNames' in response:
            result.extend(response['TableNames'])

        while pagination_token := response.get('LastEvaluatedTableName'):
            logger.debug("Paginating from token: %s", pagination_token)
            response = self.dynamodb_client.list_tables(ExclusiveStartTableName=pagination_token)
            if 'TableNames' in response:
                result.extend(response['TableNames'])
        logger.info("Found ddb tables: %s", result)
        return result


if __name__ == '__main__':
    logger.info("Application started")
    glue_builder = GlueBuilder()
    glue_builder.get_ddb_tables()

    if dbs := glue_builder.list_glue_databases():
        for db in dbs:
            tables = glue_builder.list_glue_tables(db)
