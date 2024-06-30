import json
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
        'init_clients': ['glue', 'dynamodb', 'iam'],
    }

    dynamodb_client: boto3.client = None
    glue_client: boto3.client = None
    iam_client: boto3.client = None



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


    def create_role_for_crawler(self, name: str) -> str:
        arn = ''

        logger.info("Creating assume role policy document")
        assume_role_policy_document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        })

        logger.info(f"Creating IAM role: {name}")
        response = self.iam_client.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=assume_role_policy_document,
            Path='/',
        )
        arn = response['Role']['Arn']
        logger.info(f"IAM role created with ARN: {arn}")

        logger.info(f"Attaching AWSGlueServiceRole policy to role: {name}")
        self.iam_client.attach_role_policy(
            RoleName=name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole',
        )

        inline_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "dynamodb:*",
                    "Resource": "arn:aws:dynamodb:*:*:table/*"
                }
            ]
        }

        logger.info(f"Putting inline policy to role: {name}")
        self.iam_client.put_role_policy(
            RoleName=name,
            PolicyName=f'{name}_inline_policy',
            PolicyDocument=json.dumps(inline_policy_document)
        )
        logger.info("Inline policy added successfully")

        return arn


    def create_crawler_for_ddb(self, tablename: str) -> str:
        crawler_name = f'ddb_{tablename}_crawler'
        if crawler_name in self.get_crawlers():
            logger.info("%s crawler already exists", crawler_name)
            return crawler_name

        self.glue_client.create_crawler(
            Name=crawler_name,
            Role=self.create_role_for_crawler(name=crawler_name),
            DatabaseName='ddb_tables',
            Targets={
                'DynamoDBTargets': [
                    {
                        'Path': tablename,
                    },
                ],
            }
        )
        logger.info("Created crawler %s", crawler_name)
        return crawler_name


    def get_crawlers(self) -> list:
        result = []
        response = self.glue_client.list_crawlers()
        if 'CrawlerNames' in response:
            result.extend(response['CrawlerNames'])

        while pagination_token := response.get('NextToken'):
            logger.debug("Paginating from token: %s", pagination_token)
            response = self.glue_client.list_crawlers(NextToken=pagination_token)
            if 'CrawlerNames' in response:
                result.extend(response['CrawlerNames'])
        logger.info("Found crawlers: %s", result)
        return result


    def create_crawlers_for_ddbs(self):
        tables = self.get_ddb_tables()
        for table in tables:

            self.create_crawler_for_ddb(table)


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

    glue_builder.create_crawlers_for_ddbs()
