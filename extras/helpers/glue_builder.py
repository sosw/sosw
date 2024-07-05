import json
import logging
import time

import boto3
from sosw.app import Processor as SoswProcessor
from sosw.components.benchmark import benchmark
from sosw.components.helpers import recursive_matches_extract

try:
    from aws_lambda_powertools import Logger

    logger = Logger()

except ImportError:
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)


class GlueBuilder(SoswProcessor):
    DEFAULT_CONFIG = {
        'init_clients': ['glue', 'dynamodb', 'iam'],
        'glue_database_name': 'ddb_tables',
        'region': 'us-west-2',
    }
    dynamodb_client: boto3.client = None
    glue_client: boto3.client = None
    iam_client: boto3.client = None


    def get_config(self, name):
        return {}


    def __call__(self, event={}, **kwargs):
        super().__call__(event, **kwargs)

        self.create_glue_database()
        self.create_crawlers_for_ddbs()

        self.run_existing_crawlers()

        logger.info(self.get_stats())


    def list_glue_databases(self) -> list:
        databases = []

        paginator = self.glue_client.get_paginator('get_databases')
        for page in paginator.paginate():
            for database in page['DatabaseList']:
                databases.append(database['Name'])
                logger.debug("Found database: %s", database['Name'])

        logger.info("Databases %s", databases)
        return databases


    @benchmark
    def create_glue_database(self, name: str = None) -> str:
        name = name or self.config['glue_database_name']
        logger.info(name)
        if name in self.list_glue_databases():
            return name

        self.glue_client.create_database(
            DatabaseInput={
                'Name': name,
            },
        )
        return name


    @benchmark
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
        try:
            role = self.iam_client.get_role(
                RoleName=name,
            )
            if role:
                logger.info("Found existing role %s", role)
                return recursive_matches_extract(role, 'Role.Arn')
        except self.iam_client.exceptions.NoSuchEntityException:
            pass

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

        table_name = name[4:-8]  #hard_codding is always good
        inline_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "dynamodb:*",
                    "Resource": f"arn:aws:dynamodb:{self.config['region']}:{self._account}:table/{table_name}"
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

        for i in range(3):
            i += 1
            logger.info("Waiting for role to be created. Although IAM role is created and policy is attached, "
                        "boto3 glue client create_crawler function takes time to understand this.")
            time.sleep(i * 10)
            try:
                role = self.iam_client.get_role(
                    RoleName=name,
                )
                if role:
                    logger.info("Found existing role %s", role)
                    return recursive_matches_extract(role, 'Role.Arn')
            except self.iam_client.exceptions.NoSuchEntityException:
                pass


    @benchmark
    def create_crawler_for_ddb(self, tablename: str) -> str:
        crawler_name = f'ddb_{tablename}_crawler'
        if crawler_name in self.get_crawlers():
            logger.info("%s crawler already exists", crawler_name)
            return crawler_name

        my_role = self.create_role_for_crawler(name=crawler_name)
        logger.info(my_role)
        self.glue_client.create_crawler(
            Name=crawler_name,
            Role=my_role,
            DatabaseName=self.config['glue_database_name'],
            Targets={
                'DynamoDBTargets': [
                    {
                        'Path': tablename,
                    },
                ],
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE',
            }
        )
        logger.info("Created crawler %s", crawler_name)
        return crawler_name


    @benchmark
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


    @benchmark
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


    def run_existing_crawlers(self):
        crawlers = self.get_crawlers()
        for crawler in crawlers:
            self.glue_client.start_crawler(
                Name=crawler,
            )
            logger.info("Crawler %s started", crawler)


if __name__ == '__main__':
    logger.info("Application started")
    glue_builder = GlueBuilder()
    glue_builder()
