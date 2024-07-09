.. _glue_crawlers_dynamodb:

AWS Glue Crawlers for DynamoDB
==============================

AWS Glue Crawlers is a powerful tool that can scan your data stores, extract metadata, and create tables in the
AWS Glue Data Catalog. This documentation provides an overview of using AWS Glue Crawlers to scan DynamoDB tables
and create corresponding tables in AWS Glue.

For more detailed instructions on adding a crawler, please refer to the `AWS Glue Documentation <https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html>`_.

Overview
--------

AWS Glue Crawlers simplify the process of cataloging your DynamoDB tables by automatically discovering and recording
the schema, data types, and other relevant metadata. This automation enables seamless integration of DynamoDB data
with other AWS services and allows for efficient querying and analysis using Amazon Athena, AWS Glue ETL jobs,
and other tools.

Glue DynamoDB Crawlers
----------------------

We introduce a new helper function that can automatically create and update Schemas in your AWS Glue Data Catalog.
By default it will scan all DynamoDB tables in the same region and rescan them every day.

`sys-glue-ddb-crawler <https://github.com/sosw/sosw-examples/tree/master/helper_lambdas/sys_glue_ddb_crawler>`_

`List of your crawlers <https://us-west-2.console.aws.amazon.com/glue/home#/v2/data-catalog/crawlers>`_

To install this function clone the repository, go to the relevant directory and use SAM:

..  code-block:: bash

    cd helper_lambdas/sys_glue_ddb_crawler
    sam build && sam deploy

To manually invoke the function:

..  code-block:: bash

    aws lambda invoke --function-name sys-glue-ddb-crawler \
                      --payload '{}' /tmp/sys-glue-ddb-crawler.log


#. Create if missing a ``ddb_tables`` Database in AWS Glue Data Catalog

#. Lists all existing DDB tables in the Region

#. For each table set up an individual Glue Crawler ``TABLE_NAME_crawler`` if missing

    * Each crawler has its own IAM role with access to a specific DDB table and Glue

    * Crawler creates an individual Glue table for each DDB table in the ``ddb_tables`` Glue Database

#. Run the Crawlers

    * Asynchronously invokes all crawlers

    * Scan DDB tables and update the Schemas in the AWS Glue Schema Repository and link them to the relevant
      AWS Glue tables

..  note::

    We don't create automatic schedules for crawlers, but we invoke this function with an AWS EventBridge Scheduler
    in order to find new tables and create new crawlers respectively.
