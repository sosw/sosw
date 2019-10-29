Serverless Application Model

.. code-block:: bash

   # Install environment with SAM.
   cd /var/app/sosw/examples
   pipenv install
   pipenv shell

   cd sam
   sam package --template-file template.yaml --s3-bucket sosw-s3-000000000000 --output-template-file packaged.yaml
   sam publish --template packaged.yaml

