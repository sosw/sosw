.. title:: Lambda functions with ``sosw``

Quickstart
==========

Install latest stable version in your virtual environment:

``pip3 install sosw``

Create your ``app.py`` with the following structure.

..	note:: AWS STS is simply used as an example of how to automatically initialize boto3 clients.

..	code-block:: python

	import boto3
	import sosw

	from sosw.app import LambdaGlobals, get_lambda_handler, Processor as SoswProcessor


	class Processor(SoswProcessor):

		DEFAULT_CONFIG = {
		   'init_clients':    ['sts'],    # Automatically initialize AWS STS client
		}

		sts_client: boto3.client = None


		def __call__(self, event, **kwargs):
			data = self.get_self_identity()

			return data


		@benchmark  # Collect usage metrics
		def get_self_identity(self):
			return self.sts_client.get_caller_identity()


	global_vars = LambdaGlobals()
	lambda_handler = get_lambda_handler(Processor, global_vars)



Useful tools
------------
Create a sosw Lambda Layer for faster mounting: :ref:`SOSW Layer`


