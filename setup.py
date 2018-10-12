from setuptools import setup


setup(name='sosww',
      version='0.1',
      description='Serverless Orchestrator of Serverless Workers - Worker client',
      url='http://github.com/bimpression/sosww',
      author='Nikolay Grishchenko',
      author_email='nikolay@bimpression.com',
      license='MIT',
      classifiers=[
          'Operating System :: Other OS',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development'
      ],
      packages=['sosww'],
      install_requires=[
          'boto3',
      ],
      zip_safe=False)
