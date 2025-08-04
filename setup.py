from setuptools import setup, find_packages


with open("README.md", "r") as f:
    long_description = f.read()

setup(name='sosw',
      version='0.7.51',
      description='Serverless Orchestrator of Serverless Workers',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='http://github.com/sosw/sosw',
      author='Nikolay Grishchenko',
      author_email='ngr@sosw.app',
      license='MIT',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Operating System :: Other OS',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Programming Language :: Python :: 3.12',
          'Programming Language :: Python :: 3.13',
          'Topic :: Software Development'
      ],
      packages=find_packages(exclude=['docs', 'test', 'examples', "*.test", "*.test.*"]),
      install_requires=[
          'boto3>=1.35.0'
      ])
