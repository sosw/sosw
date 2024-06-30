from setuptools import setup, find_packages


with open("README.md", "r") as f:
    long_description = f.read()

setup(name='sosw',
      version='0.7.49',
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
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Topic :: Software Development'
      ],
      packages=find_packages(exclude=['docs', 'test', 'examples', "*.test", "*.test.*"]),
      install_requires=[
          'boto3>=1.20'
      ])
