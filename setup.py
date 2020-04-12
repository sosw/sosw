from setuptools import setup, find_packages


with open("README.md", "r") as f:
    long_description = f.read()

setup(name='sosw',
      version='0.7.31',
      description='Serverless Orchestrator of Serverless Workers',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='http://github.com/sosw/sosw',
      author='Nikolay Grishchenko',
      author_email='ngr@sosw.app',
      license='MIT',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Operating System :: Other OS',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Topic :: Software Development'
      ],
      packages=find_packages(exclude=['docs', 'test', 'examples']),
      install_requires=[
          'boto3>=1.9'
      ],
      zip_safe=False)
