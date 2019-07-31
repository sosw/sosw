from setuptools import setup, find_packages


with open("README.md", "r") as f:
    long_description = f.read()

setup(name='sosw',
      version='0.7.19',
      description='Serverless Orchestrator of Serverless Workers',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='http://github.com/bimpression/sosw',
      author='Nikolay Grishchenko',
      author_email='nikolay@bimpression.com',
      license='GNU General Public License v3',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Operating System :: Other OS',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Topic :: Software Development'
      ],
      # packages=['sosw'],
      packages=find_packages(exclude=['docs', 'test', 'examples']),
      install_requires=[
          'boto3>=1.9'
      ],
      zip_safe=False)
