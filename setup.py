from setuptools import setup, find_packages


setup(name='sosw',
      version='0.3.7',
      description='Serverless Orchestrator of Serverless Workers',
      url='http://github.com/bimpression/sosw',
      author='Nikolay Grishchenko',
      author_email='nikolay@bimpression.com',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Operating System :: Other OS',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development'
      ],
      # packages=['sosw'],
      packages=find_packages(exclude=['docs']),
      install_requires=[
          'boto3>=1.9'
      ],
      zip_safe=False)
