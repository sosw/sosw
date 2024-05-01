.. _SOSW Layer:

========================
SOSW Layer
========================

A Lambda layer is a .zip file archive that contains supplementary code or data.
Layers usually contain library dependencies, a custom runtime, or configuration files.
https://docs.aws.amazon.com/lambda/latest/dg/chapter-layers.html

The following diagram represents how does Lambda Layer work.

..   figure:: img/lambda-layers-diagram.png
    :alt: Lambda Layer
    :align: center

------------------
How to deploy
------------------

Create ``deploy.sh`` file by using the next code:
`deploy.sh
<https://link_to_file.com>`_

------------------
How to run
------------------

While in the same folder as your ``deploy.sh`` file just run:

..   code-block:: bash

    ./deploy.sh [-v branch] [-p profile]

It will create a folder with all dependencies. You only need to pack it into zip-archive.

------------------
What's next?
------------------

You simply need to load Layer zip file to your bucket and deploy your
Layer with AWS SAM or CloudFormation by using one of the following templates.

------------------
CloudFormation
------------------

To deploy a layer by CloudFormation you will need to create a ``.yaml`` file with the following code

`sosw-layer.yaml
<https://link_to_file.com>`_

And after to create a stack in AWS CloudFormation.
To create a stack you run the ``aws cloudformation create-stack`` command.
You must provide the stack name, the location of a valid template, and any input parameters.

..   code-block:: bash

    aws cloudformation create-stack \
      --stack-name myteststack \
      --template-body file:///home/testuser/mytemplate.json \
      --parameters ParameterKey=Parm1,ParameterValue=test1 ParameterKey=Parm2,ParameterValue=test2

Or upload it directly via GUI.

------------------
AWS SAM
------------------

To deploy Layer with AWS SAM you will simply need to create two files ``samconfig.toml`` and ``template.yaml``
which are represented below.

`samconfig.toml
<https://link_to_file.com>`_

`template.yaml
<https://link_to_file.com>`_

After you create these files you can run them just by entering ``sam build && sam deploy`` in your console.

------------------
Versions
------------------

Lambda creates a new version of your function each time that you publish the function.
The new version is a copy of the unpublished version of the function.
The unpublished version is named ``$LATEST``.