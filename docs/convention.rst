.. _bi_doc_convention:

==============
Doc Convention
==============

This document states the convention that we follow for writing Documentation and especially docstrings for
classes and functions in Better Impression. This convention is based on
[https://www.python.org/dev/peps/pep-0008/](PEP8), with minor styling changes listed below.


Basics
------

* We use ``sphinx`` package to compile documentation.
* We use ``sphinx.autodoc`` extention to automatically parse the Python code and extract docstrings.
* ``.rst`` wins against ``.md``
* Make docstrings readable both in the code and in HTML. Use new lines and tabs to beautify the source of docstrings
  even if they are not really required.
* Richard wins against Winnie.


Common Sense Boosters
---------------------

* Document for humans. You will have to read this one day later.
* Read other good docs to notice some good practices.
* Do not be afraid to use some new markup features not yet used in our documentation.
* Create labels where required, if you feel that you will need it one day from other doc.
* Do not make pull requests if your docstrings do not compile in Sphinx without warnings.


Structure
---------

``index.rst`` of each Lambda should reside in ``./docs/``. It should have the basic Readme information and links
to the documentation of external components used in the Lambda. At the end of the file, you should include the
`autodoc` directives for each module of your function (the minimum of ``app.py``)

If you add schemas or images (good practice) include them in ``./docs/images/`` and use appropriately.


Example of Docstring
--------------------
.. code-block:: python

    def hello(name, age, tags=None):
        """
        User friendly welcome function.
        Uses `name` and `age` to salute user.
        This is still same line of documentation.

        While this is a new paragraph.
        Note that `rst` is sensitive to empty lines and spaces.

        Some code Example:

        .. code-block:: python

            def hello():
                return "world"

        This is paragraph 3.

        * And some bullet list
        * With couple rows

        Now go parameters. PyCharm adds them automatically.

        :param str name:    User name of type string.
        :param tags:        Types are not required, but this is a good
                            practice to show what you expect.

        :param age:         You can also specify multiple types, with a
                            little different syntax.
                            Note that empty lines do not matter here for
                            `sphinx`, but good for code readability.
        :type age:          int | float

        :rtype:             dict
        :return:            You can specify type of return value in
                            `rtype` (if it is uncertain).
        """

        return f"Hello {'bro' if age > 10 else 'kid'}"

I hope this example above was useful. Note the indention and spacing again. Now we are out of code-block.
Do not get frustrated with the 80 chars width that I used in the example. This is just to show this code-block nicely
when displayed as code in rendered HTML. Our convention is 120 characters max width.

--------

**Here is the compiled version of the example docstring from above:**

.. automodule:: docs.hello
   :members:

--------

**End of the compiled example.**



Configuration
-------------

There are some bugs with compiling documentation for components. Sphinx recognises the module components correctly,
but then in notices the same module during import from autodoc of lambdas. And fails to import manually.

* One workaround - create a symlink inside the lambdas (as init-lambda would normally do) and then include
  `:automodule:` for components directly in Lambdas index.

* Another option is to rename the components to smth else like `components-tmp` and compile the documentation for it.
  But you will have to take care about the links directly in the documentation of lambdas in the second case.
