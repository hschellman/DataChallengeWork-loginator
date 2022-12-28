Notes on sphinx
---------------

This was my first sphinx project.

I started with the https://www.sphinx-doc.org/en/master/tutorial/getting-started.html tutorial so I have a similar structure.

I used the napoleon theme that Igor Mandrichenko used from https://metacat.readthedocs.io/en/latest/

rst format
**********

https://docutils.sourceforge.io/docs/user/rst/quickstart.html

Not my favorite -

  - the need to keep track of indentation and blank lines with poor error messages is painful

  - indentation differences when adding comments to code others wrote can be interesting

Github actions
**************

I set up github actions to build on my github page hschellman/DataChallengeWork-loginator which is based on Jake Calcutt's data dispatcher test system

The github actions had some issues:

  - It assumed the code was in main - I had to move to develop

  - I had to add a gh-pages branch by hand

  - I adapted `<https://github.com/ammaraskar/sphinx-action>`_ from Ammar Askar

  - The action can be found at `sphinx.yml <https://github.com/hschellman/DataChallengeWork-loginator/blob/develop/.github/workflows/sphinx.yml>`_

  - I was not able to get the programoutput extension to work as adding a pip install for it did not work. So command line syntax is added by hand.

  - I had to add a

    .. code-block:: python

      autodoc_mock_imports = ["metacat","samweb_client","data_dispatcher"]

    to the `conf.py`  to avoid errors due to imports of external code.  I just got blanks otherwise.
