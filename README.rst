intercom
--------

Workbench module that fetches a user list from `Intercom
<https://www.intercom.com>`

Developing
----------

1. Get a Workbench dev environment running
2. Set up its `Intercom OAuth
<https://github.com/CJWorkbench/cjworkbench/wiki/Setting-up-a-development-environment#intercom-oauth>`
3. Clone this repository in a sibling directory
4. ``bin/dev develop-module intercom`` from the cjworkbench directory

To add a feature:

1. Run tests: ``python ./setup.py test`` and confirm tests pass
2. Write a new test in ``test_intercom.py``
3. Run ``python ./setup.py test`` and confirm your test breaks
4. Edit ``intercom.py`` to add the feature
5. Run ``python ./setup.py test`` and confirm your test passes
6. Commit and submit a pull request
