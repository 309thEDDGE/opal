# OPAL testing

The main entrypoint for opal testing is test is `test_all.bash`.
This runs through tests:
  - tip (running the binary tests)
  - opal-packages
    - pytest for anything that lives in the `opal/opal-packages/tests` 
      directory.  These tests are run in both the OPAL singleuser and OPAL 
      torch python environments.  A writer of the tests can use 
      `sys.executable` value in python to condition on the environment until
      a more robust solution is put in place (see future directions below).
  - OPAL notebooks
    - starter notebooks in `opal/starter-notebooks` are run in both the 
      singleuser and torch OPAL environments.
    - test notebooks in `opal/test-notebooks` are run in both the singleuser
      and torch environment unless the test notebook name contains the string
      "singleuser" in which case it is only run in the OPAL singleuser 
      environment, and similarly for tests whose name contains "torch", for
      the OPAL torch environment.
    - demo notebooks in `opal/demo-notebooks`. These notebooks depend on data
      that live in the OPAL acceptance environment.  They behave exactly like
      the test notebooks except that they are not run by default when 
      `test_all.bash` is run.
  - test that the default environment in a new terminal is OPAL singleuser.

The tests also try to make sure that the `opal` packages from `opal-packages`
are available.

The tests listed above can be selectively disabled using command line 
arguments.  See `test_all.bash --help` for a complete list of options.

The correct invocation for the OPAL acceptance ceremony aught to be
`bash test_all.bash --acceptance`.  (*Note, that running the suite of 
notebooks takes a significant amount of time.*)


## future directions

The majority of the test-notebooks should be migrated to plain python (pytest)
tests.  The location of these tests is still undetermined.  And similarly, 
how we will differentiate the test suite based on the OPAL environment is an 
open question.

Any new package added to one of the OPAL environments should have a test
written that verifies its basic functionality. Preferably, as mentioned above,
this should be in the form of a python (pytest) test. 

Any new notebooks should follow the conventions specified above.  Starter 
notebooks should be placed within the `opal/starter-notebooks` directory, 
they should be executable in both the singleuser and torch environments, 
unless there is a compelling reason to change that. (Such a reason would 
necessitate a change in `test_all.bash`).  Any testing notebooks should be
placed in `opal/test-notebooks` with the correct naming convention.  If 
a notebook relies on the OPAL "acceptance" deployment, or are strictly for
demonstration purposes, they should be placed in `opal/demo-notebooks`.

The script `test_all.bash` is coming to the size and complexity where a 
rewrite, in say Python, might be a better choice.


