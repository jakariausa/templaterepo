# Unit Tests for DataConnector

## Installation

The required dependencies should have installed when you installed the requirements.txt. However, you can verify you 
have the most up-to-date version by typing
```bash
pip install -U -r requirements.txt
```

## Running the tests

To run all tests for the connector, first, make sure you are first in your active environment for the connector.

Then, you can simply type
```bash
pytest --pyargs dc_sdk
```
into your terminal.

As the tests are running, the terminal will say indicate the progress of the tests by displaying either `.`, `F`, or
`E` to indicate whether each test passed, failed, or errored out, respectively. If the tests are taking a long time,
this can be a useful tool.

The console will display detailed results of each test with a shorter summary of all the test results at the bottom. The
console output will be quite lengthy, so remember to clear the terminal before the next run. 

## Modifying terminal output

### Selecting which functions to run
If your connector is just a source connector or destination connector, you don't have to see the not implemented 
functions tested. Just the argument `--source` or `--destination` when you are running the tests. For example, to run
just the functions associated with source connectors, I could just run
```bash
pytest --pyargs dc_sdk --source
```

To run only the tests for a certain function, instead of running `pytest --pyargs dc_sdk` in the terminal, run 
`pytest --pyargs dc_sdk -v -m ` and then the name of the function that is being tested. For example, if I were to test 
the authenticate() function, I would run
```bash
pytest --pyargs dc_sdk -v -m authenticate
```
and this would run all the tests associated with authentication.

The `-s` flag allows the functions run to show print statements. If the function should print to the console, then this
flag says it will.

### The -r Flag

The `-r` flag can be used to display a “short test summary info” at the end of the test session,
making it easy in large test suites to get a clear picture of all failures, skips, xfails, etc.

The `-r` options accepts a number of characters after it.
Here is the full list of available characters that can be used:

`f` - failed

`E` - error

`s` - skipped

`x` - xfailed

`X` - xpassed

`p` - passed

`P` - passed with output

`a` - all except passes

`A` - all

