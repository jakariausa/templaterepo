# Python 3.8 Connector

This template branch is used to create connectors for Python 3.8. 

## Installation

Get familiar with virtual environments and how they work. First, run this command to setup an environment:

```bash
python -m venv venv
```

or

```bash
python3 -m venv venv
```

Now we need to install the dependencies for the base connector. This will install several libraries so the app can function correctly. You should not edit any of the source files as they serve as building blocks for the connector. Run this command to activate your virtual environment:

Linux:

```bash
source venv/bin/activate
```

Windows:

```bash
./venv/Scripts/activate
```

Finally we will install the packages that are defined in requirements.txt. In order to deploy you must write to this file which will be explained in the deployment section. Run this command to install requirements.txt:

```bash
pip install -r requirements.txt
```

## Development

Now that we have installed the base libraries, we can now get to setting up our development environment. I have included a .example.env file that shows what the base .env file should include. If it doesn't have those specific variables, the connector won't work and throw an error.

Run this command to create a new .env file:

```bash
cp .env.example .env.credentials
```

Open the file connectors.yaml. This file is how we can specify connector specific resources that we can use throughout the template. The first two main ones we have so far is credentials and env. Credentials is what kind of objects are parsed in the encrypted credentials. For example, the Salesforce connector only needs a refresh token to authenticate. It's structure looks like this:

```json
{
    "token": "my refresh token"
}
```

So under salesforce we have in the list token. You will add your connector in the same way if it is not already in this file.

## Usage

Now that we have covered setting up your environment, we can now focus on actually writing code! You will mainly work out of the main.py folder. You can create any files that interact with main.py just make sure you follow correct guidelines to build a connector that will be functional.

Open `main.py` and have a look at the template. You will see in the run() method this is where you place your code. When the code is ran the inherited class will initialize the entire job and leave you with a couple of resources. Those resources are outlined below.

## Resources

#### Functions

`log_info(message)` - Log info message

`log_err(message)` - Log error message (will exit the connector)

`log_unhandled(message)` - Log unhandled message (will exit the connector)

`exit_no_fail(message)` - Exit with info message. Example: no rows returned from source so we exit but inform with "No rows returned"

`upload_rows(df)` - Uploads pandas dataframe to S3. (SourceConnector Class)

`run_batches()` - Retrieves all batches from S3 and returns them to call back function passed in.

`batch(df)` - Downloaded rows from S3. This function will run as many times as there are batches.

`run()` - Main run function of connector. Will execute your connector specific code. Any unhandled errors will get caught by `log_unhandled`

#### Variables

`row_count` - (int) variable to store row count of data

`source_columns` - (string list) of columns

`destination_columns` - (string list) of columns

`credentials` - (dict) of credentials used to authenticate to the connector

`job` - (class) JobDetails

- `job.ETLJobNM` - Name of the job
- `job.EncryptionCredentialTXT` - Encrypted Credentials
- `job.JobDataJSON` - Mapping Object to use for the job #
- `job.SourceObjectID` - Name of the source object #
- `job.DestinationObjectID` - Name of the destination object
- `job.ContainerNM` - Container name for next invoked function
- `job.VersionNBR` - Version of container
- `job.FilteredColumnNM` - Filtered Column Name #
- `job.StartSelectionNM` - Start Selection Category (Today, Yesterday, Custom, Today ) # 
- `job.StartValueTXT` - Start Selection Value #
- `job.EndSelectionNM` - End Selection Category #
- `job.EndValueTXT` - End Selection Value # 
- `job.TimezoneOffsetNBR` - Offset of timezone for filtering and scheduling. #
- `job.SourceConnectorNM` - Name of source connector
- `job.DestinationConnectorNM` - Name of destination connector
- `job.SourceConnectorID` - Source Connector ID
- `job.DestinationConnectorID` - Destination Connector ID
- `job.DestinationObjectID` - Destination Object ID
- `job.UpdateMethodCD` - Update method code. (1: Append, 2: Replace)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
