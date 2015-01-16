# google-dfp-api-python

# Requirements:

1. googleads.yaml file is required. Sample is available with the script. make sure the googleads.yaml file is in the same directory as the exec_dfp.py script
2. The process requires a "processDir" folder for dumping data from Google. Edit the exec_dfp.py with the right processDir folder.
3. dfp_api.param file is required. Sample is available with the script.

# Usage:

The script takes 1 required parameter and 2 optional parameters
This script can be executed in 2 ways:

python exec_dfp.py -a lineitem
or
python exec_dfp.py -a lineitem -s "2014-07-14T00:00:00" -e "2014-07-15T00:00:00"

This tells the script to run incremental process between start time and end time.

python tt.py -h
usage: tt.py [-h] -a API_NAME [-s START_TIME] [-e END_TIME]

Generic script for DFP data download.

optional arguments:
  -h, --help            show this help message and exit
  -a API_NAME, --api_name API_NAME
                        This is the api name. The api should be one of this:
                        ratecard lica product proposal
                        customtargetingkeysandvalues advertiser order label
                        user customfield adunit adunitsize lineitem creative
  -s START_TIME, --start_time START_TIME
                        This is the start datetime for incremental pull, for
                        example: 2014-06-01T00:00:00
  -e END_TIME, --end_time END_TIME
                        This is the end datetime for incremental pull, for
                        example: 2014-06-01T00:00:00

# Information:

- The downloaded data file is created with ^Z delimiter (ctrl-v+ctrl-z)
- The pagesize is set as 500 (standard). there is one api pull where its optionally defined as 50000
- In some cases, the data has list objects. Those are flattened as created as multiple rows.
  
  Example: 
  orderID 123 has two secondary traffickers. The source gives a list.

  123,[4567,8910]

  The process writes the data as 2 rows. If has list of 3 values, it writes as 3 rows -- flattening the list object.
  123,4567
  123,8910

- Date object is converted to YYYY-MM-DD HH24:MI:SS for database usage. you can change the format in the _ConvertDateFormat function
