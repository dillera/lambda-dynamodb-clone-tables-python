# lambda-dynamodb-clone-tables-python

Python script which clones all tables available in a DynamoDB instance or only the specified ones with a determined suffix.

Note: This script runs under Lambda Inline Code editor.

The only required parameter to be passed to the script is "suffix" which is appended to the table's name to be cloned.

Other parameters are:
- copy_data_from [not required]: array based values which tells to the script which tables should has their data copy as well, otherwise, 
only their structure will be cloned;

- tables_to_clone [not required]: array based values which specifies what table should be cloned and if it's not present, all tables will be cloned;

- region [not required]: it is only necessary when it's necessary to specify a different region (changes to the script must be applied before using it);

- sufix [required]: see description above;
