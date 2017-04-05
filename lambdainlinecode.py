import os
from time import sleep
import boto3

def lambda_handler(event, context):

    if 'region' in event:
        region = event['region']

    if 'tables_to_clone' in event:
        tables_to_clone = event['tables_to_clone']

    if 'copy_data_from' in event:
        copy_data_from = event['copy_data_from']

    if 'suffix' not in event:
        return {
            'message': 'Provide at least the "suffix" property key. Exiting...'
        }

    suffix = event['suffix']

    print('*** Obtaning DynamoDB\'s client...')
    dydb_client = boto3.client('dynamodb')
    print('*** DynamoDB\'s client obtained...')

    try:
        tables = tables_to_clone
    except Exception:
        list_tables = dydb_client.list_tables()
        tables = {}
        for lst_tb in list_tables['TableNames']:
            if suffix not in lst_tb:
                tables[lst_tb] = lst_tb

    for table in tables:
        table_with_suffix = table + suffix
        try:
            dst_table = dydb_client.describe_table(
                TableName=table_with_suffix)['Table']
            print('Table %s already exists' % table_with_suffix)
            table_with_suffix = ""
            continue
        except Exception:
            # 1. Read and copy the target table to be copied
            print('*** Starting the clone procedure...')
            
            try:
                cp_table = dydb_client.describe_table(TableName=table)['Table']
            except Exception:
                print('*** Table %s doesn\'t exist' % table)
                continue

            try:
                local_secondary_indexes = cp_table['GlobalSecondaryIndexes']
                print('*** Copying Secondary Indexes from table %s' % table)
                
                new_array_ind = list()
                
                for sec_ind in local_secondary_indexes:
                    new_sec_ind = {}
                    
                    new_sec_ind['IndexName'] = sec_ind['IndexName']
                    new_sec_ind['KeySchema'] = sec_ind['KeySchema']
                    new_sec_ind['Projection'] = sec_ind['Projection']
                    new_sec_ind['ProvisionedThroughput'] = {}
                    new_sec_ind['ProvisionedThroughput']['ReadCapacityUnits'] = sec_ind['ProvisionedThroughput']['ReadCapacityUnits']
                    new_sec_ind['ProvisionedThroughput']['WriteCapacityUnits'] = sec_ind['ProvisionedThroughput']['WriteCapacityUnits']

                    new_array_ind.append(new_sec_ind)

                dst_table = dydb_client.create_table(
                    AttributeDefinitions=cp_table['AttributeDefinitions'],
                    TableName=table_with_suffix,
                    ProvisionedThroughput={
                        'ReadCapacityUnits': cp_table['ProvisionedThroughput']['ReadCapacityUnits'],
                        'WriteCapacityUnits': cp_table['ProvisionedThroughput']['WriteCapacityUnits'],
                    },
                    GlobalSecondaryIndexes=new_array_ind,
                    KeySchema=cp_table['KeySchema']
                )
            except KeyError:
                dst_table = dydb_client.create_table(
                    AttributeDefinitions=cp_table['AttributeDefinitions'],
                    TableName=table_with_suffix,
                    ProvisionedThroughput={
                        'ReadCapacityUnits': cp_table['ProvisionedThroughput']['ReadCapacityUnits'],
                        'WriteCapacityUnits': cp_table['ProvisionedThroughput']['WriteCapacityUnits'],
                    },
                    KeySchema=cp_table['KeySchema']
                )

            print('*** Waiting the new table %s to become active' %
                  table_with_suffix)
            sleep(5)
            while dydb_client.describe_table(TableName=table_with_suffix)['Table']['TableStatus'] != 'ACTIVE':
                sleep(3)

            print('*** Structure for table %s created' % table_with_suffix)

            try:
                if 'DISABLE_DATACOPY' not in os.environ:
                    if table in copy_data_from:

                        print('*** Copying data from %s to %s' %
                              (table, table_with_suffix))

                        scanned = dydb_client.scan(TableName=table)

                        for item in scanned['Items']:
                            dydb_client.put_item(
                                TableName=table_with_suffix,
                                Item=item
                            )
            except Exception:
                print('No data will be copied from %s to %s' %
                      (table, table_with_suffix))

    return 'Program finished'
