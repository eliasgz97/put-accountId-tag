import boto3
import pandas as pd
import os
import sys
import traceback

def readCSV(accountCSV):
    accountdf = pd.read_csv(accountCSV, delimiter=';')
    accountdf = parseProctorCSV(accountdf)
    accountIds = accountdf['_id'].tolist()
    institutionIds = accountdf['institution.id'].tolist()
    return accountIds,institutionIds

def createAccountIdTag(accountIds,institutionIds):
    try:
        secretsManager = boto3.client('secretsmanager', region_name='us-east-1')
        #secretName = 'EliasTestInStage'
        #accountId = 'test123'
        #secrets = []
        #response = secretsManager.list_secrets()
        #secrets.extend(response['SecretList'])
        id_mapping = dict(zip(accountIds, institutionIds))
        for key,value in id_mapping.items():
            try:
                response = secretsManager.get_secret_value(SecretId=value)
                tags = [
                    {
                        'Key': 'accountId',
                        'Value': key,
                    },
                ]
                secretsManager.tag_resource(
                    SecretId=value,
                    Tags=tags
                )
                print(f"Added accountId tag ({key}) to secret {value}")
            except secretsManager.exceptions.ResourceNotFoundException:
                print(f"Secret {value} does not exist.")
                continue
            except secretsManager.exceptions.InvalidRequestException as e:
                if "marked for deletion" in str(e):
                    print(f"Secret {value} is marked for deletion. Skipping...")
                else:
                    print(f"Error accessing secret {value}: {e}")
                continue
        
    except Exception as e:
        traceback.print_exc()


def parseProctorCSV(proctordf):
    try:
        print('Parsing mongoDb CSV...')
        newdf = proctordf.astype(str)
        for column in proctordf.columns:
            newdf[column] = newdf[column].str.replace('"', '').str.replace('ObjectId(','').str.replace(')', '')
        return newdf
    except Exception as e:
        print(f'An error ocurred while cleaning the ids in the accounts csv: {e}')
        sys.exit()    


def main():
    accountCSV = 'sumadi.accountsEU.csv'
    if os.path.isfile(accountCSV):
        accountIds,institutionIds = readCSV(accountCSV)
        createAccountIdTag(accountIds,institutionIds)
    else:
        print('The file does not exists.')

if __name__ == '__main__':
    main()