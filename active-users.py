"""Codefresh Active Users via CLI.

Usage:
    active_users.py --key=<codefresh_api_key> [--url=<base_api_url>] [--threshold=<days>] [--batch=<batch_size>] [--limit=<batch_limit>] [--interval=<stdout_update_interval>]
    active_users.py (-g | --help)
    active_users.py --version

Options:
    -h --help           Show this screen
    --version           Show version
    --api_key           Codefresh API key to use
    --base_api_url      API base url (default is https://g.codefresh.io/api)
    --threshold_days    Time peroid that an account should have logged in during to count as active (default is 30)
    --batch_size        How many results should an API call return for processing each time? (default is 100)
    --batch_limt        How many batches should we process (only used to limit calls for testing)

"""

import yaml
from docopt import docopt
import requests
from pprint import pprint
from datetime import datetime

DB_LAST_LOGIN_FORMAT='%Y-%m-%dT%H:%M:%S.%fZ'

class ActiveUserCounter():
    def __init__(self, arguments):
        self.raw_account_buffer = None
        self.account_uuids = []
        self.user_records = {}
        self.api_key=arguments['--key']
        assert( self.api_key is not None)
        self.api_url=arguements['--url']
        if self.api_url is None:
            self.api_url = 'https://g.codefresh.io/api'
        if self.api_url[-1] != '/':
            self.api_url=self.api_url + '/'
        self.batch_size=arguements['--batch']
        if self.batch_size is None:
            self.batch_size = 100
        self.batch_limit=arguements['--limit']
        if self.batch_limit is not None:
            self.batch_limit = int(self.batch_limit)
        self.active_threshold=arguements['--threshold']
        if self.active_threshold is None:
            self.active_threshold = 30
        self.last_log_output = datetime.now()
    def _api_call(self, api, page ):
        """ Internal function to format API calls and render to json """
        url = '{}{}?limit={}&page={}'.format( self.api_url, api, self.batch_size, page)
        try:
            r = requests.get( url, headers={'Authorization': self.api_key})
            return r.json() 
        except:
            
            print(r)
    def _discover_accounts(self):
        # first request will also return the total number of records we are dealing with, which will update the below
        total_pages = 10
        current_page = 1
        while ( current_page <= total_pages ):
            api_return = self._api_call( 'admin/accounts', current_page)
            if self.batch_limit is None:
                total_pages = int(api_return['pages'])
            else:
                total_pages = min(int(api_return['pages']), self.batch_limit)
            current_page += 1
            for record in api_return['docs']:
                self.account_uuids.append(record['id'])
            print( "{}/{} accounts discovered".format(len(self.account_uuids), api_return['total']))

    def _discover_users(self):
        """Using the set of account UUIDs, we pull the user records for each account, reduce the record and store for processing"""
        account_uuid_processing_list = self.account_uuids.copy()
        while (len( account_uuid_processing_list )):
            current_account = account_uuid_processing_list.pop()
            current_page = 1
            total_pages = 1
            while ( current_page <= total_pages ):
                # get the users for the account
                api_return = self._api_call('accounts/{}/users'.format(account_uuid_processing_list.pop()), current_page)
                # allow for pages and looping through them if required
                if 'pages' in api_return:
                    total_pages = api_return['pages']
                current_page += 1
                # process the user list
                for record in api_return:
                    assert( '_id' in record)
                    self.user_records[record['_id']] = {
                        'email': record['email'] if 'email' in record else 'Undefined',
                        #'last_login': record['last_login_date'] if 'last_login_date' in record else 'None',
                        'userName': record['userName'] if 'username' in record else 'None',
                        }
                    if 'last_login_date' in record:
                        if 'last_login_date' != None:
                            if len('last_login_date')>5:
                                self.user_records[record['_id']]['last_login'] = record['last_login_date']
        print( '{} users discovered from {} accounts'.format( len( self.user_records ), len( self.account_uuids )))

    def _count_active_users(self):
        """Using the records of the last_login (if present) compared to the threshold, we determine how many users are currently active"""
        current_time = datetime.now()
        active_threshold_seconds = self.active_threshold * 24 * 60 * 60
        self.active_user_list = {}
        self.timeless_users = 0
        for uuid, record in self.user_records.items():
            if 'last_login' in record:
                last_login = datetime.strptime( record['last_login'], DB_LAST_LOGIN_FORMAT )
                if (current_time - last_login).total_seconds() < active_threshold_seconds:
                    self.active_user_list[uuid] = record
            else: 
                self.timeless_users += 1
                
    def start(self):
        """ Primary loop. 
            Fetches batch_size from the account API, up to batch_limit times (if set)
            Extracts the user records from the accounts.
            Loops over the user accounts to check last login and determine 'active users'
        """
        self._discover_accounts()
        self._discover_users()
        self._count_active_users()


if __name__=="__main__":
    arguements=docopt(__doc__, version="Codefresh Active Users via CLI 0.1.0")
    counter = ActiveUserCounter( arguements )
    counter.start()
    print('Threshold for active users is anyone who has logged in the in the {} days'.format(counter.active_threshold))
    print('{} users had invalid or no data for the last login date'.format(counter.timeless_users))
    print('{} active users found'.format(len(counter.active_user_list)))
    
