"""Codefresh Active Users via CLI.

Usage:
    active_users.py --key=<codefresh_api_key> [--url=<base_api_url>] [--days=<threshold_days>] [--months=<threshold_months>] [--exactminute] [--batch=<batch_size>] [--limit=<batch_limit>] [--interval=<stdout_update_interval>]
    active_users.py (-g | --help)
    active_users.py --version

Options:
    -h --help           Show this screen
    --version           Show version
    --api_key           Codefresh API key to use
    --base_api_url      API base url (default is https://g.codefresh.io/api)
    --threshold_days    Time peroid that an account should have logged in during to count as active (default is 0)
    --threshold_months  Time peroid (in months) that account should have logged in to count as active - any value here is added to threshold_days (default is 3)
    --batch_size        How many results should an API call return for processing each time? (default is 1000)
    --batch_limit       How many batches should we process (only used to limit calls for testing)
    --exactminute       If set, the current hour and minute will be used (for exactly x days), otherwise 'from midnight' is used.
"""

import yaml
from docopt import docopt
import requests
from pprint import pprint
from datetime import datetime
from dateutil.relativedelta import relativedelta

DB_LAST_LOGIN_FORMAT='%Y-%m-%dT%H:%M:%S.%fZ'

class ActiveUserCounter():
    def __init__(self, arg):
        self.user_records = {}
        self.api_key=arg['--key']
        assert( self.api_key is not None)
        self.api_url=arg['--url']
        if self.api_url is None:
            self.api_url = 'https://g.codefresh.io/api'
        if self.api_url[-1] != '/':
            self.api_url=self.api_url + '/'
        self.batch_size=arg['--batch']
        if self.batch_size is None:
            self.batch_size = 1000
        self.batch_limit=arg['--limit']
        if self.batch_limit is not None:
            self.batch_limit = int(self.batch_limit)
        self.active_days=arg['--days']
        if self.active_days is None:
            self.active_days = 0
        self.active_months=arg['--months']
        if self.active_months is None:
            self.active_months = 3
        self.exact_min = arg['--exactminute']
        self.last_log_output = datetime.now()


    def _api_call(self, api, page ):
        """ Internal function to format API calls and render to json """
        url = '{}{}?limit={}&page={}'.format( self.api_url, api, self.batch_size, page)
        try:
            r = requests.get( url, headers={'Authorization': self.api_key})
            return r.json() 
        except:
            print(r)


    def _fetch_users(self):
        """ first request will also return the total number of records we are dealing with,
            which is then used to configure how many times we need to loop"""
        total_pages = 10
        current_page = 1
        fields_to_save = ['_id', 'name', 'createdAt', 'updatedAt', 'last_login_date']
        while ( current_page <= total_pages ):
            api_return = self._api_call( 'admin/user', current_page)
            if self.batch_limit is None:
                total_pages = int(api_return['pages'])
            else:
                total_pages = min(int(api_return['pages']), self.batch_limit)
            current_page += 1
            for record in api_return['docs']:
                # lets extract the few bits of info we may need, and disregard the rest
                if '_id' in record:
                    self.user_records[record['_id']] = { x: record[x] for x in fields_to_save if x in record.keys()}
            print( "{}/{} accounts discovered".format(len(self.user_records), api_return['total']))
        return self.user_records


    def _save_users(self, filename="./users.yml"):
        """ save the fetched users to a file (used for testing or interactive analysis)"""
        with open( filename, 'w') as save_as:
            yaml.dump( self.user_records, save_as ) 


    def _load_users(self, filename="./users.yml"):
        """ load the previously fetched users from a file (used for testing or interactive analysis)"""
        with open( filename, 'r') as load_from:
            self.user_records = yaml.load( load_from )
            return self.user_records
            

    def _count_active_users(self):
        """Using the records of the last_login (if present),
           we then work out the date they need to have logged in after (active_months + active_days)
           The matching users are saved in self.active_user_list
           We also track how many users we encounter that have never logged in (field is blank), and
           save that as self.timeless_users
        """
        current_time = datetime.now()
        target_time = current_time - relativedelta(months = self.active_months, days = self.active_days )
        if self.exact_min:
            # measure from midnight..
            target_time.replace(hour = 0, minute = 0)
        # set up our record keeping
        self.active_user_list = {}
        self.timeless_users = 0
        # loop over each record
        for uuid, record in self.user_records.items():
            if 'last_login_date' in record:
                last_login = datetime.strptime( record['last_login_date'], DB_LAST_LOGIN_FORMAT )
                #if (current_time - last_login).total_seconds() < active_threshold_in_seconds:
                if last_login > target_time:
                    self.active_user_list[uuid] = record
            else: 
                self.timeless_users += 1


    def get_human_threshold(self):
        """ Using the active_months and active_days, this returns a human friendly string representation
            of exactly how far back we are looking
        """
        days = None
        months = None
        if self.active_months is not None and self.active_months > 0:
            months = "1 month" if self.active_months == 1 else "{} months".format( self.active_months )
        if self.active_days is not None and self.active_days > 0:
            days = "1 day" if self.active_days == 1 else "{} days".format( self.active_days )
        if days == None:
            return months
        if months == None:
            return days
        if days == None and months == None:
            return "[not set]"
        return "{} and {}".format( months, days )


    def start(self):
        """ Primary loop. 
            Fetches batch_size from the account API, up to batch_limit times (if set)
            Extracts the user records from the accounts.
            Loops over the user accounts to check last login and determine 'active users'
        """
        self._fetch_users()
        self._count_active_users()


if __name__=="__main__":
    arguements=docopt(__doc__, version="Codefresh Active Users via CLI 0.1.0")
    counter = ActiveUserCounter( arguements )
    counter.start()
    print('Threshold for active users is anyone who has logged in the in the {}'.format(counter.get_human_threshold()))
    print('{} users had invalid or no data for the last login date'.format(counter.timeless_users))
    print('{} active users found'.format(len(counter.active_user_list)))
    
