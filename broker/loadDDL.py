#!/bin/env python3
import sys
import simplejson as json
import time
import datetime
import requests

URL = 'http://localhost:19002'

def feedRecord(filename):
    with open(filename) as f:
        lines=f.read()
        r = requests.get(URL + '/ddl', params={'ddl': lines},timeout=2)
        if r.status_code!=200:
            print('Request Fail'+str(r.text))
if __name__ == "__main__":
    feedRecord(sys.argv[1])
