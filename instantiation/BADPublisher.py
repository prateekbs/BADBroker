#!/bin/env python3
import sys
import simplejson as json
import time
import datetime
import requests

URL = 'http://104.131.132.18:19002'
flaskURL='http://104.131.132.18:5000'
expressURL='http://104.131.132.18:3000'


def feedRecord(filename):
    jsonfile = filename + '.json'
    lastOffset = 0
    currentTime = datetime.datetime.now()
    count=0
    with open(jsonfile) as f:
        for line in f.readlines():
            count+=1
            if (count>50):
                break
            record = json.loads(line)
            timestamp = None
            location = None
            impactZone = None
            time.sleep(0.6)
            if 'timeoffset' in record:
                timeOffset = record['timeoffset']
                timeOffset /= 100
                wait = timeOffset - lastOffset
                #time.sleep(wait)
                timestamp = currentTime + datetime.timedelta(seconds=timeOffset)
                timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
                lastOffset = timeOffset

            if 'impactZone' in record:
                iz = record['impactZone']
                impactZone = 'circle(\"%f,%f %f\")' % (iz['center']['x'], iz['center']['y'], iz['radius'])
                del record['impactZone']

            if 'location' in record:
                loc = record['location']
                location = 'point(\"{0}, {1}\")'.format(loc['x'], loc['y'])
                del record['location']


            recordString = json.dumps(record)
            recordString = recordString.replace('}', '')

            if impactZone:
                recordString = '{0}, \"impactZone\":{1}'.format(recordString, impactZone)

            if location:
                recordString = '{0}, \"location\":{1}'.format(recordString, location)

            if timestamp:
                recordString = '{0}, \"timestamp\": datetime(\"{1}\")'.format(recordString, timestamp)

            recordString = recordString + '}'
            print(recordString)
            stmt = 'use dataverse channels; insert into dataset %s [%s]' % (filename, recordString)

         
            r = requests.get(URL + '/update', params={'statements': stmt},timeout=5)

            if r.status_code != 200:
                print('Insertation failed, %s' % str(r.text))
            
            #flask_update=requests.get(flaskURL+'/update',params={'statements':stmt})
            try: 
                express_update=requests.get(expressURL+'/update',params={'statements':stmt},timeout=1)
            except:
                pass
if __name__ == "__main__":
    feedRecord(sys.argv[1])
