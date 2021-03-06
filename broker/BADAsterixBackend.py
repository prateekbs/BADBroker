import tornado.httpclient
import json

# Test register


def service_call(URL, service_point, post_data):
    http_client = tornado.httpclient.HTTPClient()
    try:        
        print(json.dumps(post_data))
        
        request = tornado.httpclient.HTTPRequest(URL + "/" + service_point, 
                                        method='POST',
                                        body=json.dumps(post_data))
        response = http_client.fetch(request)
        return json.loads(str(response.body, encoding='utf-8'))
        
    except tornado.httpclient.HTTPError as e:
        # HTTPError is raised for non-200 responses; the response
        # can be found in e.response.
        print("Error: " + str(e) + " " + str(e.response))
    except Exception as e:
        # Other errors are possible, such as IOError.
        print("Error: " + str(e))
    
    http_client.close()


URL = "http://localhost:8989"

"""
response = service_call(URL, "register", {'username' : 'yusuf', 'email': 'rer@fdfd.com', 'password': 'pass'})

if response['status'] != 'success':
    print response['error']

"""

channelName = 'nearbyTweetChannel'
brokerName = 'brokerA'

response = service_call(URL, "notifybroker", {
    'dataverseName': 'channels',
    'channelName': channelName,
})

print(json.dumps(response))

