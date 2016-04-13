from flask import Flask
from flask import request
app = Flask(__name__)

#@app.route('/')
#def hello_world():
#    return 'Hello World!'

last_30=['0']*30
i=29

@app.route('/update', methods=['GET', 'POST'])
def update():
    global i
    global last_30
    query_string=request.args.get('statements')
    last_30[i]=query_string
    if (i==0):
        i=29
    i=i-1
    return ''


@app.route('/recent', methods=['GET', 'POST'])
def recent():
    global last_30
    result=''
    for item in last_30:
        result=result+'<br/> '+item
    return str(last_30)

if __name__ == '__main__':
    app.debug=True
    app.run(host='0.0.0.0')
