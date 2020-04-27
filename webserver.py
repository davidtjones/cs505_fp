from flask import Flask, json

app = Flask(__name__)

@app.route('/api')
def hello_world():
    return 'Hello, World!'


# API management functions
@app.route('/api/getteam')
def get_team():
    info_json = {"team_name": "dtjo223",
                 "Team_members_sids":"10697704",
                 "app_status_code": "1"}
    return json.dumps(info_json)

@app.route('/api/reset')
def reset():
    return "not implemented :D"


# Real Time Reporting

@app.route('/api/zipalertlist')
def get_ziplist():
    return "not implemented :D"


@app.route('/api/alertlist')
def get_alertlist():
    return "not implemented :D"


@app.route('/api/testcount')
def get_testcount():
    return "not implemeneted :D"


# Logical and Operational Functions
@app.route('/api/of1')
def of1():
    # looks like this is where our subscriber needs to put new information
    return "not implemented"


@app.route('/api/getpatient/<mrn>')
def get_patient(mrn):
    return "not implemetned :D"


@app.route('/api/gethostpitalid/<id>')
def get_hospital_id(id):
    return "not implemented :D"
    
