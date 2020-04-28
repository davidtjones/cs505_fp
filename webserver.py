from flask import Flask, json, request
import pandas as pd
import code
import config
from database.functional import *

app = Flask(__name__)

def get_driver():
    driver = GraphDatabase.driver(
        config.neo4j_cred['uri'],
        auth=(
            config.neo4j_cred['username'],
            config.neo4j_cred['password']),
        encrypted=False
    )
    
    return driver

@app.route('/api')
def hello_world():
    return "This is the API for David Jones' CS 505 Final Project!"


# API management functions
@app.route('/api/getteam')
def get_team():
    driver = get_driver()
    status = check_online(driver)
    driver.close()
    info_json = {"team_name": "dtjo223",
                 "Team_members_sids":"10697704",
                 "app_status_code": status}
    
    return json.dumps(info_json)


@app.route('/api/reset')
def reset():
    driver = get_driver()
    reset_patients(driver)
    status = check_online(driver)
    driver.close()
    info_json = {"reset_status_code":status}
    return json.dumps(info_json)


# Real Time Reporting
@app.route('/api/zipalertlist')
def get_ziplist():
    driver = get_driver()
    ziplist = get_alert_zips_list(driver)
    info_json = {"ziplist": ziplist}
    return json.dumps(info_json)


@app.route('/api/alertlist')
def get_alertlist():
    driver = get_driver()
    count = get_alert_state(driver)
    state_status = 1 if count >= 15 else 0
    info_json = {"state_status": state_status}
    return json.dumps(info_json)


@app.route('/api/testcount')
def get_testcount():
    driver = get_driver()
    pos_count, neg_count = get_statewide_testing_counts(driver)
    driver.close()
    info_json = {"positive_test":str(pos_count),
                 "negative_test":str(neg_count)}
    
    return json.dumps(info_json)


# Logical and Operational Functions
@app.route('/api/of1', methods=['POST'])
def of1():
    s = json.loads(request.get_json())
    driver = get_driver()
    for patient in s:
        patient_data = pd.Series(patient)
        # print(patient_data)
        update_and_route_patient(driver, patient_data)

    # update zips with status alert
    update_alert_zips(driver)
    driver.close()
    return "1"


@app.route('/api/getpatient/<mrn>')
def get_patient(mrn):
    driver = get_driver()
    patient_mrn, location_code = fetch_patient(driver, mrn)
    driver.close()
    info_json = {"mrn":str(patient_mrn),
                 "location_code": str(location_code)}
    
    return json.dumps(info_json)


@app.route('/api/gethospital/<id>')
def get_hospital_patient_numbers(id):
    driver = get_driver()
    total, free, zipcode = fetch_hospital_info(driver, id)
    driver.close()
    info_json = {"total_beds":str(total),
                 "available_beds":str(free),
                 "zipcode":str(zipcode)}
    return json.dumps(info_json)
