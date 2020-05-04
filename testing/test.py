import requests
import json
import ast
import code
import time


test_strings = [
     "{'first_name': 'Rocky', 'last_name': 'Conyers', 'mrn': '03a90f08-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}",
     "[{'first_name': 'Thomas', 'last_name': 'Lapp', 'mrn': '03a9d53c-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '1'}, {'first_name': 'Denise', 'last_name': 'Holiday', 'mrn': '03aa3b3a-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Teresa', 'last_name': 'Sanchez', 'mrn': '03aa4256-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}, {'first_name': 'Henrietta', 'last_name': 'Lewis', 'mrn': '03aa5c82-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Jennifer', 'last_name': 'Mooney', 'mrn': '03aa730c-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Patricia', 'last_name': 'Cherry', 'mrn': '03aa85ea-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}]",
    "[{'first_name': 'Gertrude', 'last_name': 'Jacoby', 'mrn': '03aad388-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Calvin', 'last_name': 'Flynn', 'mrn': '03aae62a-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Bill', 'last_name': 'Jones', 'mrn': '03aaed00-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Aaron', 'last_name': 'Furbee', 'mrn': '03ad72aa-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Robert', 'last_name': 'Martin', 'mrn': '03ad798a-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '5'}, {'first_name': 'Shannon', 'last_name': 'Puckett', 'mrn': '03ad9532-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}, {'first_name': 'Joel', 'last_name': 'Watson', 'mrn': '03ad9d0c-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}, {'first_name': 'Jennifer', 'last_name': 'Collini', 'mrn': '03b1e394-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}, {'first_name': 'Kelly', 'last_name': 'Flagge', 'mrn': '03b6d99e-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}, {'first_name': 'Courtney', 'last_name': 'Lawson', 'mrn': '03b6eeac-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}, {'first_name': 'Terrence', 'last_name': 'Nitkowski', 'mrn': '03bc40fa-8d5e-11ea-8bb5-ac87a3187c5f', 'zip_code': '40337', 'patient_status_code': '6'}]"
]

waits = [15, 15, 10]

URL = "http://localhost:5000/api/of1"
for idx, s in enumerate(test_strings):
    my_string = s
    my_string = my_string.replace("'", "\"")
    print(my_string)
    json_body = json.loads(my_string)
    print(type(json_body))
    if isinstance(json_body, dict):
        print('changed')
        json_body = [json_body]
    json_body = json.dumps(json_body)
    print(f"waiting {waits[idx]} seconds")

    payload = requests.post(URL, json=json_body)
    time.sleep(waits[idx])
