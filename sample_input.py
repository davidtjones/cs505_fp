import code
import config
import pandas as pd
from database.graph_query import *

x = {'patient.info':[{"first_name": "Charles", "last_name": "Jackson", "mrn": "1dc49720-78ca-443f-b1e0-266b85015e44", "zip_code": "40077", "patient_status_code": "4"}, {"first_name": "Eunice", "last_name": "Wright", "mrn": "bc400310-90d1-4242-8e93-df385a7f25be", "zip_code": "42103", "patient_status_code": "1"}, {"first_name": "Richard", "last_name": "Villiard", "mrn": "17523c00-f718-476d-8eeb-8b09d4c2062a", "zip_code": "42533", "patient_status_code": "5"}]}

driver = GraphDatabase.driver(
        config.neo4j_cred['uri'],
        auth=(
            config.neo4j_cred['username'],
            config.neo4j_cred['password'])
    )

# reset patients when running this test script!
reset_patients(driver)


for patient in x['patient.info']:
    data = pd.Series(patient)
    print(data)

    with driver.session() as session:
        # check if patient is in database
        records = session.read_transaction(get_patient, data)
        results = [i for i in records]
        if len(results) == 0:
            # patient doesn't exist, add new patient!
            print(f"Making new record for {data.first_name}")
            session.write_transaction(add_patient, data)
            session.write_transaction(add_patient_zip_rel, data)
        
        else:
            # find patient in database and update status
            session.write_transaction(update_patient_status_code, data)

        # if psc is 0, 1, 2, 4, send home/stay at home
        # check if there is a relationship to a hospital (remove)
        # else nothing
        if data.patient_status_code in [0, 1, 2, 4]:
            session.write_transaction(delete_patient_hospital_rel, data)

        # for cases 3, 5, 6, need routes to all hospitals
        
        # if psc is 3, 5, route to any suitable hospital
        # find path to nearest hospital with curr_beds < beds_max
        

        # if psc is 6 route to a Level IV hospital
        # - find path to nearest hospital with curr_beds < beds_max
        # - and level = 4

        # if psc is 1, 4 - negative test
        # - increment test counter for negative test cases

        # if psc is 2, 5, 6 - positive test
        # - increment test counter for positive test cases
        
        code.interact(local=locals())
        #session.write_transaction(add_patient, data)
        #session.write_transaction(add_patient_zip_rel, data)

driver.close()
                

    
    






