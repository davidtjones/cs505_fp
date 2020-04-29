import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm, trange
from config import neo4j_cred as cred
import code
from database.graph_query import *

def fetch_patient(driver, mrn):
    data = pd.Series({"mrn": mrn})
    with driver.session() as session:
        records = session.read_transaction(get_patient_records, data)
        results = [i for i in records]
        if results:
            patient_mrn = results[0]['p1.mrn']
            psc = results[0]['p1.patient_status_code']
            if psc in ['3', '5', '6']:
                records = session.read_transaction(get_patient_hospital_id, data)
                results = [i for i in records]
                location_code = results[0]['h.id']
            else:
                location_code = '0'

        return patient_mrn, location_code

def fetch_hospital_info(driver, hid):
    data = pd.Series({"id":hid})
    with driver.session() as session:
        records = session.read_transaction(get_hospital_patient_numbers, data)
        results = [i for i in records]
        total_beds = results[0]['h.beds']
        free_beds = results[0]['h.free_beds']
        zipcode = results[0]['h.zipcode']
        return total_beds, free_beds, zipcode

def reset_patients(driver):
    # this is a soft reset for use with the API
    with driver.session() as session:
        session.write_transaction(detach_delete_patients)
        session.write_transaction(set_hospital_beds_max)
        session.write_transaction(unset_zip_t1t2)

def reset_graph(driver):
    # this is a hard reset and will require the graph to be rebuilt
    with driver.session() as session:
        session.write_transaction(detach_delete_all)

def get_statewide_testing_counts(driver):
    with driver.session() as session:
        pos = session.read_transaction(get_statewide_positive_test_count)
        neg = session.read_transaction(get_statewide_negative_test_count)

        pos_results = [i for i in pos]
        neg_results = [i for i in neg]

        pos_count = pos_results[0]['count(p1)']
        neg_count = neg_results[0]['count(p1)']

        return str(pos_count), str(neg_count)

def update_alert_zips(driver):
    with driver.session() as session:
        session.write_transaction(update_patient_t1t2_rel)
        session.write_transaction(update_patient_t2_rel)
        session.write_transaction(set_alert_state)

def get_alert_zips_list(driver):
    with driver.session() as session:
        records = session.read_transaction(get_alert_zips)
        results = [i for i in records]
        ziplist = []
        for result in results:
            ziplist.append(result['z.zipcode'])

        return ziplist
          
                                       
def get_alert_state(driver):
    with driver.session() as session:
        records = session.read_transaction(get_alert_count)
        results = [i for i in records]
        count = results[0]['count(z)']
        return count

def check_online(driver):
    with driver.session() as session:
        records = session.read_transaction(get_online)
        results = [i for i in records]
        status = "1" if len(results) > 0 else "0"
        return status
        
    
def update_and_route_patient(driver, data):
    with driver.session() as session:
        # check if patient is in database
        records = session.read_transaction(get_patient, data)
        results = [i for i in records]
        if len(results) == 0:
            # patient doesn't exist, add new patient!
            session.write_transaction(add_patient, data)
            session.write_transaction(add_patient_zip_rel, data)

        else:
            # find patient in database and update status
            session.write_transaction(update_patient_status_code, data)

        # if psc is 0, 1, 2, 4, send home/stay at home
        if data.patient_status_code in ['0', '1', '2', '4']:
            session.write_transaction(delete_patient_hospital_rel, data)

        # if psc is 3, 5, route to any hospital with open beds
        if data.patient_status_code in ['3', '5']:
            session.write_transaction(add_patient_hospital_rel, data)

        # if psc is 6 route to a Level IV Trauma hospital with open beds
        if data.patient_status_code == '6':
            session.write_transaction(add_crit_patient_hospital_rel, data)

        # if psc is 2, 5, or 6, mark patient with t1_sick for rt reporting
        if data.patient_status_code in ['2', '5', '6']:
            session.write_transaction(add_patient_t1_rel, data)
        
def test_all_connected(driver, distance_df):
    # Test that there are no unconnected subgraphs in the database
    # - We can do this by checking that every zipcode-node has at
    #   least one path to a single hospital
    # - Since each hospital is connected to a zipcode, if all
    #   nodes connect to one hospital, then all nodes should
    #   connect to all hospitals
    # - This is useful to confirm a graph is safe to use
    
    print("Checking that the entire graph is connected...")
    zipcodes = distance_df["zip_from"].unique()
    with driver.session() as session:
        # remove unconnected labels from connected nodes
        for i in trange(len(zipcodes)):
            zipcode = zipcodes[i]
            records = session.read_transaction(find_path_to_UK, str(zipcode))
            nodes = None
            for record in records:
                nodes = record['p'].nodes
                if not nodes:
                    exit(f"ERROR! Found empty path... check zipcode {zipcode}")
                
                    
def init_graph_database(driver, distance_df, hospital_df):
        # Create the database
        # - create nodes for zipcodes
        # - add relationships (is_neighbor) between zipcodes
        # - add hospital nodes
        # - add relationships from zipcode (has_hospital) to hospitals
        finished = []
        distances = [1, 2, 4, 8, 16]
        with driver.session() as session:
            print("Assigning zipcodes as nodes and creating relationships...")
            for d in distances:
                print(f"connecting regions with ~{d} mile distance")
                
                for i in range(len(distance_df)):
                    row = distance_df.iloc[i]
                    zip1 = str(row['zip_from'])[:-2]
                    zip2 = str(row['zip_to'])[:-2]
                    distance = row['distance']
                    if (zip1, zip2) in finished or (zip2, zip1) in finished:
                        # already processed this zipcode
                        continue
                    if distance > d:
                        # really defeats the purpose of a graph database otherwise
                        continue
                    else:
                        # since the sum of the previous distances is < the current
                        # distance, we only want to add a path if it's necessary!
                        # if some path exists already, it will be shorter than making
                        # a new path at the current threshold
                    
                        # Search for path:
                        records = session.read_transaction(find_path_zip2zip, zip1, zip2)
                        path = None
                        for record in records:
                            path = record['p'].nodes
                        if not path:
                            session.write_transaction(add_zip, zip1)
                            session.write_transaction(add_zip, zip2)
                            session.write_transaction(add_zip_zip_rel, zip1, zip2, distance)
                            print(f"adding zips {zip1}, {zip2}")
                    finished.append((zip1, zip2))
                                   
            print("Assigning hostpitals as nodes and creating relationships...")
            for idx, row in hospital_df.iterrows():
                data = row.apply(str)
                session.write_transaction(add_hospital, data)
                session.write_transaction(add_zip_hospital_rel, data)
        
                                
                                
                                
    

    
    

    
