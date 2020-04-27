import pandas as pd
import code
from neo4j import GraphDatabase
from tqdm import tqdm, trange
from config import neo4j_cred as cred

# Define prepared statements

def detach_delete_all(tx):
    # this particular function deletes the entire graph, use with care!
    tx.run("MATCH(n) DETACH DELETE n")

    
def detach_delete_patients(tx):
    # this removes all patient data from the graph - useful for the API :D
    tx.run("MATCH(n:Patient) DETACH DELETE n")

def delete_patient_hospital_rel(tx, data):
    # this removes any RECVS_CARE_AT relationship from a patient
    tx.run("MATCH(p:Patient{mrn:$mrn})-[r:RECVS_CARE_AT]->() "
           "DELETE r")  
    
    
def add_zip(tx, zip1):
    # check if zip1 and zip2 are in database
    tx.run("MERGE(z1:Zip {zipcode:$myzip1, alert_status:$zero}) "
           "RETURN z1",
           myzip1=zip1,zero=0)

    
def add_zip_zip_rel(tx, zip1, zip2, distance):
    tx.run("MATCH(z1:Zip {zipcode:$myzip1}),(z2:Zip{zipcode:$myzip2}) "
           "MERGE(z1)-[r:IS_NEIGHBOR{distance:$mydistance}]-(z2) "
           "RETURN r",
           myzip1=zip1, myzip2=zip2, mydistance=distance)      

    
def add_hospital(tx, data):
    # do we also need to include "beds taken?"
    tx.run("MERGE(h1:Hospital{"
           "id:$id,"
           "name:$name,"
           "city:$city,"
           "state:$state,"
           "zipcode:$zipcode,"
           "type:$type,"
           "beds:$beds,"
           "free_beds:$beds,"
           "county:$county,"
           "countyfips:$countyfips,"
           "latitude:$latitude,"
           "longitude:$longitude,"
           "naics_code:$naics_code,"
           "website:$website,"
           "owner:$owner,"
           "trauma:$trauma,"
           "helipad:$helipad}) "
           "RETURN h1",
           id=data.ID, name=data.NAME, city=data.CITY, state=data.STATE, zipcode=data.ZIP,
           type=data.TYPE, beds=int(data.BEDS), zero=0, countyfips=data.COUNTYFIPS,
           county=data.COUNTY, latitude=data.LATITUDE, longitude=data.LONGITUDE,
           naics_code=data.NAICS_CODE, website=data.WEBSITE, owner=data.OWNER,
           trauma=data.TRAUMA, helipad=data.HELIPAD)
    
def add_zip_hospital_rel(tx, data):
    tx.run("MATCH(z1:Zip{zipcode:$myzip1}),(h1:Hospital{id:$myhid1}) "
           "MERGE(z1)-[r:HAS_HOSPITAL]->(h1) "
           "RETURN r",
           myzip1=data.ZIP, myhid1=data.ID)

def get_patient(tx, data):
    records = tx.run("MATCH(p1:Patient{"
           "mrn:$mrn}) "
           "RETURN p1",
           mrn=data.mrn)
    return records
                
    
def add_patient(tx, data):
    tx.run("CREATE(p1:Patient{"
           "first_name:$fname,"
           "last_name:$lname,"
           "mrn:$mrn,"
           "zip_code:$zipcode,"
           "patient_status_code:$patient_status_code}) "
           "RETURN p1",
           fname=data.first_name,
           lname=data.last_name,
           mrn=data.mrn,
           zipcode=data.zip_code,
           patient_status_code=data.patient_status_code
           )

def update_patient_status_code(tx, data):
    tx.run("MATCH(p:Patient{mrn:$mrn}) "
           "SET p.patient_status_code = $psc "
           "RETURN p",
           mrn=data.mrn, psc=data.patient_status_code)

def add_patient_zip_rel(tx, data):
    tx.run("MATCH(p1:Patient{mrn:$mrn}),(z1:Zip{zipcode:$myzip1}) "
           "MERGE(p1)-[r:LIVES_IN]->(z1) "
           "RETURN r",
           mrn=data.mrn,
           myzip1=data.zip_code
           )

def add_patient_hospital_rel(tx, data):
    tx.run("MATCH(p1:Patient{mrn:$mrn}),(h1:Hospital{id:$myhid1}) "
           "MERGE(p1)-[r:RECVS_CARE_AT]->(h1) "
           "RETURN r",
           mrn
           )
               
# Functions to test and build graph
def find_path_to_UK(tx, zip1):
    # find path, if possible, and remove unconnected label
    records = tx.run("MATCH(z1:Zip{zipcode:$myzip1}),"
           "(h1:Hospital{id:\"11640536\"}), "
           "p=shortestPath((z1)-[*]-(h1)) "
           "RETURN p",
           myzip1=zip1)

    return records

def find_path_zip2zip(tx, zip1, zip2):
    records = tx.run("MATCH(z1:Zip{zipcode:$myzip1}),"
                     "(z2:Zip{zipcode:$myzip2}), "
                     "p=shortestPath((z1)-[*]-(z2)) "
                     "RETURN p",
                     myzip1=zip1,
                     myzip2=zip2)

    return records

    

def find_patient_closest_hospital_id(tx, data):
    # need to also confirm that hospital isn't filled!
    records = tx.run("MATCH(p1:Patient{mrn:$mrn}),"
                     "(h1:Hospital),p=shortestPath((p1)-[*]-(h1)) "
                     "RETURN h1.id "
                     "ORDER BY length(p) "
                     "LIMIT 1",
                     mrn=data.mrn)
    return records

def reset_graph(driver):
    with driver.session() as session:
        session.write_transaction(detach_delete_all)

def reset_patients(driver):
    with driver.session() as session:
        session.write_transaction(detach_delete_patients)
        
def test_all_connected(driver, distance_df):
    # Test that there are no unconnected subgraphs in the database
    # - We can do this by checking that every zipcode-node has at least one path to a
    #   single hospital
    # - Since each hospital is connected to a zipcode, if all nodes connect to one hospital,
    #   then all nodes should connect to all hospitals
    # - This is useful to confirm a graph is safe to use but also to find a rough minimum
    #   distance threshold.
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
            # else:
            #     nodes = [i['zipcode'] for i in nodes]
            #     print(f"Found path: {nodes}")

            
def init_graph_database(driver, distance_df, hospital_df):
    # Create the database
    # - create nodes for zipcodes
    # - add relationships (distance) between zipcodes
    # - add hospital nodes
    # - add relationships from zipcode (has_hospital) to hospitals
    finished = []
    distances = [1, 2, 4, 8, 16, 32]
    with driver.session() as session:
        print("Assigning zipcodes as nodes and creating relationships...")
        for d in distances:
            print(f"connecting regions with ~{d} mile distance")
            
            for i in trange(len(distance_df)):
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

                    finished.append((zip1, zip2))


                    
        print("Assigning hostpitals as nodes and creating relationships...")
        for idx, row in hospital_df.iterrows():
            data = row.apply(str)
            session.write_transaction(add_hospital, data)
            session.write_transaction(add_zip_hospital_rel, data)
                

if __name__=='__main__':
    # Run this file to build the initial graph
    distance_df = pd.read_csv("database/data/kyzipdistance.csv")
    hospital_df = pd.read_csv("database/data/hospitals.csv")
    
    driver = GraphDatabase.driver(
        cred['uri'],
        auth=(
            cred['username'],
            cred['password'])
    )
    print("Building Graph Database...")

    # Reset graph database
    reset_graph(driver)    
    
    # Initialize the database
    init_graph_database(driver, distance_df, hospital_df)

    # print any still-unconnected nodes
    test_all_connected(driver, distance_df)
    
    driver.close()
