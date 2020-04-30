import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm, trange


# Define prepared statements
def test():
    print("Testing 1, 2, 3")
    
def detach_delete_all(tx):
    # this particular function deletes the entire graph, use with care!
    tx.run("MATCH(n) DETACH DELETE n")

    
def detach_delete_patients(tx):
    # this removes all patient data from the graph - useful for the API :D
    tx.run("MATCH(n:Patient) DETACH DELETE n")

def delete_patient_hospital_rel(tx, data):
    # this removes any RECVS_CARE_AT relationship from a patient
    tx.run("MATCH(p:Patient{mrn:$mrn})-[r:RECVS_CARE_AT]->(h:Hospital) "
           "SET h.free_beds = h.free_beds + 1 "
           "DELETE r",
           mrn=data.mrn)  
    
def unset_zip_t1t2(tx):
    tx.run("MATCH(z:Zip) "
           "SET z.t1 = 0, z.t2 = 0, z.alert_status = 0 "
           "RETURN count(z)")
    
    
def add_zip(tx, zip1):
    # check if zip1 and zip2 are in database
    tx.run("MERGE(z1:Zip {zipcode:$myzip1, " 
           "alert_status:0, t1:0, t2:0}) "
           "RETURN z1",
           myzip1=zip1)

    
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
           id=data.ID, name=data.NAME, city=data.CITY,
           state=data.STATE, zipcode=data.ZIP, type=data.TYPE,
           beds=int(data.BEDS), zero=0, countyfips=data.COUNTYFIPS,
           county=data.COUNTY, latitude=data.LATITUDE,
           longitude=data.LONGITUDE, naics_code=data.NAICS_CODE,
           website=data.WEBSITE, owner=data.OWNER, trauma=data.TRAUMA,
           helipad=data.HELIPAD)

def get_hospital_patient_numbers(tx, data):
    records = tx.run("MATCH(h:Hospital) "
                     "WHERE h.id = $id "
                     "RETURN h.beds, h.free_beds, h.zipcode",
                     id=data.id)
    return records

def get_patient_hospital_id(tx, data):
    records = tx.run("MATCH(p:Patient{mrn:$mrn})-[r:RECVS_CARE_AT]->(h:Hospital) "
                     "RETURN h.id",
                     mrn=mrn)
    return records
    
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

def get_patient_records(tx,data):
    records = tx.run("MATCH(p1:Patient{mrn:$mrn}) "
                     "RETURN p1.mrn, p1.patient_status_code",
                     mrn=data.mrn)
    return records


def get_patient_status_code(tx, data):
    records = tx.run("MATCH(p:Patient) "
                     "WHERE p.mrn = $mrn "
                     "RETURN p.patient_status_code",
                     mrn=data.mrn)
    return records

def get_patient_hospital_id(tx, data):
    records = tx.run("MATCH(p:Patient{mrn:$mrn})-[:RECVS_CARE_AT]->(h:Hospital) "
                     "RETURN h.id",
                     mrn=data.mrn)
    return records
    
def add_patient(tx, data):
    tx.run("CREATE(p1:Patient{"
           "first_name:$fname,"
           "last_name:$lname,"
           "mrn:$mrn,"
           "zip_code:$zipcode,"
           "patient_status_code:$patient_status_code,"
           "check_in_time:timestamp()}) "
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


def add_patient_hospital_rel(tx, data):
    # when patient status code indicates that they need to report to a hospital
    tx.run("MATCH path=(p1:Patient{mrn:$mrn})-[:LIVES_IN]-(:Zip)-[r:IS_NEIGHBOR*1..150]-(:Zip)-[:HAS_HOSPITAL]->(h1:Hospital) "
           "WHERE h1.free_beds <= h1.beds AND h1.free_beds > 0 "
           "WITH p1, h1 ORDER BY length(path) LIMIT 1 "
           "MERGE(p1)-[c:RECVS_CARE_AT]->(h1) "
           "SET h1.free_beds = h1.free_beds-1 "
           "RETURN h1",
           mrn=data.mrn
           )


def add_crit_patient_hospital_rel(tx, data):
    tx.run(
        "MATCH path=(p1:Patient{mrn:$mrn})-"
        "[:LIVES_IN]-(:Zip)-[r:IS_NEIGHBOR*1..150]-"
        "(:Zip)-[:HAS_HOSPITAL]->(h1:Hospital) "
        "WHERE h1.free_beds <= h1.beds AND "
        "h1.free_beds > 0 AND h1.trauma CONTAINS \"LEVEL IV\" "
        "WITH p1, h1 ORDER BY length(path) LIMIT 1 "
        "MERGE(p1)-[c:RECVS_CARE_AT]->(h1) "
        "SET h1.free_beds = h1.free_beds-1 "
        "RETURN c",
        mrn=data.mrn
    )

    
def set_hospital_beds_max(tx):
    tx.run("MATCH(h1:Hospital) "
           "SET h1.free_beds = h1.beds "
           "RETURN h1")


def get_statewide_positive_test_count(tx):
    records = tx.run(
        "MATCH(p1:Patient) "
        "WHERE p1.patient_status_code = \"2\" or p1.patient_status_code = \"5\" or p1.patient_status_code = \"6\" "
        "RETURN count(p1)")

    return records


def get_statewide_negative_test_count(tx):
    records = tx.run(
        "MATCH(p1:Patient) "
        "WHERE p1.patient_status_code = \"1\" or p1.patient_status_code = \"4\" "
        "RETURN count(p1)")

    return records


def add_patient_t1_rel(tx, data):
    tx.run(
        "MATCH(p:Patient{mrn:$mrn})-[:LIVES_IN]->(z:Zip) "
        "MERGE(p)-[:T1_SICK]->(z) "
        "SET z.t1 = z.t1+1 "
        "RETURN z ",
        mrn = data.mrn)


def update_patient_t1t2_rel(tx):
    tx.run(
        "MATCH(p:Patient)-[r:T1_SICK]->(z:Zip) "
        "WHERE p.check_in_time < timestamp() - 15000 "
        "DELETE r "
        "MERGE (p)-[r2:T2_SICK]->(z) "
        "SET z.t2 = z.t2+1, z.t1 = z.t1-1 "
        "RETURN z")


def update_patient_t2_rel(tx):
    tx.run(
        "MATCH(p:Patient)-[r:T2_SICK]->(z:Zip) "
        "WHERE p.check_in_time < timestamp() - 30000 "
        "DELETE r "
        "SET z.t2 = z.t2 -1 "
        "RETURN z")
           
           

def set_alert_state(tx):
    tx.run("MATCH(z:Zip) "
           "WHERE ((2*z.t1) <= z.t2) AND  (z.t1 <> 0) "
           "SET z.alert_status = 1 " 
           "RETURN z")

def unset_alert_state(tx):
    tx.run("MATCH(z:Zip) "
           "WHERE((2*z.t1) > z.t2) "
           "SET z.status_alert = 0")


def get_alert_zips(tx):
    records = tx.run("MATCH(z:Zip) "
                     "WHERE z.alert_status = 1 "
                     "RETURN z.zipcode")
    return records
    
    
def get_alert_count(tx):
    records = tx.run("MATCH(z:Zip) "
                     "WHERE z.alert_status = 1 "
                     "RETURN count(z)")
    return records

def get_online(tx):
    records = tx.run(
            "MATCH(h1:Hospital{zipcode:\"40536\"}) "
            "RETURN h1")
    return records

