import pandas as pd
from neo4j import GraphDatabase
from database.functional import reset_graph, init_graph_database, test_all_connected
from config import neo4j_cred as cred

# Run this file to build the initial graph
print("Graph Initialization")
start = input("WARNING: this script will completely erase all data and rebuild the graph, are you sure you want to continue? Enter \"Y\" to continue")

if start.capitalize() == "Y":
    distance_df = pd.read_csv("database/data/kyzipdistance.csv")
    hospital_df = pd.read_csv("database/data/hospitals.csv")
    code.interact(local=locals())
    driver = GraphDatabase.driver(
        cred['uri'],
        auth=(
            cred['username'],
            cred['password']),
        encrypted=False
    )
    print("Building Graph Database...")
    
    # Reset graph database
    reset_graph(driver)
    
    # Initialize the database
    init_graph_database(driver, distance_df, hospital_df)
    
    # check for still-unconnected nodes
    test_all_connected(driver, distance_df)
    
    driver.close()
    print(f"Graph successfully built in {time.time() - start_time} seconds")

print("Finished")
