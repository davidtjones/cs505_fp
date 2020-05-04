import json
import requests
import time
from database.functional import *
from webserver import get_driver

URL = "http://localhost:5000/api/of1"
while True:
    driver = get_driver()
    update_alert_zips(driver)
    driver.close()
    time.sleep(1)

    
