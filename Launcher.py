import time
import json
from PayloadGen import getpayload, init, getrandpayload
from Publisher import pub

init()

while True:
    payload = getrandpayload()
    payload = json.loads(payload)
    print(len(payload))
    pub(payload)
    time.sleep(2)
