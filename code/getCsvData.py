
import requests
import os
from os import path
import logging
import hashlib
import json
import time
from datetime import datetime
from util import memget 
import yaml
import csv
import functools
import operator
import pandas as pd

logging.basicConfig(level = logging.INFO)
API_BASE = "https://unstats.un.org/SDGAPI/v1/sdg"
logger = logging.getLogger(__name__)

with open("data/manual/index_targets.yaml") as f:
    indexTargets = yaml.safe_load(f)

escwacountries = pd.read_csv("data/manual/escwa_countries.csv")
escwacodes = escwacountries.iso3n

# ========================================================
# Get indicator series related to the indices

indicators = json.loads(memget("https://unstats.un.org/SDGAPI/v1/sdg/Series/List?allreleases=true"))
with open("data/whole/indicators.json","w") as f:
    json.dump(indicators,f)
    
def createIndicatorEntry(indicator):
    """
    Just a slimmer representation, with the target and indicator gcode
    """

    return {"tgt":indicator["target"],"code":indicator["code"]}

indexIndicators = {}
for index in indexTargets:
    thisIndexIndicators = []
    for indicator in indicators:
        if any([target in indexTargets[index] for target in indicator["target"]]):
            entry = createIndicatorEntry(indicator)
            thisIndexIndicators.append(entry)

    indexIndicators[index] = thisIndexIndicators

with open("data/subset/index_indicators.json","w") as f:
    json.dump(indexIndicators,f)

allIndicators = functools.reduce(operator.add, indexIndicators.values())
allIndicators = [c["code"] for c in allIndicators]

# ========================================================
# Get all indicators for all countries 

s = requests.Session()
s.headers.update({
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/octet-stream"
})

def getIndicatorData(indicator):
    r = s.post("https://unstats.un.org/SDGAPI/v1/sdg/Series/DataCSV",
        data = {"seriesCodes":indicator, "areaCodes": escwacodes})
    if r.status_code == 200:
        return r.content
    else:
        logger.error(f"request for {indicator} returned {r.status_code}")

def getAndWriteIndicator(indicator):
    tgt = f"data/indicators/{indicator}.csv"
    if not os.path.exists(tgt):
        with open(tgt,"wb") as f:
            logger.info(f"Getting data for {tgt}")
            data = getIndicatorData(indicator)
            data = data.rstrip(b"\x00")
            f.write(data)
    else:
        logger.info(f"{tgt} already exists")
        pass

for i in allIndicators:
    getAndWriteIndicator(i)
