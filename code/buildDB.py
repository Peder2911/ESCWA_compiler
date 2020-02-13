
import sqlite3
import csv
import os
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import logging
import json

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

DBPATH = "data/db.sqlite"

if os.path.exists(DBPATH):
    logger.info("Deleting previous version of database!")
    os.remove(DBPATH)

path_to_db = os.path.join(os.path.abspath("."),"data/db.sqlite")
engine = create_engine(f"sqlite:///{DBPATH}", echo = False)

# ========================================================
# Add indicator data
for datafile in os.listdir("data/indicators"):
    path = os.path.join("data/indicators",datafile)
    indicator,_ = os.path.splitext(datafile)
    
    try:
        d = pd.read_csv(path)

    except pd.errors.EmptyDataError:
        logging.debug(f"{path} is empty")
        continue

    if d.shape[0] == 0:
        logging.debug(f"{path} had no rows")
        continue

    d.columns = [c.strip() for c in d.columns]

    specificColumns = [c for c in d.columns if re.search("\[[^\]]+\]",c)]
    valuecolumns = {
        "SeriesCode","Value","GeoAreaCode","TimePeriod","Source",*specificColumns
    }
    metacolumns = {
        "SeriesCode","SeriesDescription","Goal","Target","Indicator"
    }

    # ====================================
    # Insert values as table
    columns = set.intersection(valuecolumns, set(d.columns))
    d[[*columns]].to_sql(
        indicator,engine,if_exists="replace",index=False)

    # ====================================
    # Append indicator metadata to metadata table
    splt = np.vectorize(lambda x,y: x.split(".")[y])
    for name,value in {"Goal":0,"Target":1,"Indicator":2}.items():
        d[name] = splt(d["Indicator"],value)

    d[0:1][[*metacolumns]].to_sql(
        "META_indicators", engine, if_exists="append",index=False)

# ========================================================
# Addictional metadata
# Add country info
#countries = pd.read_csv("data/subset/mena_countries.csv")
#menacodes = pd.read_csv("data/manual/escwa_codes.csv")
#countries = countries.merge(menacodes,how="right")
countries = pd.read_csv("data/manual/escwa_countries.csv")

countries.to_sql(
    "META_menacountries", engine, if_exists="replace",index=False)

# Add indices info 
with open("data/subset/index_indicators.json") as f:
    indexIndicators = json.load(f)

indicatorData = pd.DataFrame({"indicator":[],"index":[]})
for key,val in indexIndicators.items():
    indicatorCodes = [*set([e["code"] for e in val])]
    indexIndicatorData = pd.DataFrame({"indicator":indicatorCodes})
    indexIndicatorData["index"] = key
    indicatorData = pd.concat([indicatorData,indexIndicatorData])

indicatorData.to_sql(
    "META_indices", engine, if_exists="replace",index=False)
