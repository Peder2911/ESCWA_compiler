
import requests
import sqlite3
import pandas as pd
import os
import logging

BASEURL ="https://unstats.un.org/sdgs/metadata/files/Metadata-{}.pdf"

# ========================================================

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

db = sqlite3.connect("data/db.sqlite")
data = pd.read_sql("SELECT Goal,Target,Indicator FROM META_indicators",db)
db.close()

gatherInames = lambda r: "{}-{}-{}".format(
    *[r[var].zfill(2) for var in ["Goal","Target","Indicator"]])
indicators = data.apply(gatherInames,1)

for i in indicators:
    tgt = f"docs/{i}.pdf"

    if os.path.exists(tgt):
        logger.info(f"Already have {i}")
        continue 

    with open(tgt,"wb") as f:
        r = requests.get(BASEURL.format(i))
        if r.status_code == 200:
            logger.info(f"Got {i}")
            f.write(r.content)
        else:
            logger.error(f"Couldn't fetch {i}")

logger.info("done!")
