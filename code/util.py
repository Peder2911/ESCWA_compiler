
import re
import logging
import hashlib
import os
from os import path
from datetime import datetime
import requests
import time

logger = logging.getLogger(__name__)

def memoizeRequest(fun):
    cacheLocation = "cache/requests"
    def inner(*args, **kwargs):
        url = args[0]
        start = datetime.now()
        urlHash = hashlib.md5(url.encode()).hexdigest()
        cachedRequests = os.listdir(cacheLocation)

        if urlHash not in cachedRequests:
            logger.info(f"Caching {url[:15]}... ({urlHash[:5]}...)")
            res = fun(*args, **kwargs)
            with open(path.join(cacheLocation,urlHash),"w") as f:
                f.write(res)
            time.sleep(0.5)
        else:
            logger.info(f"Using cache for {url[:15]}... ({urlHash[:5]}...)")
            with open(path.join(cacheLocation,urlHash)) as f:
                res = f.read()


        duration = datetime.now() - start
        logger.info(f"Duration: {duration}")
        return res
    return inner

@memoizeRequest
def memget(url):
    r = requests.get(url)
    if r.status_code != 200:
        logger.critical(f"Returned {r.status_code}")
    return r.text

def isCodeTable(t):
    header = t.find("tr").text
    hasCode = re.search("[Cc]ode", header) is not None
    hasCname = re.search("Country name", header) is not None
    return hasCode & hasCname
