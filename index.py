
import os
import requests as r
from bs4 import BeautifulSoup as bs
from dotenv import load_dotenv
from pymongo import MongoClient
import subprocess
load_dotenv()

mongo_user = os.getenv("MONGO_USER")
mongo_pass = os.getenv("MONGO_PASS")
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")
mongo_db = os.getenv("MONGO_DB")

uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?directConnection=true"

client = MongoClient(uri)
db = client[mongo_db]
collection = db["artifacts"]

result = collection.find_one({"project": "dpd", "current": True})
print(result["current"])


index_url = "https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/what-data-extract-drug-product-database.html"


respose = r.get(index_url)


dates = []
if respose.status_code == 200:
    page = bs(respose.text, "html.parser")
    table = page.select_one("table.table-bordered")
    # print(table)
    for tr in table.select("tr")[1:]:
        for td in tr.select("td"):
            target = (td.get_text(strip=True))
            if "2025" in target:
                dates.append(target)


dates = list(set(dates))
print(dates)

if len(dates) > 0:
    if dates[0] != result["last_update"]:
        print("new change detected, running main.py  ...")
        subprocess.run(["python", "main.py"])
    else:
        print("no new change detected, skipping ...")
