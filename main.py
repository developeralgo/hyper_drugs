import json
import re
import os
import subprocess
import zipfile
import uuid
import copy
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
import requests as r
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
mongo_user = os.getenv("MONGO_USER")
mongo_pass = os.getenv("MONGO_PASS")
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")
mongo_db = os.getenv("MONGO_DB")

uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?directConnection=true"

client = MongoClient(uri)
db = client[mongo_db]
old_dpd_collection = db["dpd"]


def get_product_page(drug_code):
    url = f"https://health-products.canada.ca/dpd-bdpp/info?lang=eng&code={drug_code}"
    response = r.get(url)
    inter = {}
    if response.status_code == 200:
        page = bs(response.text, "lxml")
        rows = page.select("div.row")
        for row in rows:
            text = row.get_text(strip=True).replace("See footnote", "")
            splitted = text.split(":")
            if len(splitted) > 1:
                inter[splitted[0].strip().replace(" ", "_").lower()
                      ] = splitted[1].strip()
            if "Product Monograph" in text:
                target = row.select_one("a")
                if target is not None:
                    inter["product_monograph"] = target["href"]
                date = row.select_one("p.col-sm-8")
                monograph_date = date.span.get_text(strip=True)
                inter["monograph_date"] = monograph_date
            if "Original market date" in text:
                original_market_date = row.select_one(
                    "p.col-sm-8").get_text(strip=True)
                inter["original_market_date"] = original_market_date

        final = {}
        try:
            final["current_status"] = inter["current_status"]
            final["product_monograph"] = inter["product_monograph"]
            final["monograph_date"] = inter["monograph_date"]
            final["original_market_date"] = inter["original_market_date"]
            final["monograph_date_parsable"] = datetime.strptime(
                inter["monograph_date"], "%Y-%m-%d")
        except:
            final["current_status"] = ""
            final["product_monograph"] = ""
            final["monograph_date"] = ""
            final["original_market_date"] = ""
            final["monograph_date_parsable"] = ""
        print("success fully fetched monographs for", drug_code)
        return final
    else:
        print(
            f"Failed to get product page. Status code: {response.status_code}")
        return None


# Define the URL you want to fetch
url = "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles.zip"

# Use subprocess to call wget
files = os.listdir()

if "allfiles.zip" not in files:
    try:
        # The '-O' option allows you to specify the output file name
        output_file = "allfiles.zip"
        subprocess.run(["wget", url, "-O", output_file], check=True)
        print(f"Resource downloaded successfully and saved as {output_file}.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while trying to download the resource: {e}")


data_dir = Path.cwd() / "data"
data_dir.mkdir(exist_ok=True)  # create 'data' if it doesn't exist

# Path to your zip file
zip_path = Path("./allfiles.zip")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(data_dir)

print(f"Extracted contents of {zip_path.name} into {data_dir}")


with open("./data/drug.txt", "r") as file:
    drugs = file.read().split("\n")


cleaned_drugs = []
for item in drugs:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    # print(len(cleaned))
    if len(cleaned) > 10:
        drug_dict = {
            "drug_code": cleaned[0],
            "product_categorization": cleaned[1],
            "class": cleaned[2],
            "din": cleaned[3],
            "brand_name": cleaned[4],
            "accession_number": cleaned[7],
            "number_of_ais": cleaned[8],
            "last_update_date": cleaned[9],
            "ai_group_no": cleaned[10],

        }
        cleaned_drugs.append(
            drug_dict) if drug_dict["class"] == "Human" else None


with open("./data/ingred.txt", "r") as file:
    ings = file.read().split("\n")

ingredients = []
for item in ings:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    if len(cleaned) > 10:
        ing_dict = {
            "drug_code": cleaned[0],
            "ai_code": cleaned[1],
            "ingredient": cleaned[2],
            "ingredient_supplied_ind": cleaned[3],
            "strength": cleaned[4],
            "strength_unit": cleaned[5],
            "strength_type": cleaned[6],
            "dosage_value": cleaned[7],
            "base": cleaned[8],
            "dosage_unit": cleaned[9],
            "notes": cleaned[10],
        }
        ingredients.append(ing_dict)


# Step 1: Index ingredients by drug_code
ingredient_index = defaultdict(list)
for ing in ingredients:
    ingredient_index[ing["drug_code"]].append(ing)

# Step 2: Attach ingredients to each drug
for item in cleaned_drugs:
    item["ingredients"] = ingredient_index.get(item["drug_code"], [])
    for element in item["ingredients"]:
        del (element["drug_code"])


print(len(cleaned_drugs))


with open("./data/comp.txt", "r") as file:
    comps = file.read().split("\n")
companies = []
for item in comps:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    if len(cleaned) > 14:
        comp_dict = {
            "drug_code": cleaned[0],
            "mfr_code": cleaned[1],
            "company_code": cleaned[2],
            "company_name": cleaned[3],
            "company_type": cleaned[4],
            "address_mailing_flag": cleaned[5],
            "address_billing_flag": cleaned[6],
            "address_notification_flag": cleaned[7],
            "address_other": cleaned[8],
            "suite_number": cleaned[9],
            "street_name": cleaned[10],
            "city_name": cleaned[11],
            "province": cleaned[12],
            "country": cleaned[13],
            "postal_code": cleaned[14],
            "post_office_box": cleaned[15],
        }
        companies.append(comp_dict)

companies_index = defaultdict(list)
for item in companies:
    companies_index[item["drug_code"]].append(item)

for item in cleaned_drugs:
    item["company"] = companies_index.get(item["drug_code"], [])
    item["company"] = item["company"][0]
    item["company_code"] = item["company"]["company_code"]
    del (item["company"])


with open("./data/form.txt", "r") as file:
    form = file.read().split("\n")

forms = []
for item in form:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    if len(cleaned) > 1:
        form_dict = {
            "drug_code": cleaned[0],
            "form_code": cleaned[1],
            "form": cleaned[2],
        }
        forms.append(form_dict)

forms_index = defaultdict(list)
for item in forms:
    forms_index[item["drug_code"]].append(item)
for item in cleaned_drugs:
    item["forms"] = forms_index.get(item["drug_code"], [])
    new_forms = [x["form"] for x in item["forms"]]
    new_form_codes = [x["form_code"] for x in item["forms"]]
    item["forms"] = ", ".join(new_forms)
    item["form_codes"] = ", ".join(new_form_codes)


with open("./data/status.txt", "r") as file:
    status = file.read().split("\n")

conds = []
for item in status:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]

    if len(cleaned) > 3:
        cond_dict = {
            "drug_code": cleaned[0],
            "current_status_flag": cleaned[1],
            "status": cleaned[2],
            "history_date": cleaned[3]

        }
        conds.append(cond_dict)


cond_index = defaultdict(list)
for item in conds:
    cond_index[item["drug_code"]].append(item)

for item in cleaned_drugs:
    item["status"] = cond_index.get(item["drug_code"], [])
    for element in item["status"]:
        del (element["drug_code"])


with open("./data/route.txt", "r") as file:
    route = file.read().split("\n")
routes = []
for item in route:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    if len(cleaned) > 1:
        route_dict = {
            "drug_code": cleaned[0],
            "route_code": cleaned[1],
            "route": cleaned[2],
        }
        routes.append(route_dict)


routes_index = defaultdict(list)
for item in routes:
    routes_index[item["drug_code"]].append(item)

for item in cleaned_drugs:
    item["routes"] = routes_index.get(item["drug_code"], [])
    new_routes = [x["route"] for x in item["routes"]]
    new_route_codes = [x["route_code"] for x in item["routes"]]
    item["routes"] = ", ".join(new_routes)
    item["route_codes"] = ", ".join(new_route_codes)


with open("./data/schedule.txt", "r") as file:
    schedule = file.read().split("\n")
schedules = []
for item in schedule:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    if len(cleaned) > 1:
        schedule_dict = {
            "drug_code": cleaned[0],
            "schedule": cleaned[1],
        }
        schedules.append(schedule_dict)

schedule_index = defaultdict(list)
for item in schedules:
    schedule_index[item["drug_code"]].append(item)

for item in cleaned_drugs:
    item["schedule"] = schedule_index.get(item["drug_code"], [])
    item["schedule"] = ", ".join([x["schedule"] for x in item["schedule"]])


with open("./data/ther.txt", "r") as file:
    the = file.read().split("\n")
thers = []
for item in the:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    if len(cleaned) > 1:
        the_dict = {
            "drug_code": cleaned[0],
            "tc_atc_number": cleaned[1],
            "tc_atc": cleaned[2],

        }
        thers.append(the_dict)

thers_index = defaultdict(list)
for item in thers:
    thers_index[item["drug_code"]].append(item)

for item in cleaned_drugs:
    item["thers"] = thers_index.get(item["drug_code"], [])
    item["tc_atc_number"] = item["thers"][0]["tc_atc_number"] if len(
        item["thers"]) > 0 else ""
    item["tc_atc"] = item["thers"][0]["tc_atc"] if len(
        item["thers"]) > 0 else ""
    del (item["thers"])


with open("./data/pharm.txt", "r") as file:
    pharm = file.read().split("\n")
pharms = []
for item in pharm:
    target = (item.split('",'))
    cleaned = [x.replace('"', "") for x in target]
    if len(cleaned) > 1:
        pharm_dict = {
            "drug_code": cleaned[0],
            "pharmaceutical_std": cleaned[1],
        }
        pharms.append(pharm_dict)


phams_index = defaultdict(list)
for item in pharms:
    phams_index[item["drug_code"]].append(item)

for item in cleaned_drugs:
    item["pharms"] = phams_index.get(item["drug_code"], [])
    item["pharms"] = "" if len(item["pharms"]) == 0 else ", ".join(
        [x["pharmaceutical_std"] for x in item["pharms"]])


for item in cleaned_drugs:
    item["list_ingredients"] = [x["ingredient"].lower()
                                for x in item["ingredients"]]
    item["list_ingredients"] = sorted(
        list(set(item["list_ingredients"])), key=lambda x: len(x))

print(len(cleaned_drugs))
print(cleaned_drugs[100].keys())

for item in cleaned_drugs:
    item["uuid"] = str(uuid.uuid4())

with open("./final.json", "w") as file:
    json.dump(cleaned_drugs, file)


keys_list = ['current_status', 'monograph_date',
             'product_monograph', 'monograph_date_parsable']

news = []
match_index = defaultdict(list)
for item in old_dpds:
    match_index[item["din"]].append(item)


for item in cleaned_drugs:
    match = match_index.get(item["din"])
    if match is not None:
        for element in keys_list:
            item[element] = match[0][element]
    if match is None:
        news.append({"din": item["din"], "drug_code": item["drug_code"]})


for item in news:
    try:
        result = get_product_page(item["drug_code"])
        if result is not None:
            item["current_status"] = result["current_status"]
            item["product_monograph"] = result["product_monograph"]
            item["monograph_date"] = result["monograph_date"]
            item["monograph_date_parsable"] = result["monograph_date_parsable"]
    except:
        print(item["drug_code"], "failed")


for item in news:
    target = [x for x in cleaned_drugs if x["drug_code"] == item["drug_code"]]
    if len(target) > 0:
        target = target[0]
        for element in keys_list:
            target[element] = item[element]


updated_dpd_collection = db["updated_dpd"]
result = updated_dpd_collection.insert_many(cleaned_drugs)
print(result)


copied_cleaned_drugs = copy.deepcopy(cleaned_drugs)


for item in copied_cleaned_drugs:
    item["_id"] = str(item["_id"])
    item["monograph_date_parsable"] = str(item["monograph_date_parsable"])

with open("./updated_dpd.json", "w") as file:
    json.dump(copied_cleaned_drugs, file)


# attempting to categorize drugs into tms


singles = [x for x in cleaned_drugs if x["number_of_ais"] == "1"]

ings = sorted(list(set([x["list_ingredients"][0] for x in singles])))
ings = [{"text": x, "det": False, "tm": ""} for x in ings]


def clean_paranthesis(item):
    splitted = item["text"].split(" ")
    print(splitted)
    if len(splitted) > 1:
        condition = splitted[0] == splitted[1][1:]
        print(condition)
        if condition:
            item["tm"] = splitted[0]
            item["det"] = True
    if len(splitted) == 1:
        item["tm"] = splitted[0]
        item["det"] = True


for item in ings:
    clean_paranthesis(item)

cleaned = [x for x in ings if x["det"] == True]
for item in ings:
    if item["det"] == False:
        target = [x for x in cleaned if x["tm"] in item["text"]]
        if len(target) > 0:
            # print(item["text"],target)
            inter = list(set(x["tm"] for x in target))[0]
            item["det"] = True
            item["tm"] = inter

cleaned = [x for x in ings if x["det"] == True]


with open("./data_artifacts/drugs_ccd.json", "r") as file:
    ccds = json.load(file)

cleaned_ccds = [x for x in ccds if len(x["tm"].split(" ")) == 1]


for item in ings:
    if item["det"] == False:
        target = [x for x in cleaned_ccds if x["tm"] in item["text"]]
        if len(target) > 0:
            print(item["text"])
            item["tm"] = target[0]["tm"]
            item["det"] = True


converstion_dict = {
    "penicillin": "penicillin",
    "valproic acid": "valproic acid",
    "tenofovir": "tenofovir",
    "insulin": "insulin",
    "ascorbic acid": "ascorbic acid",
    "acetylsalicylic acid": "asprin",
    "folic acid": "folic acid",
    "mineral oil": "mineral oil",
    "ethacrynic acid": "ethacrynic acid",
    "fusidic acid": "fusidic acid",
    "mycophenolate mofetil": "mycophenolate mofetil",

}


for item in ings:
    if item["det"] == False:
        print(item["text"])
        if len(item["text"].split(" ")) > 4:
            item["tm"] = item["text"]
            item["det"] = True
        if "mineral oil" in item["text"]:
            item["tm"] = "mineral oil"
            item["det"] = True
        if item["text"].startswith("sodium"):
            item["tm"] = item["text"]
            item["det"] = True
        if item["text"].startswith("vitamin"):
            item["tm"] = item["text"]
            item["det"] = True
        for member in converstion_dict.keys():
            if member in item["text"]:
                item["tm"] = converstion_dict[member]
                item["det"] = True


for item in ings:
    if item["det"] == False:
        item["tm"] = item["text"]
        item["det"] = True


unique_tms = sorted(list(set([x["tm"] for x in ings])))


new_tms = []
for item in unique_tms:
    ing_target = [x["text"] for x in ings if x["tm"] == item]
    family = [x for x in singles if x["list_ingredients"][0] in ing_target]
    inter = {}
    inter["tm"] = item
    inter["family"] = family
    new_tms.append(inter)
