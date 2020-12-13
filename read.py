import csv
import json
from typing import Optional
from collections import OrderedDict
from pymongo import MongoClient
from types import SimpleNamespace

# Parse josn file into dict->dict
def create_objects(csv_file: str, **kwargs) -> Optional[list]:
    csvfile = open(csv_file, 'r', encoding='Latin1')

    entries = []

    fieldnames = ("data_id","event_id_cnty",
    "event_id_no_cnty","event_date",
    "year","time_precision","event_type",
    "sub_event_type","actor1","assoc_actor_1",
    "inter1","actor2","assoc_actor_2",
    "inter2","interaction",
    "region","country","admin1",
    "admin2","location","latitude","longitude"
    ,"geo_precision","source","source_scale"
    ,"notes", "timestamp","iso3")

    reader = csv.DictReader(csvfile, fieldnames)
    # Skip name of columns and descrption
    iter_rows = iter(reader)
    next(iter_rows)
    next(iter_rows)

    # Dicts for nested elements
    date_row = OrderedDict()
    event_row = OrderedDict()
    participants_row = OrderedDict()
    location_row = OrderedDict()
    gps_row = OrderedDict()
    
    try:
        # Create dict
        for row in reader:
            # Reset values for every element in dict 
            date_row = OrderedDict()
            event_row = OrderedDict()
            entry = OrderedDict()
            participants_row = OrderedDict()
            location_row = OrderedDict()
            gps_row = OrderedDict()

            for field in fieldnames:
                element_field = row[field]
                # Date nested
                if not element_field.strip() and not element_field:
                    element_field = "Empty"
                if field == "data_id":
                    element_field = int(element_field)
                    entry[field] = element_field
                if field == "event_date" or field == "year" or field == "timestamp":
                    if field !=  "event_date":
                        element_field = int(element_field)
                    date_row.update({field : element_field })
                    entry["date"] = date_row
                # Event nested
                elif field == "event_id_cnty" or field == "event_id_no_cnty" or field == "event_type" or field == "sub_event_type":
                    element_field = element_field.split('/')
                    if len(element_field) == 1:
                        element_field = element_field[0]
                    if field == "event_id_no_cnty":
                        element_field = float(element_field)
                    event_row.update({field : element_field })
                    entry["events"] = event_row
                # Participants nested
                elif field == "actor1" or field == "assoc_actor_1" or field == "actor2" or field == "assoc_actor_2":
                    element_field = element_field.split(';')
                    if len(element_field) == 1:
                        element_field = element_field[0]
                    participants_row.update({field: element_field})
                    entry["participants"] = participants_row   
                # Country location nested
                elif field == "region" or field == "country" or field == "admin1" or field == "admin2":
                    location_row.update({field: element_field})
                    entry["country_locations"] = location_row
                # Gps nested
                elif field == "location" or field == "latitude" or field == "longitude" or field == "geo_precision":
                    if field != "location":
                        element_field = float(element_field)
                    gps_row.update({field: element_field})
                    entry["gps"] = gps_row
                # Source split
                elif field == "source":
                    element_field = element_field.split(';')
                    if len(element_field) == 1:
                        element_field = element_field[0]
                    entry[field] = element_field
                elif field == "inter1" or field == "inter2" or field == "interaction" or field == "time_precision":
                    element_field = int(element_field)
                    entry[field] = element_field                    
                else:
                    entry[field] = element_field
            entries.append(entry)
    except Exception:
        return None
    return entries

def save_to_db(elements: Optional[list], **kwargs) -> bool:
    if not elements:
        return False
    try:
        client = MongoClient("localhost", 27017)
        db = client["aremeniaDB"]
        elements_collection = db["ArmeniaConflicts"]
        elements_collection.insert_many(elements)
    except ConnectionFailure:
        return False

    return True

db_elements = create_objects(csv_file='./files/conflict_data_arm.csv')
result = save_to_db(elements=db_elements)

if result:
    print("Successfully inserted records to mongoDB")
else:
    print("Error while working with localhost mongoDB")