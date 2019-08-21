import requests
from datetime import date
import gbif_dbtools as db

# Get no. of images in specimen collection
query_image_count = {
    "resource_id": "05ff2255-c38a-40c9-b657-4ccb55ab2feb",
    "raw_result": True,
    "search": {
        "size": 0,
        "aggs": {"media_count": {"sum": {"field": "data.associatedMediaCount.number"}}}
    }
}

today = date.today()

r = requests.post('https://data.nhm.ac.uk/api/3/action/datastore_search_raw', json=query_image_count)
result_image_count = r.json()['result']['aggregations']['media_count']['value']

# Get no. specimens with at least one image attached
query_specimens_imaged = {
    "resource_id": "05ff2255-c38a-40c9-b657-4ccb55ab2feb",
    "raw_result": True,
    "search": {
        "size": 0,
        "query": {
            "exists": {"field": "data.associatedMedia"}
        }
    }
}

r2 = requests.post('https://data.nhm.ac.uk/api/3/action/datastore_search_raw', json=query_specimens_imaged)
result_imaged_specimens = r2.json()['result']['hits']['total']

# Insert into dashboard.specimen_images
sql = f"INSERT INTO specimen_images (date, image_count, imaged_specimens, resource_id) VALUES ('{today}', " \
    f"{result_image_count}, {result_imaged_specimens}, " \
    f"'05ff2255-c38a-40c9-b657-4ccb55ab2feb')"

cursor = db.query_db(sql)
cursor.close()
