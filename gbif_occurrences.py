import requests
import pymysql


def get_parts(citation_id):
    # Get gbif keys from local database - may be > 1 value
    g_keys = get_gbif_keys(citation_id)

    datasets = {}
    # For each gbif key, get the component dataset keys and record counts
    for k in g_keys:
        for d in get_dataset_keys(k):

            # Find out if we've seen this org before for this citation (might be different dataset)
            d['orgKey'] = get_org_keys(d['d_key'])
            d['citation_key'] = citation_id

            # If org is already there, increment the record count accordingly
            if d['orgKey'] in datasets:
                datasets[d['orgKey']]['record_count'] += d['record_count']
            else:
                datasets[d['orgKey']] = d

    for i, d in datasets.items():
        query_db(f"""INSERT INTO gbif_occurrences VALUES (null, "{d['citation_key']}", "{d['orgKey']}", {d['record_count']});""")


# Use download keys for each dataset to organization key
def get_org_keys(d_key):
        try:
            r = requests.get(f"http://api.gbif.org/v1/dataset/{d_key}")
            r.raise_for_status()
            return r.json()['publishingOrganizationKey']
        except requests.exceptions.HTTPError as e:
            print(e)


def update_publishers():
    pubKeys = get_publishers()
    try:
        count = requests.get("http://api.gbif.org/v1/organization?limit=0").json()['count']
        offset = 0
        while count > offset:
            results = requests.get(f"http://api.gbif.org/v1/organization?limit=200&offset={offset}").json()['results']
            for p in results:
                # If org is new, add the name, key and country to the table
                if p['key'] not in pubKeys:
                    publisher_name = p['title'].replace("\"", "\'").encode('ascii', 'ignore').decode('utf-8', 'ignore')
                    publisher_country = p['country'] if 'country' in p else None
                    query_db(f"""INSERT INTO gbif_publishers VALUES ("{p['key']}", "{publisher_name}","{publisher_country}")""")
                    print(p['key'])
            # Increment offset
            offset += 200

    except requests.exceptions.HTTPError as e:
        print(e)


def get_publishers():
    cursor = query_db(f"SELECT publisher_key FROM gbif_publishers;")
    pubKeys = set()
    try:
        for (publisher_key) in cursor:
            pubKeys.add(publisher_key[0])
        return pubKeys
    except pymysql.Error as e:
        print(e)


def query_db(sql):
    host, user, password, database = get_keys('server-permissions.txt')
    with pymysql.connect(host=host, user=user, password=password, db=database) as cursor:
        try:
            cursor.execute(sql)
            return cursor
        except pymysql.Error as e:
            print(e)


def get_keys(filename):
    with open(filename, 'r') as f:
        keys = f.read().splitlines()
        return keys


# Get gbif download keys for one citation from local citations table
def get_gbif_keys(citation_id):
    try:
        cursor = query_db(f"SELECT gbif_download_key FROM gbif_citations WHERE id = '{citation_id}';")
        g_keys = cursor.fetchone()[0].split("; ")
        return g_keys
    except pymysql.Error as e:
        print(e)


# Use download keys to get details about other contributing datasets from GBIF
def get_dataset_keys(g_key):
    record_sets = []
    try:
        count = requests.get("http://api.gbif.org/v1/organization?limit=0").json()['count']
        offset = 0
        while count > offset:
            results = requests.get(f"http://api.gbif.org/v1/occurrence/download/{g_key}/datasets?limit=500&offset={offset}").json()['results']
            offset += 500
            # Get the info we're interested in for each other dataset and return
            for d in results:
                dataset = {'d_key': d['datasetKey'], 'record_count': d['numberRecords']}
                record_sets.append(dataset)
    except requests.exceptions.HTTPError as e:
        print(e)

    print(len(record_sets))
    return record_sets


occurrences = {}
if __name__ == '__main__':
    get_parts('61455454-5239-3266-8e05-673f458a11ef')
