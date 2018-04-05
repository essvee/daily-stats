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
            d['orgName'] = get_org_names(d['orgKey'])
            d['citation_key'] = citation_id

            # If org is already there, increment the record count accordingly
            if d['orgName'] in datasets:
                datasets[d['orgName']]['record_count'] += d['record_count']
            else:
                datasets[d['orgName']] = d

    write_out(datasets)


def write_out(datasets):

    with open('server-permissions.txt', 'r') as f:
        keys = f.read().splitlines()
        host, user, password, database = keys

    for i, d in datasets.items():
        with pymysql.connect(host=host, user=user, password=password, db=database) as cursor:
            sql = f"INSERT INTO gbif_occurrences VALUES (null, '{d['citation_key']}', '{d['orgKey']}'," \
                  f" '{d['orgName']}', {d['record_count']});"
            try:
                cursor.execute(sql)

            except pymysql.Error as e:
                print(sql)
                print(e)

# Use download keys for each dataset to organization key
def get_org_keys(d_key):
        try:
            r = requests.get(f"http://api.gbif.org/v1/dataset/{d_key}")
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(e)

        return r.json()['publishingOrganizationKey']


def get_org_names(org_key):
    try:
        r = requests.get(f"http://api.gbif.org/v1/organization/{org_key}")
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)

    return r.json()['title']


# Get gbif download keys for one citation from local citations table
def get_gbif_keys(citation_id):
    with open('server-permissions.txt', 'r') as f:
        keys = f.read().splitlines()
        host, user, password, database = keys

    # Connect to database and grab keys from known citations:
    with pymysql.connect(host=host, user=user, password=password, db=database) as cursor:
        sql = f"SELECT gbif_download_key FROM gbif_citations WHERE id = '{citation_id}';"

        try:
            cursor.execute(sql)
            g_keys = cursor.fetchone()[0].split("; ")

        except pymysql.Error as e:
            print(sql)
            print(e)

    return g_keys


# Use download keys to get details about other contributing datasets from GBIF
def get_dataset_keys(g_key):
    try:
        r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{g_key}/datasets?limit=100")
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)

    record_sets = []

    # Get the info we're interested in for each other dataset and return
    for d in r.json()['results']:
        dataset = {'d_key': d['datasetKey'], 'record_count': d['numberRecords']}
        record_sets.append(dataset)

    return record_sets


occurrences = {}
if __name__ == '__main__':
    get_parts('146e8e22-4bed-3ee2-bca3-d77fc537e2c8')
