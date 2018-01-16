import requests
import pymysql


def get_parts(citation_id):
    # Get gbif keys - may be > 1 value
    g_keys = get_gbif_keys(citation_id)

    datasets = {}
    # For each gbif key, get the component dataset keys and record counts
    for k in g_keys:
        dataset_list = get_dataset_keys(k)
        for d in dataset_list:
            # If datasets is duplicated, increment the record count accordingly
            if d['d_key'] in datasets:
                datasets[d['d_key']]['record_count'] += d['record_count']
            else:
                datasets[d['d_key']] = d


def get_gbif_keys(citation_id):
    # Get gbif download keys from citations table
    with open('local_details.txt', 'r') as f:
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


def get_dataset_keys(g_key):
    try:
        r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{g_key}/datasets?limit=100")
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)

    record_sets = []

    for d in r.json()['results']:
        dataset = {}
        dataset['d_key'] = d['datasetKey']
        dataset['record_count'] = d['numberRecords']
        record_sets.append(dataset)

    return record_sets


def count_datasets(temp_occurrences):
    for d in temp_occurrences:
        # For each gkey, insert into api url, parse results and get 'count'
        try:
            r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{d}/datasets?limit=10")
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(e)

        dataset_count = r.json()['count']
        occurrences[d]['dataset_count'] = dataset_count

    print(occurrences)


def organizations():
    try:
        r = requests.get("http://api.gbif.org/v1/organization")
        r.raise_for_status()
        org_count = r.json()['count']

        r = requests.get(f"http://api.gbif.org/v1/organization?limit={org_count}")
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)

    results = r.json()['results']
    datasets = 0
    data_publishers = 0

    for org in results:
        if org['numPublishedDatasets'] > 0:
            data_publishers += 1
            datasets += org['numPublishedDatasets']

    print(f"There are {datasets} datasets published to GBIF by {data_publishers} organizations.")



occurrences = {}
if __name__ == '__main__':
    get_parts('146e8e22-4bed-3ee2-bca3-d77fc537e2c8')
