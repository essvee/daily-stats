import requests
import pymysql
import gbif_dbtools as db


def assemble_parts(citation_id):
    """
    Retrieves and reshapes data from GBIF API:
    Get citation keys, use them to get gbif_key(s) for each GBIF dataset cited in the paper.
    Use g_keys to get dataset_key for each contributing organisation's subset of records.
    Use d_key to get the org_key: group record counts together by organisation.
    :param citation_id: citation_id taken from gbif_citations.id
    """
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
        db.query_db(f"""INSERT INTO gbif_occurrences VALUES (null, "{d['citation_key']}", "{d['orgKey']}",
         {d['record_count']});""")


def get_gbif_keys(citation_id):
    """
    Use citation_id to get gbif_key(s) for each GBIF dataset cited in the paper.
    :param citation_id: string ID of citation being processed
    :return: List<String> of gbif download keys
    """
    try:
        cursor = db.query_db(f"SELECT gbif_download_key FROM gbif_citations WHERE id = '{citation_id}';")
        g_keys = cursor.fetchone()[0].split("; ")
        return g_keys
    except pymysql.Error as e:
        print(e)


def get_dataset_keys(g_key):
    """
    Get a dataset key for each combination of organisation + dataset that contributed to the cited
    dataset.
    :param g_key:
    :return: List<Dict> of keys for each component dataset and the number of records in each dataset.
    """
    record_sets = []
    try:
        count = requests.get(f"http://api.gbif.org/v1/occurrence/download/{g_key}/datasets?limit=0").json()['count']
        offset = 0
        end_of_results = False
        while not end_of_results:
            r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{g_key}/datasets?limit=500&"
                             f"offset={offset}").json()
            offset += 500
            end_of_results = r['endOfRecords']

            # Get the info we're interested in and return
            for d in r['results']:
                dataset = {'d_key': d['datasetKey'], 'record_count': d['numberRecords']}
                record_sets.append(dataset)
    except requests.exceptions.HTTPError as e:
        print(e)

    return record_sets


def get_org_keys(d_key):
        """
        Use download key to get org_key of the publishing organisation.
        :param d_key: String ID of one organisation + dataset that contributed to the result
        :return: String organisation key
        """
        try:
            r = requests.get(f"http://api.gbif.org/v1/dataset/{d_key}")
            r.raise_for_status()
            return r.json()['publishingOrganizationKey']
        except requests.exceptions.HTTPError as e:
            print(e)
