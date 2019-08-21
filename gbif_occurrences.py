import requests
import gbif_dbtools as db
import itertools
import collections


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
    org_rec_count = collections.Counter()

    # For each gbif key, get the component dataset keys and record counts
    for k in g_keys:
        for d in get_dataset_keys(k):
            # Get publishing organization ID
            key = get_org_keys(d['d_key'])

            # If org is already there, increment the record count accordingly
            org_rec_count[key] += d['record_count']

    for key, count in org_rec_count.items():
        db.query_db(f"""INSERT INTO gbif_occurrences VALUES (null, "{citation_id}", "{key}", {count});""")


def get_gbif_keys(citation_id):
    """
    Use citation_id to get gbif_key(s) for each GBIF dataset cited in the paper.
    :param citation_id: string ID of citation being processed
    :return: List<String> of gbif download keys
    """
    cursor = db.query_db(f"SELECT gbif_download_key FROM gbif_citations WHERE id = '{citation_id}';")
    g_keys = cursor.fetchone()[0].split("; ")
    return g_keys


def get_dataset_keys(g_key):
    """
    Get a dataset key for each combination of organisation + dataset that contributed to the cited
    dataset.
    :param g_key:
    :return: List<Dict> of keys for each component dataset and the number of records in each dataset.
    """
    record_sets = []

    for offset in itertools.count(step=500):
        r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{g_key}/datasets?limit=500&offset={offset}").json()

        # Get the info we're interested in and return
        for d in r['results']:
            dataset = {'d_key': d['datasetKey'], 'record_count': d['numberRecords']}
            record_sets.append(dataset)

        if r['endOfRecords']:
            break

    return record_sets


def get_org_keys(d_key):
        """
        Use download key to get org_key of the publishing organisation.
        :param d_key: String ID of one organisation + dataset that contributed to the result
        :return: String organisation key
        """
        r = requests.get(f"http://api.gbif.org/v1/dataset/{d_key}")
        r.raise_for_status()
        return r.json()['publishingOrganizationKey']


if __name__ == '__main__':
    top_up = ['9eda626a-a684-3403-93e3-d5311628c6b0', '9fb8af30-af45-3038-a913-c4c2a0ed2a76', 'b228b5db-489d-3213-a15b-180496c361ad',
         'b3b3e04c-97c6-3e03-ba5e-1148ec56f330', 'b6bf2031-c905-35e5-b3db-87463a5035bf', 'c52170b4-7ecb-3bf5-9065-f208445569dc',
          'c6fdb24e-1c09-3f6f-93e4-b3f2cb796a33', 'd3963e13-5226-3d6c-8983-8a94d9d9a417', 'd85bbf54-eb0d-3612-acd4-534efde5efad',
           'f85c1a25-32b7-32d2-a4df-96510a20643b', 'fa38fb17-9f04-38e0-9e0e-237fc7314c6f', 'fdacad58-2a25-316a-b82e-f588fc26625a']

    for t in top_up:
        print(f"Gathering data for {t}...")
        assemble_parts(t)
