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
            # Find out if we've seen this org before for this citation (might be different dataset)
            key = get_org_keys(d['d_key'])

            # If org is already there, increment the record count accordingly
            org_rec_count[key] += d['record_count']

    for key, count in org_rec_count.items():
        db.query_db(f"""INSERT INTO gbif_occurrences VALUES (null, "{citation_id}", "{key}",
         {count});""")
        # TODO() executeAll() instead?


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
        r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{g_key}/datasets?limit=500&"
                         f"offset={offset}").json()

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
    assemble_parts('0d372067-5567-331b-bb80-ef61cb34092b')