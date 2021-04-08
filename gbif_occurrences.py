import requests
import itertools


def assemble_parts(citation_record):
    """
    Retrieves and reshapes data from GBIF API:
    Use download keys to retrieve download counts + source dataset keys for each download cited in the paper.
    :param citation_record: citation dict to be updated and returned
    """
    # Get gbif keys from local database - may be > 1 value
    gbif_download_keys = citation_record['gbif_dk_list']

    # Temporary structures to aggregate and de-duplicate dataset references and numberRecords for
    # each dataset that's part of each download for the current citation
    total_citation_record_count = 0
    all_citation_datasets = set()
    total_nhm_record_count = 0

    # For each gbif key, get the component dataset keys and record counts
    for k in gbif_download_keys:
        # Get overall record count for this download record and increment total_citation_record_count
        download_record_count = get_download_counts(k)
        total_citation_record_count += download_record_count

        # Get list of datasets included in the download and number of NHM records, if any
        datasets, nhm_record_count = get_dataset_details(k)
        # Union with the existing set dataset identifiers
        all_citation_datasets = all_citation_datasets | datasets
        # increment NHM count
        total_nhm_record_count += nhm_record_count

    # update the citation record with total count of nhm records, total records cited and dataset count
    citation_record['nhm_record_count'] = total_nhm_record_count
    citation_record['total_record_count'] = total_citation_record_count
    citation_record['total_dataset_count'] = len(all_citation_datasets)

    return citation_record


def get_download_counts(gbif_download_key):
    # Get the overall metadata for this download first + grab total number of records
    url = f"http://api.gbif.org/v1/occurrence/download/{gbif_download_key}"
    r = requests.get(url)
    try:
        r.raise_for_status()
        download_results = r.json()
        download_total_records = download_results['totalRecords']
        return download_total_records
    except ValueError as e:
        print(e)


def get_dataset_details(gbif_download_key):
    # Get the id of each contributing dataset and record count of any which are from the NHM specimen collection
    datasets = set()
    nhm_record_count = 0

    for offset in itertools.count(step=500):

        r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{gbif_download_key}"
                         f"/datasets?limit=500&offset={offset}").json()

        # Get the info we're interested in and return
        for d in r['results']:
            dataset_key = d['datasetKey']
            datasets.add(dataset_key)

            if dataset_key == '7e380070-f762-11e1-a439-00145eb45e9a':
                nhm_record_count += d['numberRecords']

        if r['endOfRecords']:
            break

    return datasets, nhm_record_count
