import requests
import pymysql


def citation_list():
    # Get list of articles that cited NHM specimens
    try:
        r = requests.get('https://www.gbif.org/api/resource/search?contentType='
                         'literature&limit=100&gbifDatasetKey=7e380070-f762-11e1-a439-00145eb45e9a')
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)

    # dict to hold gbifDownloadKey: id pairs to return + pass to occurrence() later
    download_keys = {}
    separator = ', '

    # Connect to database:
    with pymysql.connect(host="localhost", user="root", password="r3ptar", db="dashboard") as cursor:

        contents = r.json()['results']
        for c in contents:
            # Make sure record has an id, skip if not
            if not c.get('id', None):
                id = c['id']
                abstract = c['abstract'] if 'abstract' in c else None
                city = c['city'] if 'city' in c else None
                content_type = c['contentType '] if 'contentType' in c else None
                harvest_date = c['accessed'] if 'accessed' in c else None
                doi = c['identifiers']['doi'] if 'doi' in c['identifiers'] else None
                language = c['language'] if 'language' in c else None
                literature_type = c['literatureType'] if 'literatureType' in c else None
                open_access = c['openAccess'] if 'openAccess' in c else None
                peer_review = c['peerReview'] if 'peerReview' in c else None
                publisher = c['publisher'] if 'publisher' in c else None
                source = c['source'] if 'source' in c else None
                title = c['title'] if 'title' in c else None
                year = c['year'] if 'year' in c else None
                # Concatenate fields that may contain lists
                countries_of_researcher = separator.join(c['countriesOfResearcher']) if 'countriesOfResearcher' \
                                                                                        in c else None
                topics = separator.join(c['topics']) if 'topics' in c else None
                gbif_download_key = separator.join(c['gbifDownloadKey']) if 'gbifDownloadKey' in c else None
                # Parse author name dicts
                if 'authors' in c:
                    namelist = []
                    for n in c['authors']:
                        namelist.append(' '.join(n.values()))
                    authors = separator.join(namelist)
            else:
                continue


def occurrence():
    # Get occurrence metadata, including contribution ids
    return ""


def dataset():
    # Get basic metadata for each contributing dataset
    return ""


def organizations():
    # Get organizational details
    return ""


if __name__ == '__main__':
    citation_list()
