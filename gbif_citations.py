import requests


def citation_list():
    # Get list of articles that cited NHM specimens
    try:
        r = requests.get('https://www.gbif.org/api/resource/search?contentType='
                         'literature&limit=100&gbifDatasetKey=7e380070-f762-11e1-a439-00145eb45e9a')
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)


def occurrence():
    # Get occurrence metadata, including contribution ids
    return ""


def dataset():
    # Get basic metadata for each contributing dataset
    return ""


def organizations():
    # Get organizational details
    return ""
