import requests
import pymysql
import datetime


def get_list():
    # Get list of articles that cited NHM specimens
    try:
        r = requests.get('https://www.gbif.org/api/resource/search?contentType='
                         'literature&limit=100&gbifDatasetKey=7e380070-f762-11e1-a439-00145eb45e9a')
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)

    contents = r.json()['results']
    # Cycle through contents and delete any without an id
    for c in contents:
        if 'id' not in c:
            contents.remove(c)

    map_fields(contents)


def map_fields(contents):

    # dict to hold gbifDownloadKey: id pairs to return + pass to occurrence() later
    download_keys = {}
    separator = '; '

    for c in contents:
        citation_dict = {}

        # Extract update_date to check against version in database
        citation_dict['update_date'] = datetime.date(int(c['updatedAt'][0:4]), int(c['updatedAt'][5:7]),
                                                     int(c['updatedAt'][8:10]))
        citation_dict['gid'] = c['id']
        citation_dict['abstract'] = c['abstract'].replace("'", "''") if 'abstract' in c else None
        citation_dict['city'] = c['city'] if 'city' in c else None
        citation_dict['content_type'] = c['contentType'] if 'contentType' in c else None
        citation_dict['harvest_date'] = c['accessed'] if 'accessed' in c else '1111-01-01'
        citation_dict['language'] = c['language'] if 'language' in c else None
        citation_dict['literature_type'] = c['literatureType'] if 'literatureType' in c else None
        citation_dict['open_access'] = c['openAccess'] if 'openAccess' in c else None
        citation_dict['peer_review'] = c['peerReview'] if 'peerReview' in c else None
        citation_dict['publisher'] = c['publisher'].replace("'", "''") if 'publisher' in c else None
        citation_dict['source'] = c['source'].replace("'", "''") if 'source' in c else None
        citation_dict['title'] = c['title'].replace("'", "''") if 'title' in c else None
        citation_dict['year'] = c['year'] if 'year' in c else None

        # Concatenate fields that may contain lists
        citation_dict['countries_of_researcher'] = \
            separator.join(c['countriesOfResearcher']) if 'countriesOfResearcher' in c else None
        citation_dict['topics'] = separator.join(c['topics']) if 'topics' in c else None

        # Get DOI if available
        if 'identifiers' in c:
            citation_dict['doi'] = c['identifiers']['doi'] if 'doi' in c['identifiers'] else None
        else:
            citation_dict['doi'] = None

        # Parse author name dicts
        if 'authors' in c:
            namelist = []
            for n in c['authors']:
                namelist.append(' '.join(n.values()))
            citation_dict['authors'] = separator.join(namelist).replace("'", "''")
        else:
            citation_dict['authors'] = None

        # Use gbifDownloadKey as dict key - should be unique
        if 'gbifDownloadKey' in c:
            citation_dict['gbif_download_key'] = separator.join(c['gbifDownloadKey'])
            for gkey in c['gbifDownloadKey']:
                download_keys[gkey] = c['id']
        else:
            citation_dict['gbif_download_key'] = None

        update_or_delete(citation_dict)


def update_or_delete(cd):

    # Connect to database:
    with pymysql.connect(host="localhost", user="root", password="r3ptar", db="dashboard") as cursor:

        # Check if citation is new
        sql = f"SELECT DISTINCT id FROM gbif_citations;"
        try:
            cursor.execute(sql)
            ids = []

            # List of ids already seen
            for n in cursor.fetchall():
                ids.append(n[0])

            if cd['gid'] not in ids:
                add_sql = f"INSERT INTO gbif_citations(abstract, authors, city, " \
                          f"content_type, countries_of_researcher, gbif_download_key," \
                          f"id, harvest_date, doi, language, literature_type, open_access," \
                          f"peer_review, publisher, source, title, topics, update_date, year)" \
                          f"VALUES('{cd['abstract']}', '{cd['authors']}', '{cd['city']}', '{cd['content_type']}'," \
                          f"'{cd['countries_of_researcher']}', '{cd['gbif_download_key']}', '{cd['gid']}'," \
                          f"'{cd['harvest_date']}', '{cd['doi']}', '{cd['language']}', '{cd['literature_type']}'," \
                          f"'{cd['open_access']}', '{cd['peer_review']}', '{cd['publisher']}', '{cd['source']}'," \
                          f"'{cd['title']}', '{cd['topics']}', '{cd['update_date']}', {cd['year']});"

                add_sql = add_sql.encode('ascii', 'ignore').decode('utf-8', 'ignore')
                cursor.execute(add_sql)

        except pymysql.Error as e:
            print(add_sql)
            print(e)


if __name__ == '__main__':
    get_list()
