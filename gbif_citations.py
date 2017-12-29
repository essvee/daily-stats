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
    all_citations = {}

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

        # Adds this citation to the overall list
        all_citations[c['id']] = citation_dict

    update_or_delete(all_citations)


def update_or_delete(all_citations):
    # Get auth details + date
    with open('local_details.txt', 'r') as f:
        keys = f.read().splitlines()
        host, user, password, database = keys

    # Connect to database:
    with pymysql.connect(host=host, user=user, password=password, db=database) as cursor:

        # todo - check against update date
        sql = f"SELECT id, update_date FROM gbif_citations;"
        try:
            cursor.execute(sql)

            # Create list of ids already recorded
            ids = {}
            for n in cursor.fetchall():
                ids[n[0]] = n[1]

            for gid, c in all_citations.items():
                if c['gid'] not in ids:
                    sql = f"INSERT INTO gbif_citations(abstract, authors, city, " \
                              f"content_type, countries_of_researcher, gbif_download_key," \
                              f"id, harvest_date, doi, language, literature_type, open_access," \
                              f"peer_review, publisher, source, title, topics, update_date, year)" \
                              f"VALUES('{c['abstract']}', '{c['authors']}', '{c['city']}', '{c['content_type']}'," \
                              f"'{c['countries_of_researcher']}', '{c['gbif_download_key']}', '{c['gid']}'," \
                              f"'{c['harvest_date']}', '{c['doi']}', '{c['language']}', '{c['literature_type']}'," \
                              f"'{c['open_access']}', '{c['peer_review']}', '{c['publisher']}', '{c['source']}'," \
                              f"'{c['title']}', '{c['topics']}', '{c['update_date']}', {c['year']});"

                elif c['update_date'] > ids[c['gid']]:
                    sql = f"UPDATE gbif_citations SET abstract = '{c['abstract']}', authors = '{c['authors']}', " \
                          f"city = '{c['city']}', content_type = '{c['content_type']}', " \
                          f"countries_of_researcher = '{c['countries_of_researcher']}', " \
                          f"gbif_download_key = '{c['gbif_download_key']}', " \
                          f"harvest_date = '{c['harvest_date']}', doi = '{c['doi']}', language = '{c['language']}', " \
                          f"literature_type = '{c['literature_type']}', open_access = '{c['open_access']}', " \
                          f"peer_review = '{c['peer_review']}', publisher = '{c['publisher']}', " \
                          f"source = '{c['source']}', title = '{c['title']}', topics = '{c['topics']}', " \
                          f"update_date = '{c['update_date']}', year = {c['year']} " \
                          f"WHERE id = '{c['gid']}';"

                else:
                    continue

                sql = sql.encode('ascii', 'ignore').decode('utf-8', 'ignore')
                cursor.execute(sql)

        except pymysql.Error as e:
            print(sql)
            print(e)


if __name__ == '__main__':
    get_list()
