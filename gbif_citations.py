import requests
import pymysql
import datetime
import gbif_dbtools as db
import gbif_occurrences as go

new_ids = []


def get_list():
    """
    Get list of NHM data citations from GBIF api
    """
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
    """
    Flatten json and add default values where required
    :param contents: json object containing info about NHM data citations
    """
    separator = '; '
    all_citations = {}

    for c in contents:
        citation_dict = {'update_date': datetime.date(int(c['updatedAt'][0:4]), int(c['updatedAt'][5:7]),
                                                      int(c['updatedAt'][8:10])), 'gid': c['id'],
                         'abstract': c['abstract'].replace("'", "''") if 'abstract' in c else None,
                         'city': c['city'] if 'city' in c else None,
                         'content_type': c['contentType'] if 'contentType' in c else None,
                         'harvest_date': c['accessed'] if 'accessed' in c else '1111-01-01',
                         'language': c['language'] if 'language' in c else None,
                         'literature_type': c['literatureType'] if 'literatureType' in c else None,
                         'open_access': c['openAccess'] if 'openAccess' in c else None,
                         'peer_review': c['peerReview'] if 'peerReview' in c else None,
                         'publisher': c['publisher'].replace("'", "''") if 'publisher' in c else None,
                         'source': c['source'].replace("'", "''") if 'source' in c else None,
                         'title': c['title'].replace("'", "''") if 'title' in c else None,
                         'year': c['year'] if 'year' in c else None, 'month': c['month'] if 'month' in c else 0,
                         'countries_of_researcher': separator.join(
                             c['countriesOfResearcher']) if 'countriesOfResearcher' in c else None,
                         'topics': separator.join(c['topics']) if 'topics' in c else None}

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
        else:
            citation_dict['gbif_download_key'] = None

        # Adds this citation to the overall list
        all_citations[c['id']] = citation_dict

    update_or_delete(all_citations)


def update_or_delete(all_citations):
    """
    Checks through citation database to see if anything is new or updated
    :param all_citations: dict containing cleaned citation info
    """
    cursor = db.query_db(f"SELECT id, update_date FROM gbif_citations;")
    try:
        # Create dict of ids already recorded
        ids = {}

        for n in cursor.fetchall():
            ids[n[0]] = n[1]

        # adds/updates new/amended records
        for gid, c in all_citations.items():
            # Creates YYYY-MMM-DD string for publication date
            pub_date = f"{c['year']}-{c['month']}-01" if c['month'] > 0 else "1000-00-00"
            if c['gid'] not in ids:
                sql = f"INSERT INTO gbif_citations(abstract, authors, city, " \
                      f"content_type, countries_of_researcher, gbif_download_key," \
                      f"id, harvest_date, doi, language, literature_type, open_access," \
                      f"peer_review, publisher, source, title, topics, update_date, year, month, pub_date)" \
                      f"VALUES('{c['abstract']}', '{c['authors']}', '{c['city']}', '{c['content_type']}'," \
                      f"'{c['countries_of_researcher']}', '{c['gbif_download_key']}', '{c['gid']}'," \
                      f"'{c['harvest_date']}', '{c['doi']}', '{c['language']}', '{c['literature_type']}'," \
                      f"'{c['open_access']}', '{c['peer_review']}', '{c['publisher']}', '{c['source']}'," \
                      f"'{c['title']}', '{c['topics']}', '{c['update_date']}', {c['year']}, {c['month']}," \
                      f"'{pub_date}');"

                # If new citation, trigger download of occurrence data
                new_ids.append(c['gid'])

            elif c['update_date'] > ids[c['gid']]:
                sql = f"UPDATE gbif_citations SET abstract = '{c['abstract']}', authors = '{c['authors']}', " \
                      f"city = '{c['city']}', content_type = '{c['content_type']}', " \
                      f"countries_of_researcher = '{c['countries_of_researcher']}', " \
                      f"gbif_download_key = '{c['gbif_download_key']}', " \
                      f"harvest_date = '{c['harvest_date']}', doi = '{c['doi']}', language = '{c['language']}', " \
                      f"literature_type = '{c['literature_type']}', open_access = '{c['open_access']}', " \
                      f"peer_review = '{c['peer_review']}', publisher = '{c['publisher']}', " \
                      f"source = '{c['source']}', title = '{c['title']}', topics = '{c['topics']}', " \
                      f"update_date = '{c['update_date']}', year = {c['year']}, month = {c['month']}, " \
                      f"pub_date = '{pub_date}'" \
                      f"WHERE id = '{c['gid']}';"

            # Skip if we've seen the rec before and it hasn't changed since then
            else:
                continue

            sql = sql.encode('ascii', 'ignore').decode('utf-8', 'ignore')
            db.query_db(sql)

    except pymysql.Error as e:
        print(sql)
        print(e)


def occurrences():
    """
    Triggers gbif_occurrences script to run for any new citation ids
    """
    # TODO() Wrap in try/catch block to get MySQL errors - email notification
    for n in new_ids:
        go.assemble_parts(n)


if __name__ == '__main__':
    get_list()
    occurrences()
