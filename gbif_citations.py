import requests
import pymysql
import datetime
import gbif_dbtools as db
import gbif_occurrences as go
import itertools

new_ids = []


def get_list():
    """
    Get list of NHM data citations from GBIF api
    """

    works = []

    # Get list of works that cited NHM specimens
    try:
        for offset in itertools.count(step=100):
            # url =f"https://www.gbif.org/api/resource/search?contentType=literature&limit=100&offset={offset}" \
            #      f"&gbifDatasetKey=7e380070-f762-11e1-a439-00145eb45e9a"
            url =f"https://api.gbif.org/v1/literature/search?gbifDataSetKey=7e380070-f762-11e1-a439-00145eb45e9a" \
                 f"&limit=100&offset={offset}"
            r = requests.get(url)
            r.raise_for_status()
            results = r.json()['results']

            # Cycle through contents and skip any without an id
            for result in results:
                if 'id' not in result:
                    continue
                else:
                    works.append(result)

            if r.json()['endOfRecords']:
                break

    except requests.exceptions.HTTPError as e:
        print(e)

    print(f"no. of results being passed to map_fields(): {len(works)}")
    map_fields(works)


def get_doi(record):
    # Get DOI if available
    if 'identifiers' in record:
        return record['identifiers']['doi'] if 'doi' in record['identifiers'] else None
    else:
        return None


def parse_author_names(record):

    if 'authors' in record:
        namelist = []
        for n in record['authors']:
            namelist.append(' '.join(n.values()))
        return '; '.join(namelist).replace("'", "''")
    else:
        return None


def get_gbif_download_key(record):
    # Todo: amend this function so it sparks the occurrence-aggregation/contributor data process
    # List of one to many dataset IDs that were used in the paper
    if 'gbifDownloadKey' in record:
        download_keys = '; '.join(record['gbifDownloadKey'])
        if len(download_keys) > 12000:
            print(f"download key length for {record['id']}: {len(download_keys)} ")
        return download_keys
    else:
        return None


def map_fields(works):
    """
    Flatten json and add default values where required
    :param contents: json object containing info about NHM data citations
    """
    separator = '; '
    all_citations = {}

    for c in works:
        citation_dict = {'update_date': datetime.datetime.strptime(c['modified'][0:10], "%Y-%m-%d").date(),
                         'gid': c['id'],
                         'abstract': c['abstract'].replace("'", "''") if 'abstract' in c else None,
                         'harvest_date': c['discovered'] if 'discovered' in c else '1111-01-01',
                         'pub_date': c['published'][0:10] if 'published' in c else '1111-01-01',
                         'language': c['language'] if 'language' in c else None,
                         'literature_type': c['literatureType'] if 'literatureType' in c else None,
                         'open_access': c['openAccess'] if 'openAccess' in c else None,
                         'peer_review': c['peerReview'] if 'peerReview' in c else None,
                         'publisher': c['publisher'].replace("'", "''") if 'publisher' in c else None,
                         'source': c['source'].replace("'", "''") if 'source' in c else None,
                         'title': c['title'].replace("'", "''") if 'title' in c else None,
                         'year': c['year'] if 'year' in c else None, 'month': c['month'] if 'month' in c else 0,
                         'countries_of_researcher': separator.join(c['countriesOfResearcher'])
                         if 'countriesOfResearcher' in c else None,
                         'topics': separator.join(c['topics']) if 'topics' in c else None,
                         'doi': get_doi(c),
                         'authors': parse_author_names(c),
                         'gbif_download_key': get_gbif_download_key(c)}

        # Adds this citation to the overall list
        all_citations[c['id']] = citation_dict

    triage_citations(all_citations)


def triage_citations(works):
    """
    Identify citations to be removed, added and updated on database
    """
    # Set of current citation haul IDs, to check against
    api_citation_ids = set(works.keys())

    # Ditto for ids currently in the database.
    existing_citations = db.query_db("SELECT id, update_date FROM gbif_citations;").fetchall()
    existing_citation_ids = set([c[0] for c in existing_citations])

    # Set of IDs which are in the database, but NOT in the api results any more. Need removing.
    citation_ids_to_delete = list(existing_citation_ids - api_citation_ids)

    # Set of IDs which are not in the database, but are in the api results. Need adding.
    citation_ids_to_add = list(api_citation_ids - existing_citation_ids)

    # Set of IDs that are in both payload and database. Some might need updating.
    common_citation_ids = existing_citation_ids & api_citation_ids

    # Identify updated records since last run and queue them for deletion and re-insertion
    for x in existing_citations:
        if x[0] in common_citation_ids:
            # get matching result from api payload
            api_record = works[x[0]]

            # compare modified and update_date
            # if modified is > updated, api result is newer
            if api_record['update_date'] > x[1]:
                citation_ids_to_delete.append(x[0])
                citation_ids_to_add.append(x[0])
            # if not, ignore
            else:
                continue

    # Remove redacted records and records in need of updating
    if len(citation_ids_to_delete) > 0:
        remove_citations(citation_ids_to_delete)

    # Get brand new + updated/to be re-added citation records from api payload
    if len(citation_ids_to_add) > 0:
        # Generate list containing new row values
        citations_to_add = []
        for citation_id in citation_ids_to_add:
            citations_to_add.append(works[citation_id])

        add_citations(citations_to_add)


def remove_citations(citation_ids):
    # Delete statement: generate and execute in a batch
    sql = "DELETE FROM gbif_citations where id = %s"

    # List of ids that to be deleted from gbif_citations and cascades across related tables
    params = citation_ids

    # Trigger delete operation. Cascades to related records in gbif_occurrences and gbif_bibliometrics
    db.update_db(sql, params)


def add_citations(new_citations):
    try:
        # adds/updates new/amended records
        for c in new_citations:
            insert_sql = f"INSERT INTO gbif_citations(abstract, authors, " \
                  f"countries_of_researcher, gbif_download_key," \
                  f"id, harvest_date, doi, language, literature_type, open_access," \
                  f"peer_review, publisher, source, title, topics, update_date, year, month, pub_date)" \
                  f"VALUES('{c['abstract']}','{c['authors']}'," \
                  f"'{c['countries_of_researcher']}', '{c['gbif_download_key']}', '{c['gid']}'," \
                  f"'{c['harvest_date']}', '{c['doi']}', '{c['language']}', '{c['literature_type']}'," \
                  f"'{c['open_access']}', '{c['peer_review']}', '{c['publisher']}', '{c['source']}'," \
                  f"'{c['title']}', '{c['topics']}', '{c['update_date'].strftime('%Y-%m-%d')}', {c['year']}, {c['month']}," \
                  f"'{c['pub_date']}');"

            sql = insert_sql.encode('ascii', 'ignore').decode('utf-8', 'ignore')
            db.query_db(sql)

    except pymysql.Error as e:
        print(sql)
        print(e)


def occurrences():
    """
    Triggers gbif_occurrences script to run for any new citation ids
    """
    try:
        for n in new_ids:
            go.assemble_parts(n)

    except pymysql.Error as e:
        print(n)
        print(e)


if __name__ == '__main__':
    get_list()
#    occurrences()
