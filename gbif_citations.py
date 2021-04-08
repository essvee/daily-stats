import requests
import pymysql
import datetime
import gbif_dbtools as db
import gbif_occurrences as go
import itertools


def get_list():
    """
    Get list of NHM data citations from GBIF api
    """
    works = []

    # Get list of works that cited NHM specimens
    try:
        for offset in itertools.count(step=100):
            url = f"https://api.gbif.org/v1/literature/search?gbifDataSetKey=7e380070-f762-11e1-a439-00145eb45e9a" \
                  f"&limit=100&offset={offset}"
            r = requests.get(url)
            r.raise_for_status()
            results = r.json()['results']

            # Only keep results with an id
            works.extend([result for result in results if 'id' in result])

            if r.json()['endOfRecords']:
                break

    except requests.exceptions.HTTPError as e:
        print(e)

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
            full_name = ' '.join(n.values())
            namelist.append(full_name)
        return '; '.join(namelist)
    else:
        return None


def map_fields(works):
    """
    Flatten json and add default values where required
    :param works: list of dicts each containing info about a single NHM data citation
    """
    separator = '; '
    all_citations = {}

    for c in works:
        citation_dict = {'update_date': datetime.datetime.strptime(c['modified'][0:10], "%Y-%m-%d").date(),
                         'gid': c['id'],
                         'abstract': c['abstract'].replace("\n", "") if 'abstract' in c else None,
                         'harvest_date': c['discovered'] if 'discovered' in c else '1111-01-01',
                         'pub_date': c['published'][0:10] if 'published' in c else '1111-01-01',
                         'language': c['language'] if 'language' in c else None,
                         'literature_type': c['literatureType'] if 'literatureType' in c else None,
                         'open_access': c['openAccess'] if 'openAccess' in c else None,
                         'peer_review': c['peerReview'] if 'peerReview' in c else None,
                         'publisher': c['publisher'].replace("\n", "") if 'publisher' in c else None,
                         'source': c['source'].replace("\n", "") if 'source' in c else None,
                         'title': c['title'].replace("\n", "") if 'title' in c else None,
                         'year': c['year'] if 'year' in c else None, 'month': c['month'] if 'month' in c else 0,
                         'countries_of_researcher': separator.join(c['countriesOfResearcher'])
                         if 'countriesOfResearcher' in c else None,
                         'topics': separator.join(c['topics']) if ('topics' in c and len(c['topics']) > 0) else None,
                         'doi': get_doi(c),
                         'authors': parse_author_names(c),
                         'gbif_download_count': len(c['gbifDownloadKey']) if 'gbifDownloadKey' in c else None,
                         'nhm_record_count': None,
                         'total_record_count': None,
                         'total_dataset_count': None,
                         'gbif_dk_list': c['gbifDownloadKey'] if 'gbifDownloadKey' in c else None}

        # Adds this citation to the overall list
        all_citations[c['id']] = citation_dict

    triage_citations(all_citations)


def triage_citations(works):
    """
    Identify citations to be removed, added and updated on database
    """
    # Keys from current API call
    api_citation_ids = set(works.keys())
    # Keys and records on database
    existing_citations = db.query_db("SELECT id, update_date FROM gbif_citations;").fetchall()
    existing_citation_ids = set([c[0] for c in existing_citations])
    # Anything in the db but not the API needs deleting from the db
    citation_ids_to_delete = list(existing_citation_ids - api_citation_ids)
    # Anything in the api and not the db needs adding to the db
    citation_ids_to_add = list(api_citation_ids - existing_citation_ids)
    # Anything in both needs to be checked for latest update
    common_citation_ids = existing_citation_ids & api_citation_ids

    # Identify updated records since last run and queue them for deletion and re-insertion
    for x in existing_citations:
        if x[0] in common_citation_ids:
            # get matching result from api payload
            api_record = works[x[0]]

            # compare modified and update_date. If api result update > database update date, add to 'add' list
            # any duplicate keys will trigger an update
            if api_record['update_date'] > x[1]:
                citation_ids_to_add.append(x[0])

    # Get brand new + updated/to be re-added citation records from api payload
    if citation_ids_to_add:
        # Generate list containing new row values
        citations_to_add = [works[p] for p in citation_ids_to_add]
        add_citations(citations_to_add)

    # Remove redacted records and records in need of updating
    if citation_ids_to_delete:
        remove_citations(citation_ids_to_delete)


def remove_citations(citation_ids):
    # Delete statement: generate and execute in a batch
    sql = "DELETE FROM gbif_citations where id = %s"

    # List of ids to be deleted from gbif_citations
    params = citation_ids

    # Trigger delete operation. Cascades to related records in gbif_occurrences and gbif_bibliometrics
    db.update_db(sql, params)

    # check there's no orphaned rows left in gbf_bibliometrics: remove if any found
    gb_sql = "select * FROM gbif_bibliometrics gb WHERE gb.doi NOT IN (SELECT gc.doi FROM gbif_citations gc);"
    orphaned_bibliometrics = db.query_db(gb_sql).fetchall()

    if len(orphaned_bibliometrics) > 0:
        bib_sql = "DELETE FROM gbif_bibliometrics where id = %s"
        db.update_db(bib_sql, orphaned_bibliometrics)


def add_citations(new_citations):
    # structures to hold row_data for insertion
    row_data = []
    insert_query = "INSERT INTO gbif_citations(abstract, authors, countries_of_researcher, id, harvest_date, " \
                   "doi, language, literature_type, open_access, peer_review, publisher, source, title, " \
                   "topics, update_date, year, month, pub_date, total_dataset_count, total_record_count, " \
                   "nhm_record_count) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                   "%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE abstract = VALUES(abstract), " \
                   "authors = VALUES(authors), countries_of_researcher = VALUES(countries_of_researcher), " \
                   "harvest_date = VALUES(harvest_date), doi = VALUES(doi), language = VALUES(language), " \
                   "literature_type = VALUES(literature_type), open_access = VALUES(open_access), " \
                   "peer_review = VALUES(peer_review), publisher = VALUES(publisher), source = VALUES(source), " \
                   "title = VALUES(title), topics = VALUES(topics), update_date = VALUES(update_date), " \
                   "year = VALUES(year), month = VALUES(month), pub_date = VALUES(pub_date), " \
                   "total_dataset_count = VALUES(total_dataset_count), " \
                   "total_record_count = VALUES(total_record_count), nhm_record_count = VALUES(nhm_record_count)"

    # Call occurrence script here: should take new_citations and return a list of the same record with total
    # publisher count, total record count, nhm record count fields added and populated
    for record in new_citations:
        if record['gbif_dk_list']:
            record = go.assemble_parts(record)

        # add/update new/amended records
        row_data.append((record['abstract'], record['authors'], record['countries_of_researcher'], record['gid'],
                         record['harvest_date'], record['doi'], record['language'], record['literature_type'],
                         record['open_access'], record['peer_review'], record['publisher'], record['source'],
                         record['title'], record['topics'], record['update_date'].strftime('%Y-%m-%d'), record['year'],
                         record['month'], record['pub_date'], record['total_dataset_count'],
                         record['total_record_count'], record['nhm_record_count']))

    #    print(record['title'])

    try:
        db.update_db(insert_query, row_data)

    except pymysql.Error as e:
        print(e.args)


if __name__ == '__main__':
    get_list()
