import datetime
from requests import HTTPError
import gbif_dbtools as db
import requests
import time


def get_citations():
    doi_sql = "SELECT DISTINCT(doi) from gbif_citations WHERE doi != 'None';"
    insert_sql = "INSERT INTO gbif_bibliometrics (doi, times_cited, field_citation_ratio, relative_citation_ratio, harvest_date) " \
                "VALUES (%s, %s, %s, %s, %s)"

    # Get list of DOIs
    cursor = db.query_db(doi_sql)
    today_dt = datetime.datetime.today().date()
    query_list = []

    # For all DOIs, get citation count
    for d in cursor.fetchall():
        # Throttle query rate to comply with API terms of use
        time.sleep(1)
        url = f"https://metrics-api.dimensions.ai/doi/{d[0]}"
        print(url)
        try:
            r = requests.get(url)
            r.raise_for_status()
            query_list.append((f'{d[0]}', r.json()['times_cited'] or 0,
                               r.json()['field_citation_ratio'] or 0, r.json()['relative_citation_ratio'] or 0,
                               today_dt))
        # Skip over DOIs which aren't found
        except HTTPError:
            print(d[0])
            continue

    # Insert
    db.update_db(insert_sql, query_list)


if __name__ == '__main__':
    get_citations()
