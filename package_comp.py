import requests
import datetime
import gbif_dbtools as db


def count_records():
    #  Get the most recent figures from the portal api
    try:
        r = requests.get('http://data.nhm.ac.uk/api/3/action/dataset_statistics')
        r.raise_for_status()

        # Get record detail and group according to pkg_name - record count should be sum of all resources in a package
        contents = r.json()['result']['resources']

        results = {}

        for resource in contents:
            pkg_name = resource['pkg_name']
            record_count = resource['total']
            # Check if we've seen another resource under this package already:
            if pkg_name in results:
                # If yes, update overall count and exit
                results[pkg_name]['count'] = results[pkg_name]['count'] + record_count
            else:
                # Otherwise, get the title and identify resource type
                long_name = resource['pkg_title']
                pkg_type = 'collection records' if long_name in ['Collection specimens', 'Index Lot collection', 'Artefacts'] else 'research records'
                # Add to dict
                resource_dict = {'count': record_count, 'name': long_name, 'collection': pkg_type}
                results[pkg_name] = resource_dict

        write_records(results)

    except requests.exceptions.HTTPError as e:
        print(e.response)


def write_records(results):
    query_sql = "INSERT INTO package_comp (pkg_name, date, record_count, pkg_type, pkg_title) " \
                "VALUES (%s, %s, %s , %s, %s);"
    query_list = []

    # Check we have results, just in case
    if len(results) > 0:
        today_dt = datetime.datetime.today().date().strftime("%Y-%m-%d")
        for name, resource in results.items():
            pkg_name = name.replace("'", "''")
            record_count = resource['count']
            pkg_type = resource['collection']
            long_name = str(resource['name']).replace("'", "''").replace("\u2013", "")
            query_list.append((pkg_name, today_dt, record_count, pkg_type, long_name))

    db.update_db(query_sql, query_list)


if __name__ == '__main__':
    count_records()
