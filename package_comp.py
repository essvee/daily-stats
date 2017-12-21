#!/usr/bin/env python
# coding=utf-8

import requests
import datetime
import pymysql


def count_records():
    #  Get the most recent figures from the portal api
    try:
        r = requests.get('http://data.nhm.ac.uk/api/3/action/dataset_statistics')
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e.message)

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
            pkg_type = 'collection records' if long_name in ['Collection specimens', 'Index Lot collection',
                                                             'Artefacts'] else 'research records'
            # Add to dict
            resource_dict = {'count': record_count, 'name': long_name, 'collection': pkg_type}
            results[pkg_name] = resource_dict

    write_records(results)


def write_records(results):
    # Get auth details + date
    with open('server-permissions.txt', 'r') as f:
        keys = f.read().splitlines()
        host, user, password, database = keys

    # Connect to database
    with pymysql.connect(host="xxxx", user="xxxx", password="xxxx", db="xxxx") as cursor:
        # Write update to package_comp
        for name, resource in results.items():
            pkg_name = name.replace("'", "''")
            today_dt = datetime.datetime.today().date()
            record_count = resource['count']
            pkg_type = resource['collection']
            long_name = str(resource['name']).replace("'", "''").replace("\u2013", "")
            # Add new row and commit
            sql = f"INSERT INTO package_comp(pkg_name, date, record_count, pkg_type, pkg_title, id) " \
                  f"VALUES('{pkg_name}', '{today_dt}', {record_count} , '{pkg_type}', '{long_name}', null);"
            try:
                cursor.execute(sql)
            except pymysql.Error as e:
                print("MySQL Error: %s \nResource name: %s" % (e, long_name))
                print(sql)
                cursor.rollback()


if __name__ == '__main__':
    count_records()
