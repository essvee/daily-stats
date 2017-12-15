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
    contents = r.json()
    contents = contents.get('result').get('resources')
    results = {}

    for resource in contents:
        pkg_name = resource.get('pkg_name')
        record_count = resource.get('total')
        # Check if we've seen another resource under this package already:
        if pkg_name in results:
            # If yes, update overall count and exit
            results[pkg_name]['count'] = results[pkg_name]['count'] + record_count
        else:
            # Otherwise, get the title and identify resource type
            long_name = resource.get('pkg_title')
            if long_name == 'Collection specimens' or long_name == 'Index Lot collection' or long_name == 'Artefacts':
                pkg_type = 'collection records'
            else:
                pkg_type = 'research records'
            # Add to dict
            resource_dict = {'count': record_count, 'name': long_name, 'collection': pkg_type}
            results[pkg_name] = resource_dict

    write_records(results)


def write_records(results):
    # Get auth details + date
    f = open('server-permissions.txt', 'r')
    keys = f.read().splitlines()
    host = keys[0]
    user = keys[1]
    password = keys[2]
    database = keys[3]

    # Connect to database
    db = pymysql.connect(host=host, user=user, password=password, db=database)
    cursor = db.cursor()

    # Write update to package_comp
    try:
        for n in results:
            pkg_name = n.replace("'", "''")
            today_dt = datetime.datetime.today().date()
            record_count = results[n].get('count')
            pkg_type = results[n].get('collection')
            long_name = results[n].get('name').replace("'", "''").replace("\u2013", "")
            # Add new row and commit
            sql = "INSERT INTO package_comp(pkg_name, date, record_count, pkg_type, pkg_title, id) " \
                  "VALUES('%s', '%s', %s, '%s', '%s', null);" % (pkg_name, today_dt, record_count, pkg_type, long_name)
            cursor.execute(sql)
            db.commit()

    except pymysql.Error as e:
        print("MySQL Error: %s \nResource name: %s" % (e, long_name))
        print(sql)
        db.rollback()

    cursor.close()
    db.close()

count_records()



