import requests
import pymysql
import gbif_dbtools as db


def update_publishers():
    """
    Looks at the GBIF Registry endpoint and adds any new publishers to local table
    """
    pub_keys = get_publishers()
    try:
        count = requests.get("http://api.gbif.org/v1/organization?limit=0").json()['count']
        offset = 0
        while count > offset:
            results = requests.get(f"http://api.gbif.org/v1/organization?limit=200&offset={offset}").json()['results']
            for p in results:
                # If org is new, add the name, key and country to the table
                if p['key'] not in pub_keys:
                    publisher_name = p['title'].replace("\"", "\'").encode('ascii', 'ignore').decode('utf-8', 'ignore')
                    publisher_country = p['country'] if 'country' in p else None
                    db.query_db(f"""INSERT INTO gbif_publishers VALUES ("{p['key']}", "{publisher_name}",
                    "{publisher_country}")""")
                    print(p['key'])
            # Increment offset
            offset += 200

    except requests.exceptions.HTTPError as e:
        print(e)


def get_publishers():
    """
    Retrieves keys of existing publishers from local table
    :return: Set<string> of publisher keys
    """
    cursor = db.query_db(f"SELECT publisher_key FROM gbif_publishers;")
    pub_keys = set()
    try:
        for (publisher_key) in cursor:
            pub_keys.add(publisher_key[0])
        return pub_keys
    except pymysql.Error as e:
        print(e)


if __name__ == '__main__':
    update_publishers()
