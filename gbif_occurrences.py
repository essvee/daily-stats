import requests
import pymysql
import datetime


def get_keys():
    # Get gbif download keys from citations table
    with open('local_details.txt', 'r') as f:
        keys = f.read().splitlines()
        host, user, password, database = keys

    # Connect to database and grab keys from known citations:
    with pymysql.connect(host=host, user=user, password=password, db=database) as cursor:
        sql = f"SELECT id, gbif_download_key FROM gbif_citations;"

        try:
            cursor.execute(sql)
            for n in cursor.fetchall():
                downloads = {}
                # Get keys and split if multiple values present
                gk_list = n[1].split("; ")
                # For add each key to a dict with its corresponding id
                for gk in gk_list:
                    if gk is not "":
                        downloads['gbif_download_key'] = gk
                        downloads['gid'] = n[0]
                        # Add to occurrences dict
                        occurrences[gk] = downloads

        except pymysql.Error as e:
            print(sql)
            print(e)

    count_datasets(occurrences)


def count_datasets(temp_occurrences):
    for d in temp_occurrences:
        # For each gkey, insert into api url, parse results and get 'count'
        try:
            r = requests.get(f"http://api.gbif.org/v1/occurrence/download/{d}/datasets?limit=10")
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(e)

        dataset_count = r.json()['count']
        occurrences[d]['dataset_count'] = dataset_count


occurrences = {}
if __name__ == '__main__':
    get_keys()
