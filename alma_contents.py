import datetime
import xmltodict
import gbif_dbtools as db
import pandas as pd
import requests


# Maps library names to special/modern collections - could be done better (via dict? easier to update in the future..)
def translate_library(row):
    if row['Column2'] == 'PAL-ARTHRO':
        row['library_code'] = 'modern-collections'
    elif 'MSS' in row['Column2'] or 'SC' in row['Column2'] or 'ART' in row['Column2']:
        row['library_code'] = 'special-collections'
    elif row['Column2'] in ['BOT-HENREY', 'BOT-CRYPSC', 'GEN-OWEN', 'TRI-ROTHSC']:
        row['library_code'] = 'special-collections'
    else:
        row['library_code'] = 'modern-collections'
    return row


def main():
    token = db.get_keys('alma-key.txt').pop()
    url = f"https://api-eu.hosted.exlibrisgroup.com/almaws/v1/analytics/reports?&path=/shared/" \
          f"Natural%20History%20Museum%20UK%20(NHM)/Reports/JTD/ItemCount&limit=1000" \
          f"&apikey={token}"

    # Query API and flatten result
    r = requests.get(url)
    doc = xmltodict.parse(r.text)

    # Navigate past all the headers etc to get to the row-level data
    row_data = doc['report']['QueryResult']['ResultXml']['rowset']['Row']
    mapped_row_data = [translate_library(b) for b in row_data]

    harvest_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    insert_parameters = []

    # Get values for each row
    for row in mapped_row_data:
        insert_parameters.append((row['Column1'], row['library_code'], harvest_date, int(row['Column3'])))
        print(f"{row['Column1']} | {row['library_code']} | {'harvest_date'} | {row['Column3']}")

    # Get summary for each type of collection/bib level combo (was 600 rows per day otherwise and unnecessary)
    df = pd.DataFrame(insert_parameters, columns=['bib_level', 'collection', 'harvest_date', 'record_count']) \
        .groupby(['bib_level', 'collection', 'harvest_date']).sum()
    df.reset_index(inplace=True)

    # Change the dataframe back to tuples
    insert_parameters = list(df.itertuples(index=False, name=None))

    sql = f"INSERT INTO alma_csf_package_comp (bib_level, collection, date, record_count) " \
          f"VALUES (%s, %s, %s, %s)"

    print(sql)

    cursor = db.update_db(sql, insert_parameters)
    cursor.close()


if __name__ == "__main__":
    main()
