import glob
import numpy as np
import os
import pandas as pd
import connect_db
from sql_queries import *


def try_except(func, default=None, except_err=(KeyError, )):
    try:
        return func()
    except except_err:
        return default


def process_job_offer(db_cursor, filepath):
    """
    Extracting, transforming, and loading data from job offers JSON files to Database

    :param db_cursor: Database cursor
    :param filepath: Path to job offer json files
    :return: None
    """
    # Open job offer json files
    df = pd.read_json(filepath, orient='records')
    df['ts'] = pd.to_datetime(df['modifikationsTimestamp']).values.astype(np.int64) // 10**9
    stellenangebote_ts = pd.to_datetime(df['ts'], unit='ms')

    # Insert job offer records
    stellenangebote_data = list(zip(list(df['hashId']),
                                    list(df['refnr']),
                                    list(df['beruf']),
                                    list(df['titel']),
                                    list(df['arbeitgeber']),
                                    list(df['aktuelleVeroeffentlichungsdatum']),
                                    list(df['eintrittsdatum']),
                                    list(pd.DatetimeIndex(stellenangebote_ts.values))))
    stellenangebote_labels = ['hashId', 'refnr', 'beruf', 'titel', 'arbeitgeber',
                              'aktuelleVeroeffentlichungsdatum', 'eintrittsdatum', 'ts']
    stellenangebote_df = pd.DataFrame(stellenangebote_data, columns=stellenangebote_labels)
    for i, row in stellenangebote_df.iterrows():
        db_cursor.execute(stellenangebote_table_insert, list(row))

    # Insert time records
    t = pd.to_datetime(df['ts'], unit='s')
    time_data = list(zip(list(pd.DatetimeIndex(t.values)),
                         list(pd.DatetimeIndex(t.values).hour),
                         list(pd.DatetimeIndex(t.values).day),
                         list(pd.DatetimeIndex(t.values).isocalendar().week),
                         list(pd.DatetimeIndex(t.values).month),
                         list(pd.DatetimeIndex(t.values).year),
                         list(pd.DatetimeIndex(t.values).weekday)))

    time_col_labels = ['modifikationsTimestamp', "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns=time_col_labels)

    for i, row in time_df.iterrows():
        db_cursor.execute(zeit_table_insert, list(row))


def process_job_details(db_cursor, filepath):
    """
    Extracting, transforming, and loading data from job details JSON files to Database

    :param db_cursor: Database cursor
    :param filepath: Path to job details json files
    :return: None
    """
    # Open log file
    df = pd.read_json(filepath, orient='records')

    # Insert job details records
    auftragsdetails_data = list(zip(list(df['refnr']),
                                    list(df['stellenbeschreibung']),
                                    list(df['branchengruppe']),
                                    list(df['befristung']),
                                    list(df['angebotsart'])))
    auftragsdetails_labels = ['refnr', 'stellenbeschreibung', 'branchengruppe', 'befristung', 'angebotsart']
    auftragsdetails_df = pd.DataFrame(auftragsdetails_data, columns=auftragsdetails_labels)
    for i, row in auftragsdetails_df.iterrows():
        if list(row)[0] is np.NaN:
            continue
        db_cursor.execute(auftragsdetails_table_insert, list(row))

    # Insert arbeit ort records
    for i in range(len(df)):
        one_job_detail = df.iloc[i]
        if one_job_detail['refnr'] is np.NaN:
            continue
        for one_add in one_job_detail['arbeitsorte']:
            arbeitort_data = (one_job_detail['refnr'],
                              try_except(lambda: one_add['strasse']),
                              try_except(lambda: one_add['ort']),
                              try_except(lambda: one_add['plz']),
                              try_except(lambda: one_add['region']),
                              try_except(lambda: one_add['land']))
            db_cursor.execute(arbeitort_table_insert, arbeitort_data)

    # Insert arbeitgeber records
    for i in range(len(df)):
        one_job_detail = df.iloc[i]
        if one_job_detail['refnr'] is np.NaN:
            continue
        arbeitgeber_data = (one_job_detail['arbeitgeber'],
                            one_job_detail['branchengruppe'],
                            one_job_detail['arbeitgeberdarstellungUrl'],
                            one_job_detail['arbeitgeberdarstellung'],
                            try_except(lambda: int(one_job_detail['betriebsgroesse']), except_err=ValueError),
                            try_except(lambda: one_job_detail['arbeitgeberAdresse']['strasse']),
                            try_except(lambda: one_job_detail['arbeitgeberAdresse']['ort']),
                            try_except(lambda: one_job_detail['arbeitgeberAdresse']['plz']),
                            try_except(lambda: one_job_detail['arbeitgeberAdresse']['region']),
                            try_except(lambda: one_job_detail['arbeitgeberAdresse']['land']))

        db_cursor.execute(arbeitgeber_table_insert, arbeitgeber_data)


def process_reference_tables(db_cursor, conn):
    # Insert reference tables
    befristung_data = [(1, 'BEFRISTET'), (2, 'UNBEFRISTET'), (3, 'UNKNOWN')]
    for one in befristung_data:
        db_cursor.execute(befristung_table_insert, one)

    angebotsart_data = [(1, 'ARBEIT'), (2, 'SELBSTAENDIGKEIT'),
                        (4, 'AUSBILDUNG/Duales Studium'), (34, 'Praktikum/Trainee')]
    for one in angebotsart_data:
        db_cursor.execute(angebotsart_table_insert, one)

    conn.commit()


def process_data(db_cursor, conn, filepath, func):
    """
    Finding all files and processing all files in filepath

    :param db_cursor: Database cursor
    :param conn: Database connection
    :param filepath: File path to job_offer and job_details files
    :param func: Function for ETL pipeline
    :return: None
    """
    # Get all files from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Number of files in directory
    num_files = len(all_files)

    # Iterating over files and process
    for i, datafile in enumerate(all_files, 1):
        print(datafile)
        func(db_cursor, datafile)
        conn.commit()
        print('{}/{} files processed'.format(i, num_files))


def process_etl():
    conn, cursor = connect_db.get_db_connection(db_config_filepath='db.cfg', section='stepstone')

    process_data(cursor, conn,
                 filepath=r'job_offer_data',
                 func=process_job_offer)

    process_data(cursor, conn,
                 filepath=r'job_detail_data',
                 func=process_job_details)

    process_reference_tables(cursor, conn)


if __name__ == "__main__":
    process_etl()
