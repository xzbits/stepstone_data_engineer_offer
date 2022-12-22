from pyspark.sql.functions import col, to_timestamp, when, explode, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear
from pyspark.sql import SparkSession
from sql_queries import *
import connect_db
import os


def create_spark_session():
    """
    Create Spark session
    :return: SparkSession object
    """
    spark = SparkSession.builder\
        .appName("Enriching StepStone database with Data Engineer offers")\
        .config('spark.jars', r'C:\Program Files\Java\jdbc-42.5.1-jar\postgresql-42.5.1.jar')\
        .getOrCreate()
    return spark


def load_df_to_db(spark_df, table_name, db_config):
    """
    Load data from spark df to postgres db

    :param spark_df: Table dataframe
    :param table_name: Table name in DB
    :param db_config: Database configuration
    :return: None
    """
    # Insert job offer data into postgres DB
    url = 'jdbc:postgresql://{}:5432/{}?user={}&password={}'.format(db_config['host'],
                                                                    db_config['dbname'],
                                                                    db_config['user'],
                                                                    db_config['password'])
    properties = {"driver": "org.postgresql.Driver"}

    spark_df.write.jdbc(url=url, table=table_name, mode='overwrite', properties=properties)


def process_job_offer(spark_obj, input_data, db_config):
    """
    Ingesting Job Offer log files and loading into postgres db

    :param spark_obj: Spark session object
    :param input_data: Path to job offers json log files
    :param db_config: Database configuration
    :return: None
    """
    # Get file path to job offers files
    job_offer_path = os.path.join(input_data, 'job_offer_page_*.json')

    # Read json job offer log files
    job_offer_data = spark_obj.read.json(job_offer_path)
    job_offer_data = job_offer_data\
        .withColumn("parsed_modifikationsTimestamp",
                    when(to_timestamp(col('modifikationsTimestamp'), "yyyy-MM-dd'T'HH:mm:ss.SSS").isNotNull(),
                         to_timestamp(col('modifikationsTimestamp'), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
                    .when(to_timestamp(col('modifikationsTimestamp'), "yyyy-MM-dd'T'HH:mm:ss").isNotNull(),
                          to_timestamp(col('modifikationsTimestamp'), "yyyy-MM-dd'T'HH:mm:ss")))

    # Get job offer records
    stellenangebote_data = job_offer_data\
        .select(['hashId',
                 'refnr',
                 'beruf',
                 'titel',
                 'arbeitgeber',
                 'aktuelleVeroeffentlichungsdatum',
                 'eintrittsdatum',
                 'modifikationsTimestamp',
                 'parsed_modifikationsTimestamp'])\
        .distinct()\
        .where(col('refnr').isNotNull())\
        .dropDuplicates(['refnr'])

    # Load job offer data into postgres DB
    load_df_to_db(stellenangebote_data, 'stellenangebote', db_config)

    # Get time records
    time_data = job_offer_data.select(['parsed_modifikationsTimestamp',
                                       hour('parsed_modifikationsTimestamp').alias('hour'),
                                       dayofmonth('parsed_modifikationsTimestamp').alias('date'),
                                       weekofyear('parsed_modifikationsTimestamp').alias('week'),
                                       month('parsed_modifikationsTimestamp').alias('month'),
                                       year('parsed_modifikationsTimestamp').alias('year'),
                                       dayofweek('parsed_modifikationsTimestamp').alias('weekday')]).dropDuplicates()

    # Load time data into postgres db
    load_df_to_db(time_data, 'zeit', db_config)


def process_job_details(spark_obj, input_data, db_config):
    """
    Ingesting Job detail log files and loading into staging tables

    :param spark_obj: Spark session object
    :param input_data: Path to job detail json log files
    :param db_config: Database configuration
    :return: None
    """
    # Get file path to job detail files
    job_detail_path = os.path.join(input_data, 'job_detail_page_*.json')

    # Read job detail json files
    job_detail_data = spark_obj.read.json(job_detail_path)

    # Get job detail records
    auftragsdetails_data = job_detail_data\
        .select(['refnr',
                 'stellenbeschreibung',
                 'branchengruppe',
                 'befristung',
                 'angebotsart'])\
        .distinct()\
        .where(col('refnr').isNotNull())\
        .dropDuplicates(['refnr'])

    # Load job detail data into postgres db
    load_df_to_db(auftragsdetails_data, 'auftragsdetails', db_config)

    # Get company info records
    arbeitgeber_data = job_detail_data\
        .select(['arbeitgeber',
                 'branchengruppe',
                 'arbeitgeberdarstellungUrl',
                 'arbeitgeberdarstellung',
                 'betriebsgroesse',
                 'arbeitgeberAdresse.adresszusatz',
                 'arbeitgeberAdresse.strasse',
                 'arbeitgeberAdresse.ortsteil',
                 'arbeitgeberAdresse.ort',
                 'arbeitgeberAdresse.plz',
                 'arbeitgeberAdresse.region',
                 'arbeitgeberAdresse.land'])\
        .where(col('arbeitgeber').isNotNull())\
        .where(col('branchengruppe').isNotNull())\
        .dropDuplicates(['arbeitgeber', 'branchengruppe'])

    # Load company info data into postgres db
    load_df_to_db(arbeitgeber_data, 'arbeitgeber', db_config)

    # Get workplace address records
    arbeitort_data = job_detail_data\
        .select(job_detail_data.refnr, explode(job_detail_data.arbeitsorte).alias('arbeitsorte'))\
        .select(['refnr',
                 'arbeitsorte.koordinaten.lat',
                 'arbeitsorte.koordinaten.lon',
                 'arbeitsorte.adresszusatz',
                 'arbeitsorte.strasse',
                 'arbeitsorte.ortsteil',
                 'arbeitsorte.ort',
                 'arbeitsorte.plz',
                 'arbeitsorte.region',
                 'arbeitsorte.land'])\
        .withColumn('arbeitort_id', monotonically_increasing_id())

    # Load workplace address data into postgres db
    load_df_to_db(arbeitort_data, 'arbeitort', db_config)


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


def process_etl():
    # Setup parameters
    stepstone_db_config = connect_db.config_parser(config_filepath='db.cfg', section='stepstone')
    conn, cursor = connect_db.get_db_connection(db_config_filepath='db.cfg', section='stepstone')
    spark = create_spark_session()

    process_job_offer(spark, 'job_offer_data', stepstone_db_config)
    process_job_details(spark, 'job_detail_data', stepstone_db_config)
    process_reference_tables(cursor, conn)


if __name__ == "__main__":
    process_etl()
