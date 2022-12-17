# DROP TABLES
stellenangebote_table_drop = "DROP TABLE IF EXISTS stellenangebote"
auftragsdetails_table_drop = "DROP TABLE IF EXISTS auftragsdetails"
zeit_table_drop = "DROP TABLE IF EXISTS zeit"
arbeitgeber_table_drop = "DROP TABLE IF EXISTS arbeitgeber"
arbeitort_table_drop = "DROP TABLE IF EXISTS arbeitort"

# Reference tables
angebotsart_table_drop = "DROP TABLE IF EXISTS angebotsart"
befristung_table_drop = "DROP TABLE IF EXISTS befristung"

# CREATE TABLES
stellenangebote_table_create = ("""
CREATE TABLE IF NOT EXISTS stellenangebote
(
    hashId TEXT PRIMARY KEY NOT NULL,
    refnr TEXT NOT NULL,
    beruf TEXT,
    titel TEXT,
    arbeitgeber TEXT,
    aktuelleVeroeffentlichungsdatum TEXT,
    eintrittsdatum TEXT,
    modifikationsTimestamp TIMESTAMP
)
""")

auftragsdetails_table_create = ("""
CREATE TABLE IF NOT EXISTS auftragsdetails
(
    refnr TEXT PRIMARY KEY NOT NULL,
    stellenbeschreibung TEXT,
    befristung INT,
    angebotsart INT
)
""")

arbeitort_table_create = ("""
CREATE TABLE IF NOT EXISTS arbeitort
(
    arbeitort_id SERIAL PRIMARY KEY NOT NULL,
    refnr TEXT NOT NULL,
    arbeitort_strasse TEXT,
    arbeitort_ort TEXT,
    arbeitort_plz TEXT,
    arbeitort_region TEXT,
    arbeitort_land TEXT
)
""")

zeit_table_create = ("""
CREATE TABLE IF NOT EXISTS zeit
(
    modifikationsTimestamp TIMESTAMP PRIMARY KEY NOT NULL, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
)
""")

arbeitgeber_table_create = ("""
CREATE TABLE IF NOT EXISTS arbeitgeber
(
    company_name TEXT,
    branchengruppe TEXT,
    website TEXT,
    uber_uns TEXT,
    betriebsgroesse TEXT,
    arbeitgeberort_strasse TEXT,
    arbeitgeberort_ort TEXT,
    arbeitgeberort_plz TEXT,
    arbeitgeberort_region TEXT,
    arbeitgeberort_land TEXT,
    PRIMARY KEY (company_name, branchengruppe)
)
""")

befristung_table_create = ("""
CREATE TABLE IF NOT EXISTS befristung
(
    id INT PRIMARY KEY NOT NULL,
    content TEXT
)
""")

angebotsart_table_create = ("""
CREATE TABLE IF NOT EXISTS angebotsart
(
    id INT PRIMARY KEY NOT NULL,
    content TEXT
)
""")


# INSERT tables
stellenangebote_table_insert = ("""
INSERT INTO stellenangebote
(
    hashId,
    refnr,
    beruf,
    titel,
    arbeitgeber,
    aktuelleVeroeffentlichungsdatum,
    eintrittsdatum,
    modifikationsTimestamp
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(hashId) DO NOTHING
""")

auftragsdetails_table_insert = ("""
INSERT INTO auftragsdetails
(
    refnr,
    stellenbeschreibung,
    befristung,
    angebotsart
)
VALUES (%s, %s, %s, %s)
ON CONFLICT(refnr) DO NOTHING
""")

zeit_table_insert = ("""
INSERT INTO zeit
(
    modifikationsTimestamp, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(modifikationsTimestamp) DO NOTHING
""")


arbeitort_table_insert = ("""
INSERT INTO arbeitort
(
    refnr,
    arbeitort_strasse,
    arbeitort_ort,
    arbeitort_plz,
    arbeitort_region,
    arbeitort_land
)
VALUES (%s, %s, %s, %s, %s, %s)
""")

arbeitgeber_table_insert = ("""
INSERT INTO arbeitgeber
(
    company_name,
    branchengruppe,
    website,
    uber_uns,
    betriebsgroesse,
    arbeitgeberort_strasse,
    arbeitgeberort_ort,
    arbeitgeberort_plz,
    arbeitgeberort_region,
    arbeitgeberort_land
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(company_name, branchengruppe) DO NOTHING
""")

befristung_table_insert = ("""
INSERT INTO befristung 
(
    id, 
    content
)
VALUES (%s, %s)
""")

angebotsart_table_insert = ("""
INSERT INTO angebotsart 
(
    id, 
    content
)
VALUES (%s, %s)
""")

# Queries list
drop_table_queries = [stellenangebote_table_drop,
                      auftragsdetails_table_drop,
                      zeit_table_drop,
                      arbeitort_table_drop,
                      arbeitgeber_table_drop,
                      angebotsart_table_drop,
                      befristung_table_drop]

create_table_queries = [stellenangebote_table_create,
                        auftragsdetails_table_create,
                        zeit_table_create,
                        arbeitort_table_create,
                        arbeitgeber_table_create,
                        angebotsart_table_create,
                        befristung_table_create]

insert_dict = {"insert_stellenangebote": stellenangebote_table_insert,
               "insert_zeit": zeit_table_insert,
               "insert_auftragsdetails": stellenangebote_table_insert,
               "insert_arbeitort": arbeitort_table_insert,
               "insert_arbeitgeber": arbeitgeber_table_insert,
               "insert_befristung": befristung_table_insert,
               "insert_angebotsart": angebotsart_table_insert}
