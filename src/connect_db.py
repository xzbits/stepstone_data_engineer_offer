import configparser
import psycopg2


def config_parser(config_filepath, section):
    """
    Get required info dict

    :param config_filepath: Path points to config file
    :param section: section need to extract
    :return: Params dict
    """
    parser = configparser.ConfigParser()
    parser.read(config_filepath)

    result = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            result[param[0]] = param[1]
    else:
        raise Exception('{} does not have section {}'.format(config_filepath, section))

    return result


def get_db_connection(db_config_filepath, section):
    """
    Connecting and executing the database

    :param db_config_filepath: Path points to DB config file
    :param section: DB Section in config file
    :return: Database connection and cursor
    """
    db_config = config_parser(config_filepath=db_config_filepath, section=section)
    db_connection = psycopg2.connect(**db_config)
    db_cursor = db_connection.cursor()
    return db_connection, db_cursor
