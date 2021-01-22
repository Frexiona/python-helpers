def insert_to_mongodb(mongo_url, db_name, collection_name, json_obj):
    try:
        client = pymongo.MongoClient(mongo_url)
        db = client[db_name]
        col = db[collection_name]
        col.insert_many(json_obj)
    except Exception as error:
        logging.error(f"Error while calling insert_to_mongodb: {error}")
        logging.error(traceback.format_exc())


def get_from_mongodb(mongo_url, db_name, collection_name, options):
    try:
        client = pymongo.MongoClient(mongo_url)
        db = client[db_name]
        col = db[collection_name]
        col.find(options)
    except Exception as error:
        logging.error(f"Error while calling get_from_mongodb: {error}")
        logging.error(traceback.format_exc())


def get_df_from_csv(path):
    '''
    Import csv data as DataFrame
    :param path: Path of the csv file
    :return: pd.Dataframe
    '''
    return pd.read_csv(path)


def get_df_from_json(path, lines=True):
    '''
    Import JSON data as DataFrame
    :param path: Path of the JSON file
    :return: pd.Dataframe
    '''
    return pd.read_json(path, lines=lines)


def get_list_from_json(path):
    '''
    Import JSON data as list
    :param path: Path of the JSON file
    :return: list() || dict()
    '''
    with open(path) as json_file:
        data = json.load(json_file)

    return data


def list_to_json(object_list, export_file_name):
    with open(export_file_name, 'w') as outfile:
        json.dump(object_list, outfile)
