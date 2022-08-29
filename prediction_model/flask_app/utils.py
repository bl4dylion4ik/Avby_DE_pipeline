import pandas as pd
import requests


def get_data_object_from_request(request) -> pd.DataFrame:
    """
    Process data from request for get DataFrame
    :param request:
    :return pd.DataFrame:
    """
    brand = request.args.get('brand')
    model = request.args.get('model')
    generation = request.args.get('generation')
    year = request.args.get('year')
    engine_capacity = request.args.get('engine_capacity')
    engine_type = request.args.get('engine_type')
    transmission_type = request.args.get('transmission_type')
    body_type = request.args.get('body_type')
    drive_type = request.args.get('drive_type')
    color = request.args.get('color')
    mileage_km = request.args.get('mileage_km')
    condition = request.args.get('condition')

    X = [brand, model, generation, year, engine_capacity,
         engine_type, transmission_type, body_type, drive_type,
         color, mileage_km, condition]

    columns = ['brand', 'model', 'generation', 'year', 'engine_capacity',
               'engine_type', 'transmission_type', 'body_type', 'drive_type',
               'color', 'mileage_km', 'condition']

    features = pd.DataFrame([X], columns=columns)

    features['year'] = features['year'].astype(int)
    features['engine_capacity'] = features['engine_capacity'].astype(float)
    features['mileage_km'] = features['mileage_km'].astype(int)
    return features


def get_data_object_from_json(request) -> pd.DataFrame:
    """
    Process json from api for get DataFrame
    :param request:
    :return pd.DataFrame:
    """
    x = request.get_json()[0]

    columns = ['brand', 'model', 'generation', 'year', 'engine_capacity',
               'engine_type', 'transmission_type', 'body_type', 'drive_type',
               'color', 'mileage_km', 'condition']

    features = pd.DataFrame.from_records(x, columns=columns, index=[0])
    features['year'] = features['year'].astype(int)
    features['engine_capacity'] = features['engine_capacity'].astype(float)
    features['mileage_km'] = features['mileage_km'].astype(int)
    return features


def get_brand_model_gen() -> dict:
    """
    Get dict from api for dropdown menu, like: {'Audi': {'A4': [C4, C5]}}
    :return dict:
    """
    BRAND_MODEL_GEN = {}
    BRAND_URL = 'https://api.av.by/offer-types/cars/catalog/brand-items'

    brand_response = requests.get(BRAND_URL).json()
    for brand in brand_response:
        MODEL_URL = 'https://api.av.by/offer-types/cars/catalog/brand-items/{}/models'.format(brand['id'])
        model_response = requests.get(MODEL_URL).json()
        brand_model = {}
        for model in model_response:
            GEN_URL = 'https://api.av.by/offer-types/cars/catalog/brand-items/{0}/models/{1}/generations'.\
                format(brand['id'], model['id'])

            gen_response = requests.get(GEN_URL).json()
            model_gen = []

            for gen in gen_response:
                model_gen.append(gen['name'])

            brand_model[model['name']] = model_gen
        BRAND_MODEL_GEN[brand['name']] = brand_model
    return BRAND_MODEL_GEN


def get_body_type() -> list:
    """
    Get list of body type from api for dropdown menu
    :return list:
    """
    body_type = []
    URL = 'https://api.av.by/offer-types/cars/catalog/body-types'
    response = requests.get(URL).json()
    for type in response:
        body_type.append(type['name'])
    return body_type


def get_engine_type() -> list:
    """
    Get list of engine type from api for dropdown menu
    :return list:
    """
    engine_type = []
    URL = 'https://api.av.by/offer-types/cars/catalog/engine-types'
    response = requests.get(URL).json()
    for type in response:
        engine_type.append(type['label'])
    return engine_type


def get_transmission_type() -> list:
    """
    Get list of transmission type from api for dropdown menu
    :return list:
    """
    transmission_type = []
    URL = 'https://api.av.by/offer-types/cars/catalog/transmission-types'
    response = requests.get(URL).json()
    for type in response:
        transmission_type.append(type['label'])
    return transmission_type