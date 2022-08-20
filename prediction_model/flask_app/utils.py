import pandas as pd


def get_data_object_from_request(request) -> pd.DataFrame:
    brand = request.form.get('brand')
    model = request.form.get('model')
    generation = request.form.get('generation')
    year = int(request.form.get('year'))
    transmission_type = request.form.get('transmission_type')
    engine_type = request.form.get('engine_type')
    engine_capacity = float(request.form.get('engine_capacity'))
    body_type = request.form.get('body_type')
    drive_type = request.form.get('drive_type')
    color = request.form.get('color')
    mileage_km = int(request.form.get('mileage_km'))
    condition = request.form.get('condition')

    X = [brand, model, generation, year, transmission_type,
         engine_type, engine_capacity, body_type, drive_type,
         color, mileage_km, condition]

    columns = ['brand', 'model', 'generation', 'year', 'transmission_type',
               'engine_type', 'engine_capacity', 'body_type', 'drive_type',
               'color', 'mileage_km', 'condition']

    features = pd.DataFrame([X], columns=columns)

    return features