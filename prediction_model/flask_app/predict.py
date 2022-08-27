from flask import Blueprint, request, jsonify
from catboost import CatBoostRegressor

import utils

predict = Blueprint("predict", __name__)


@predict.route('/predict', methods=['POST'])
def predict_price():
    x_request = utils.get_data_object_from_request(request)

    model = CatBoostRegressor()
    model.load_model('catboost_regression')
    pred = round(model.predict(x_request)[0], 1)

    return jsonify({
        "price": pred
    })
