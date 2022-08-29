from flask import Blueprint, request, jsonify
from catboost import CatBoostRegressor

import utils
import pages

predict = Blueprint("predict", __name__)


@predict.route('/dropdown_menu_1')
def dropdown_menu_1():
    selected_brand = request.args.get('brand', type=str)

    # get values for the second dropdown
    updated_values = list(pages.BRAND_MODEL_GEN[selected_brand].keys())

    # create the value sin the dropdown as a html string
    html_string_selected = ''
    for entry in updated_values:
        html_string_selected += '<option value="{}">{}</option>'.format(entry, entry)

    return jsonify(html_string_selected=html_string_selected)


@predict.route('/dropdown_menu_2')
def dropdown_menu_2():
    selected_brand = request.args.get('brand', type=str)
    selected_model = request.args.get('model', type=str)
    # get values for the second dropdown
    updated_values = list(pages.BRAND_MODEL_GEN[selected_brand][selected_model])

    # create the value sin the dropdown as a html string
    html_string_selected = ''
    for entry in updated_values:
        html_string_selected += '<option value="{}">{}</option>'.format(entry, entry)

    return jsonify(html_string_selected=html_string_selected)


@predict.route('/predict')
def predict_price():
    x_request = utils.get_data_object_from_request(request)

    model = CatBoostRegressor()
    model.load_model('catboost_regression')
    pred = round(model.predict(x_request)[0], 0)

    return jsonify({
        "price": pred
    })


@predict.route('/api/predict')
def predict_price():
    x_request = utils.get_data_object_from_request(request)

    model = CatBoostRegressor()
    model.load_model('catboost_regression')
    pred = round(model.predict(x_request)[0], 0)

    return jsonify({
        "price": pred
    })