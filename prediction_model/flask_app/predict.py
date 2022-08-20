from flask import Blueprint, render_template, request, jsonify


predict = Blueprint("predict", __name__)


@predict.route('/predict', methods=['POST'])
def predict_price():
    pass