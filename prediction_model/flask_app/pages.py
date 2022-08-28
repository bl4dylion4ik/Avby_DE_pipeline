from flask import Blueprint, render_template, request
import numpy as np

import utils

pages = Blueprint("pages", __name__)

BRAND_MODEL_GEN = utils.get_brand_model_gen()


@pages.route('/')
def index_page():

    class_entry_relations = BRAND_MODEL_GEN

    default_brand = list(BRAND_MODEL_GEN.keys())
    default_model = list(class_entry_relations[default_brand[0]].keys())
    default_gen = class_entry_relations[default_brand[0]][default_model[0]][0]
    default_body = utils.get_body_type()
    default_transmission = utils.get_transmission_type()
    default_engine = utils.get_engine_type()
    year = list(range(1930, 2022))
    condition = ['с пробегом', 'на запчасти', 'аварийный']
    drive_type = ['передний привод', 'постоянный полный привод', 'задний привод', 'подключаемый полный привод']
    color = ['чёрный', 'белый', 'серый', 'зелёный', 'серебристый', 'бордовый', 'синий',
             'коричневый', 'красный', 'жёлтый', 'фиолетовый', 'оранжевый', 'другой']
    engine_capacity = list(np.around(np.linspace(1, 7, 61), 1))
    return render_template('index.html',
                           all_classes=default_brand,
                           all_entries=default_model,
                           all_gen=default_gen,
                           year=year,
                           all_engine=default_engine,
                           all_transmission=default_transmission,
                           all_body=default_body,
                           drive_type=drive_type,
                           engine_capacity=engine_capacity,
                           color=color,
                           condition=condition
                           )


@pages.route('/about')
def about_page():
    return render_template('about.html')
