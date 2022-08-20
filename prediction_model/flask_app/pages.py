from flask import Blueprint, render_template, request


pages = Blueprint("pages", __name__)


@pages.route('/')
def index_page():
    return render_template('index.html')


@pages.route('/about')
def about_page():
    return render_template('about.html')
