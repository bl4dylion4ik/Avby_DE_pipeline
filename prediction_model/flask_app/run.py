from flask import Flask
from pages import pages
from predict import predict


app = Flask(__name__)
app.register_blueprint(pages)
app.register_blueprint(predict)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')