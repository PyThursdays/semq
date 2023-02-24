from flask import Flask

from .api_hello import api_hello as hello

# Flask application instance
app = Flask(__name__)

app.register_blueprint(hello)
