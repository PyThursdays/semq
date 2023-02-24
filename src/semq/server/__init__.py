from flask import Flask

from .api_hello import api_hello as hello
from .api_discover import api_discover as discover
# Flask application instance
app = Flask(__name__)

app.register_blueprint(hello)
app.register_blueprint(discover)
