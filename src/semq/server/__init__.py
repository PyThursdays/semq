from flask import Flask

from .api_hello import api_hello as hello
from .api_discover import api_discover as discover
from .api_queue import api_queue as queue
# Flask application instance
app = Flask(__name__)

app.register_blueprint(hello)
app.register_blueprint(discover)
app.register_blueprint(queue)
