from flask import Blueprint, request, jsonify

from semq.settings import SEMQ_DEFAULT_METASTORE_PATH
from semq.q import SimpleExternalQueue

# Create endpoint blueprint
api_discover = Blueprint(
    "discover",
    __name__,
    url_prefix="/discover"
)


# Register endpoints
@api_discover.route("/", methods=["GET"])
def discover():
    params = request.args.to_dict()
    metastore_path = params.get("metastore_path", SEMQ_DEFAULT_METASTORE_PATH)
    return jsonify(SimpleExternalQueue.discover(metastore_path=metastore_path))
