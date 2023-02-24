import json
from flask import Blueprint, request, jsonify

from semq.settings import SEMQ_DEFAULT_METASTORE_PATH
from semq.q import SimpleExternalQueue

# Create endpoint blueprint
api_queue = Blueprint(
    "queue",
    __name__,
    url_prefix="/queue"
)


# Register endpoints
@api_queue.route("/get", methods=["GET"])
def get():
    params = request.args.to_dict()
    # Extract params
    metastore_path = params.get("metastore_path", SEMQ_DEFAULT_METASTORE_PATH)
    wait_seconds = int(params.get("wait_seconds", -1))
    name = params.get("name")
    # Validate params
    if not name:
        raise ValueError("Queue name needed!")
    # Create queue instance
    queue = SimpleExternalQueue(
        name=name,
        metastore_path=metastore_path
    )
    return jsonify(queue.get(wait_seconds=wait_seconds))


@api_queue.route("/put", methods=["GET"])
def put():
    params = request.args.to_dict()
    # Extract params
    metastore_path = params.get("metastore_path", SEMQ_DEFAULT_METASTORE_PATH)
    item = params.get("item")
    name = params.get("name")
    # Validate params
    if not name or not item:
        raise ValueError("Queue `name` and `item` are required!")
    # Create queue instance
    queue = SimpleExternalQueue(
        name=name,
        metastore_path=metastore_path
    )
    return jsonify(queue.put(item=item, item_hashing=True))
