from flask import Blueprint, request, jsonify

from .utils import validate_queue_attributes, cleanup_wrapper


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
    wait_seconds = int(params.get("wait_seconds", -1))
    # Create queue instance
    queue = validate_queue_attributes(**params)
    return jsonify(queue.get(wait_seconds=wait_seconds))


@api_queue.route("/put", methods=["GET"])
def put():
    params = request.args.to_dict()
    # Extract params
    item = params.get("item")
    # Create queue instance
    queue = validate_queue_attributes(**params)
    return jsonify(queue.put(item=item, item_hashing=True))


@api_queue.route("/size", methods=["GET"])
def size():
    params = request.args.to_dict()
    # Create queue instance
    queue = validate_queue_attributes(**params)
    return jsonify(queue.size(
        include_items=True
    ))


@api_queue.route("/cleanup", methods=["GET"])
def cleanup():
    params = request.args.to_dict()
    return cleanup_wrapper(
        everything=False,
        **params
    )


@api_queue.route("/cleanup-everything", methods=["GET"])
def cleanup_everything():
    params = request.args.to_dict()
    return cleanup_wrapper(
        everything=True,
        **params
    )
