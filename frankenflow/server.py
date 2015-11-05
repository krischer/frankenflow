import os

import flask

from frankenflow.flow_manager import FlowManager

STATIC_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                "static"))

app = flask.Flask(__name__, static_folder=STATIC_DIRECTORY)


@app.route("/")
def index():
    filename = os.path.join(STATIC_DIRECTORY, "index.html")
    with open(filename, "rt") as fh:
        data = fh.read()
    return data


@app.route("/info")
def info():
    return flask.jsonify(app.flow_manager.info)


@app.route("/iterate")
def iterate():
    app.flow_manager.iterate()
    # Also return the status after the iteration.
    return status()


@app.route("/status")
def status():
    return flask.jsonify(app.flow_manager.current_status)


@app.route("/graph")
def graph():
    return flask.jsonify(app.flow_manager.graph.get_json())


def serve(flow_manager, port=12111, debug=False, open_to_outside=False):
    """
    Start the server.

    :param flow_manager: The flow manager instance for the server.
    :param comm: LASIF communicator instance.
    :param port: The port to launch on.
    :param debug: Debug on/off.
    :param open_to_outside: By default it only serves on localhost thus the
        server cannot be accessed from other PCs. Set this to True to enable
        access from other computers.
    """
    if open_to_outside is True:
        host = "0.0.0.0"
    else:
        host = None

    app.flow_manager = flow_manager
    app.run(port=port, debug=debug, host=host)


def __main__():
    fm = FlowManager("/Users/lion/temp/flow_small/flow")
    serve(flow_manager=fm, port=12111, debug=True,
          open_to_outside=False)


if __name__ == "__main__":
    __main__()
