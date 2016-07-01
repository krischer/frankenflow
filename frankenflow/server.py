import argparse
import glob
import os
import re

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


@app.route("/misfits")
def misfits():
    misfit_folder = app.flow_manager.info["output_folders"]["misfits"]
    files = glob.glob(os.path.join(misfit_folder, "*.txt"))

    misfits = []

    for filename in files:
        name = re.sub(r"\.txt$", "", os.path.basename(filename))
        with open(filename, "rt") as fh:
            misfit = float(fh.read())

        misfits.append({"model_name": name, "misfit": misfit})

    return flask.jsonify({"misfits": misfits})


@app.route("/reset/<job_id>")
def reset_job(job_id):
    app.flow_manager.reset_job(job_id)
    return ""


@app.route("/graph")
def graph():
    json_graph = app.flow_manager.graph.get_json()

    # Rewrite so it can be easily plotted with vis.js.
    json_graph["edges"] = []
    for edge in json_graph["links"]:
        json_graph["edges"].append({
            "from": json_graph["nodes"][edge["source"]]["id"],
            "to": json_graph["nodes"][edge["target"]]["id"],
            "arrows": "to"
        })

    # Move all the meta information to "_meta" and add a label to make it
    # easier to plot in the browser.
    new_nodes = []
    for node in json_graph["nodes"]:
        new_nodes.append({
            "id": node["id"],
            "label": node["id"].split("_")[1],
            "_meta": node
        })
    json_graph["nodes"] = new_nodes

    # Add colors depending on the status.
    for node in json_graph["nodes"]:
      if node["_meta"]["job_status"] == "success":
        node["color"] = "green"

        if node["label"] == "Start":
            node["color"] = "#D0A9F5"
        elif node["label"] == "Orchestrate":
            node["color"] = "white"

      elif node["_meta"]["job_status"] == "failed":
        node["color"] = "red"
      elif node["_meta"]["job_status"] == "running":
        node["color"] = "yellow"

      if node["label"] in ("Start", "Orchestrate"):
        node["shape"] = "box"

    return flask.jsonify(json_graph)


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
    parser = argparse.ArgumentParser(
        description="Launch the frankenflow server.")
    parser.add_argument("--port", default=12111,
                       help="web server port", type=int)
    parser.add_argument("--debug", action="store_true", help="debug mode")
    parser.add_argument("--open_to_outside", action="store_true",
                        help="access server from other computers")
    parser.add_argument("folder",
                        help="folder containing the frankenflow project")

    args = parser.parse_args()

    if not os.path.exists(args.folder):
        raise ValueError("Folder '%s' does not exist." % args.folder)

    if not os.path.isdir(args.folder):
        raise ValueError("'%s' is not a folder." % args.folder)

    fm = FlowManager(os.path.abspath(args.folder))
    serve(flow_manager=fm, port=args.port, debug=args.debug,
          open_to_outside=args.open_to_outside)


if __name__ == "__main__":
    __main__()
