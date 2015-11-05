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
    json_graph = app.flow_manager.graph.get_json()
    # Rewrite so it can be easily plotted with vis.js.
    json_graph["edges"] = []
    for edge in json_graph["links"]:
        json_graph["edges"].append({
            "from": json_graph["nodes"][edge["source"]]["id"],
            "to": json_graph["nodes"][edge["target"]]["id"],
        })


    json_graph = {
      "directed": True,
      "edges": [
        {
          "from": "151105T160957_ProjectModelTask_e0287748-7379-40f6-98c5-0f9cfc202530",
          "to": "151105T161205_PlotSpectralElementGrid_a39d6975-5a6c-4f36-b7f0-8a621bb059ad"
        }
      ],
      "graph": {},
      "links": [
        {
          "source": 2,
          "target": 1
        }
      ],
      "multigraph": False,
      "nodes": [
        {
          "id": "151105T160957_PlotRegularGridModel_fd9a8e54-57c1-4705-93ac-28078ac7218c",
          "label": "PlotRegularGridModel",
          "_meta": {
              "celery_task_id": "46220054-d9a6-48a3-a779-6e79725564f3",
              "inputs": {
                "model_folder": "/Users/lion/temp/flow_small/flow/__OPTIMIZATION/000_1_Model"
              },
              "job_status": "success",
              "priority": 1,
              "run_information": {
                "001_check_pre_staging": {
                  "end_time_stage": "2015-11-05 16:09:57.130099",
                  "runtime_stage": 0.00034499168395996094,
                  "start_time_stage": "2015-11-05 16:09:57.129772"
                },
                "002_stage_data": {
                  "end_time_stage": "2015-11-05 16:09:57.130175",
                  "runtime_stage": 3.790855407714844e-05,
                  "start_time_stage": "2015-11-05 16:09:57.130137"
                },
                "003_check_post_staging": {
                  "end_time_stage": "2015-11-05 16:09:57.130297",
                  "runtime_stage": 4.410743713378906e-05,
                  "start_time_stage": "2015-11-05 16:09:57.130253"
                },
                "004_run": {
                  "end_time_stage": "2015-11-05 16:11:18.849472",
                  "runtime_stage": 81.71914100646973,
                  "start_time_stage": "2015-11-05 16:09:57.130329"
                },
                "005_check_post_run": {
                  "end_time_stage": "2015-11-05 16:11:18.853511",
                  "runtime_stage": 0.003983020782470703,
                  "start_time_stage": "2015-11-05 16:11:18.849527"
                },
                "006_generate_next_steps": {
                  "end_time_stage": "2015-11-05 16:11:18.853638",
                  "runtime_stage": 3.886222839355469e-05,
                  "start_time_stage": "2015-11-05 16:11:18.853600"
                },
                "next_steps": None,
                "status": "success"
              },
              "stderr": "/Users/lion/temp/flow_small/flow/__JOBS/151105T160957_PlotRegularGridModel_fd9a8e54-57c1-4705-93ac-28078ac7218c/stderr",
              "stdout": "/Users/lion/temp/flow_small/flow/__JOBS/151105T160957_PlotRegularGridModel_fd9a8e54-57c1-4705-93ac-28078ac7218c/stdout",
              "task_type": "PlotRegularGridModel",
              "working_dir": "/Users/lion/temp/flow_small/flow/__JOBS/151105T160957_PlotRegularGridModel_fd9a8e54-57c1-4705-93ac-28078ac7218c"
          }
        },
        {
          "id": "151105T161205_PlotSpectralElementGrid_a39d6975-5a6c-4f36-b7f0-8a621bb059ad",
          "label": "PlotSpectralElementGrid",
          "_meta": {
              "celery_task_id": "17e5f871-bf00-4054-ba0e-598d24a3949c",
              "inputs": {
                "model_name": "000_1_Model"
              },
              "job_status": "running",
              "priority": 1,
              "stderr": "/Users/lion/temp/flow_small/flow/__JOBS/151105T161205_PlotSpectralElementGrid_a39d6975-5a6c-4f36-b7f0-8a621bb059ad/stderr",
              "stdout": "/Users/lion/temp/flow_small/flow/__JOBS/151105T161205_PlotSpectralElementGrid_a39d6975-5a6c-4f36-b7f0-8a621bb059ad/stdout",
              "task_type": "PlotSpectralElementGrid",
              "working_dir": "/Users/lion/temp/flow_small/flow/__JOBS/151105T161205_PlotSpectralElementGrid_a39d6975-5a6c-4f36-b7f0-8a621bb059ad"
          }
        },
        {
          "id": "151105T160957_ProjectModelTask_e0287748-7379-40f6-98c5-0f9cfc202530",
          "label": "ProjectModelTask",
          "_meta": {
              "celery_task_id": "2f37d2c2-28d3-4a87-a922-225f87203b8b",
              "inputs": {
                "model_folder": "/Users/lion/temp/flow_small/flow/__OPTIMIZATION/000_1_Model"
              },
              "job_status": "success",
              "priority": 0,
              "run_information": {
                "001_check_pre_staging": {
                  "end_time_stage": "2015-11-05 16:11:25.261982",
                  "runtime_stage": 0.0002620220184326172,
                  "start_time_stage": "2015-11-05 16:11:25.261722"
                },
                "002_stage_data": {
                  "end_time_stage": "2015-11-05 16:11:25.487358",
                  "runtime_stage": 0.22534394264221191,
                  "start_time_stage": "2015-11-05 16:11:25.262010"
                },
                "003_check_post_staging": {
                  "end_time_stage": "2015-11-05 16:11:25.487568",
                  "runtime_stage": 0.0001499652862548828,
                  "start_time_stage": "2015-11-05 16:11:25.487418"
                },
                "004_run": {
                  "end_time_stage": "2015-11-05 16:11:31.104602",
                  "runtime_stage": 5.617006063461304,
                  "start_time_stage": "2015-11-05 16:11:25.487594"
                },
                "005_check_post_run": {
                  "end_time_stage": "2015-11-05 16:11:31.104813",
                  "runtime_stage": 0.0001728534698486328,
                  "start_time_stage": "2015-11-05 16:11:31.104640"
                },
                "006_generate_next_steps": {
                  "end_time_stage": "2015-11-05 16:11:31.104869",
                  "runtime_stage": 3.2901763916015625e-05,
                  "start_time_stage": "2015-11-05 16:11:31.104837"
                },
                "next_steps": [
                  {
                    "inputs": {
                      "model_name": "000_1_Model"
                    },
                    "priority": 1,
                    "task_type": "PlotSpectralElementGrid"
                  }
                ],
                "status": "success"
              },
              "stderr": "/Users/lion/temp/flow_small/flow/__JOBS/151105T160957_ProjectModelTask_e0287748-7379-40f6-98c5-0f9cfc202530/stderr",
              "stdout": "/Users/lion/temp/flow_small/flow/__JOBS/151105T160957_ProjectModelTask_e0287748-7379-40f6-98c5-0f9cfc202530/stdout",
              "task_type": "ProjectModelTask",
              "working_dir": "/Users/lion/temp/flow_small/flow/__JOBS/151105T160957_ProjectModelTask_e0287748-7379-40f6-98c5-0f9cfc202530"
          }
        }
      ]
    }


    for node in json_graph["nodes"]:
      if node["_meta"]["job_status"] == "success":
        node["color"] = "green"
      elif node["_meta"]["job_status"] == "failure":
        node["color"] = "red"
      elif node["_meta"]["job_status"] == "running":
        node["color"] = "yellow"

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
    fm = FlowManager("/Users/lion/temp/flow_small/flow")
    serve(flow_manager=fm, port=12111, debug=True,
          open_to_outside=False)


if __name__ == "__main__":
    __main__()
