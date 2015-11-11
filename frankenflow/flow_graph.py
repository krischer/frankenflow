import datetime
import json
import os
import uuid

import networkx as nx
import networkx.readwrite.json_graph

from . import NoJobsLeft


class FlowGraph():
    def __init__(self, filename):
        self.filename = filename
        if os.path.exists(self.filename):
            self.deserialize()
        else:
            self.graph = nx.DiGraph()

    def serialize(self):
        with open(self.filename, "wt") as fh:
            json.dump(self.get_json(), fh)

    def deserialize(self):
        with open(self.filename, "rt") as fh:
            self.graph = networkx.readwrite.json_graph.node_link_graph(
                json.load(fh))

    def get_json(self):
        return networkx.readwrite.json_graph.node_link_data(self.graph)

    def add_job(self, task_type, inputs, priority=0, from_node=None):
        now = datetime.datetime.now()
        graph_id = now.strftime("%y%m%dT%H%M%S_") + task_type + "_" + str(
            uuid.uuid4())
        self.graph.add_node(n=graph_id, attr_dict={
            "task_type": task_type,
            "inputs": inputs,
            "priority": priority,
            "job_status": "not started"
        })

        if from_node:
            print("Adding edge from", from_node, "to", graph_id)
            self.graph.add_edge(from_node, graph_id)

        return graph_id, self[graph_id]

    def get_current_or_next_job(self):
        """
        Get the current or next job.
        """
        # Find all jobs that have no outwards pointing edges.
        out_nodes = [i for i in self.graph.nodes_iter() if
                     self.graph.out_degree(i) == 0]
        if not out_nodes:
            raise NoJobsLeft

        # Find the one that has status == running
        running_nodes = [i for i in out_nodes
                         if self.graph.node[i]["job_status"] == "running"]
        assert len(running_nodes) <= 1, "Only one job can be active at any " \
                                        "given time."
        # One running node.
        if running_nodes:
            return running_nodes[0]

        # Make sure non of the out_nodes has a success state.
        out_nodes = [i for i in out_nodes
                     if self.graph.node[i]["job_status"] != "success"]

        # Now pick the job with the highest priority
        out_nodes = sorted(out_nodes,
                           key=lambda x: self.graph.node[x]["priority"],
                           reverse=True)

        if not out_nodes:
            raise NoJobsLeft

        return out_nodes[0]

    def __getitem__(self, item):
        return self.graph.node[item]

    def __setitem__(self, item, value):
        self.graph.node[item] = value

    def __len__(self):
        return len(self.graph)