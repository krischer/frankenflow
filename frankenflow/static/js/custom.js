var info;
var info_last_accessed;

var network;
var graph;
var graph_type = "normal";


function update_status_on_page(data) {
    $("#status_status").text(data["status"]);
    $("#status_message").text(data["message"]);
};


function plot_graph() {
    var container = $("#graph_plot")[0];
    var options = {layout: {randomSeed: 2}};

    var this_graph = _.cloneDeep(graph);

    // Update dropdown with list of jobs.
    var all_node_ids = _.pluck(this_graph.nodes, "id");
    $("#job_selector").empty().append(function() {
        var output = "";
        _.forEach(all_node_ids, function(n) {
            output += "<option>" + n + "</option>";
        });
        return output
    });

    if (graph_type === "clustered") {
        // For each node, add a new edge.
        _.forEach(this_graph.nodes, function(n) {
            this_graph.edges.push({
                "from": n._meta.current_goal,
                "to": n.id
            });
        });

        // For each goal, add a goal node.
        var all_goals = _(this_graph.nodes).pluck("_meta.current_goal").unique().filter().value();
        _.forEach(all_goals, function(n) {
            this_graph.nodes.push({
                "id": n,
                "label": n,
                "color": "orange"
            });
        })
    }

    network = new vis.Network(container, this_graph, options);

    network.on("click", function(params) {
        var node = network.findNode(params.nodes[0])[0];
        var info = node.options._meta;
        $("#node_detail").JSONView(info, {collapsed: true});

    });
}


function toggle_graph() {
    if (graph_type === "normal") {
        graph_type = "clustered"
    }
    else {
        graph_type = "normal"
    }
    plot_graph();
}


function update_graph() {
    $.ajax({
        url: '/graph',
        success: function(data) {
            graph = data;
            plot_graph();
        }
    });
}


$(function() {

    (function update_status() {
        $.ajax({
            url: '/status',
            success: function(data) {
                $("#error_message").hide();
                info_last_accessed = new Date();
                update_status_on_page(data);
            },
            error: function(error_object) {
                $("#error_message").text(
                    "Error connecting to server: '" +
                    error_object.statusText + "' (Status code: " + error_object.status + ")").show();
            },
            complete: function() {
                // Poll every second.
                setTimeout(update_status, 1000);
            }
        });
    })();

    // Keep updating the last server contact time on the page.
    (function update_info_last_accessed() {
        if (info_last_accessed) {
            $("#last_contact_time").text(moment(info_last_accessed).fromNow());
        }
        else {
            $("#last_contact_time").text("??");
        }
        setTimeout(update_info_last_accessed, 500);
    })();
});


$('#update_graph_button').on('click', function() {
    update_graph();
});


$('#toggle_graph_button').on('click', function() {
    toggle_graph();
});


$('#reset_job_button').on('click', function() {
    var selected_node = $("#job_selector").find(":selected").text();
    $.ajax({
        url: '/reset/' + selected_node,
        success: function() {
            update_graph();
        }
    });
    $('#reset-job-modal').modal('hide');
});


$('#iterate_button').on('click', function() {
    $.ajax({
        url: '/iterate',
        success: function(data) {
            $("#error_message").hide();
            info_last_accessed = new Date();
            update_status_on_page(data);
        },
        error: function(error_object) {
            $("#error_message").text(
                "Error connecting to server: '" +
                error_object.statusText + "' (Status code: " + error_object.status + ")").show();
        }
    });
});