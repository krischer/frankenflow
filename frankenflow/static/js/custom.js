var info;
var info_last_accessed;

var network;
var graph;
var graph_type = "clustered";

var current_status;
var current_message;


function update_status_on_page(data) {
    // Update if nothing changed. If something changed, also update the graph.
    if (data.status != current_status || data.message != current_message) {
        current_status = data.status;
        current_message = data.message;

        $("#status_status").text(current_status);
        $("#status_message").text(current_message);
    }
};


function get_status_tooltip() {
    return "<div class='tip'>" +
        current_message.replace(/(?:\r\n|\r|\n)/g, '<br />') +
        "</div>";
}


function plot_graph() {
    var container = $("#graph_plot")[0];
    var options = {
        layout: {
            randomSeed: 2
        },
        physics: {
            barnesHut: {
                springLength: 140,
                avoidOverlap: 0.10,
                damping: 0.3
            },
        }
    };

    var this_graph = _.cloneDeep(graph);

    // Loop over all nodes and add a title which will show as a popup.
    _.forEach(this_graph.nodes, function(n) {
        var ri = n._meta.run_information;

        // Finished.
        if (ri && ri["001_check_pre_staging"] && ri["006_generate_next_steps"]) {

            var total_run_time =
                ri["001_check_pre_staging"].runtime_stage +
                ri["002_stage_data"].runtime_stage +
                ri["003_check_post_staging"].runtime_stage +
                ri["004_run"].runtime_stage +
                ri["005_check_post_run"].runtime_stage +
                ri["006_generate_next_steps"].runtime_stage;

            n["title"] = "Run time of task: " + total_run_time.toFixed(2) +
                " sec<br>" +
                "Finished " + moment(ri["006_generate_next_steps"].end_time_stage).fromNow();

            return
        }
    });

    // Update dropdown with list of jobs. Sort so the latest job is
    // preselected.
    var all_node_ids = _.pluck(this_graph.nodes, "id").sort().reverse();
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
                from: n._meta.current_goal,
                to: n.id,
                dashes: true
            });
        });

        // For each goal, add a goal node.
        var all_goals = _(this_graph.nodes).pluck("_meta.current_goal").unique().filter().value();
        _.forEach(all_goals, function(n) {
            this_graph.nodes.push({
                "id": n,
                "label": n,
                "color": "orange",
                "shape": "box"
            });
        })
    }

    network = new vis.Network(container, this_graph, options);

    network.on("click", function(params) {
        var node = network.findNode(params.nodes[0])[0];
        var info = node.options._meta;
        $("#node_detail").JSONView(info, {collapsed: true, nl2br: true});

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
    // Activate tooltips.
    $("#status_tooltip").tooltip({
        title: get_status_tooltip,
        placement: "bottom",
        html: true,
        container: 'body'
    });

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


$('#misfit_evolution_button').on('click', function() {
    $.ajax({
        url: '/misfits',
        success: function(data) {

            $("#misfit-table").empty().append(function() {
                var output = "<tr><th style='border:0px'>Model Name</th>" +
                    "<th style='border:0px'>Misfit</th></tr>";
                _.forEach(data.misfits, function(n) {
                    output += "<tr><td>" + n.model_name +
                        "</td><td>" + n.misfit + "</td></tr>";
                });
                return output
            });


            $('#misfit-modal').modal('show');
            console.log(data);
        }
    });
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