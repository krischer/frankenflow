var info;
var info_last_accessed;

var network;
var graph;


function update_status_on_page(data) {
    $("#status_status").text(data["status"]);
    $("#status_message").text(data["message"]);
};


function plot_graph() {
    var container = $("#graph_plot")[0];
    var options = {layout: {randomSeed: 2}};

    network = new vis.Network(container, graph, options);

    network.on("click", function(params) {
        var node = network.findNode(params.nodes[0])[0];
        var info = node.options._meta;
        $("#node_detail").JSONView(info, { collapsed: true });

    });
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