var info;
var info_last_accessed;

function update_status_on_page(data) {
    $("#status_status").text(data["status"]);
    $("#status_message").text(data["message"]);
};

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

    (function update_graph() {
        $.ajax({
            url: '/graph',
            success: function(data) {
                $("#graph").text(JSON.stringify(data, null, 2));
            },
            error: function(error_object) {
            },
            complete: function() {
                // Poll every second.
                setTimeout(update_graph, 1000);
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

    $.ajax({
        url: '/graph',
        success: function(data) {
            console.log(data);
            container = $("#graph_plot")[0];

            var network = new vis.Network(container, data, {});

            network.on("click", function (params) {
                var node = network.findNode(params.nodes[0])[0];
                var info = node.options._meta;
                $("#node_detail").text(JSON.stringify(info, null, 2));
            });

        }
    });


});


$('#iterate_button').on('click', function () {
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