var function_num = 1;
var data_types = null;

function init_data_types() {
    var selected = null;
    $.get('/datatypes', function(result){
        data_types = result;
        for (var key in result) {
            if (selected == null) {
                selected = key;
            }
            $('#input_data_type_id').append('<option value=' + key + '>' + key + '</option>');
        }
        $('#data_type_desc').text(JSON.stringify(data_types[selected]['attrs']));
    });

}

function on_data_type_select() {
    var selected = $('#input_data_type_id').val();
    $('#data_type_desc').text(JSON.stringify(data_types[selected]['attrs']));
}

function add_function() {
    var new_id = function_num + 1;
    var new_function = $('<p>Function ' + new_id + ':<br><textarea id="function_desc_' + new_id + '" name="function_desc_' + new_id + '" rows="10" cols="90"></textarea></p>');
    new_function.attr('id', 'function_' + new_id);
    $('#function_' + function_num).after(new_function);
    function_num = function_num + 1;
}

function del_function() {
    if (function_num > 1) {
        $('#function_' + function_num).remove();
        function_num = function_num - 1;
    }
}

function register_task() {
    var input_data = {}
    input_data['title'] = $('#title').val();
    input_data['input_data_type_id'] = parseInt($('#input_data_type_id').val());
    input_data['time_range'] = $('#time_range').val() + $('#time_range_pst').val();
    input_data['functions'] = {};
    for (var i = 1; i <= function_num; i++) {
        input_data['functions'][i.toString()] = jQuery.parseJSON($('#function_desc_' + i).val());
    }

    $.ajax({
        url: '/task',
        type: 'POST',
        data: {desc: JSON.stringify(input_data)},
        success: function(result) {
            if (result.result == 'successful') {
                alert('Register successful!\nYour task id is ' + result.task_id);
            } else {
                alert('Register failed.\n' + result.info);
            }
        }
    });
}
