var attr_num = 1;
var basic_types = null;

function init_basic_types() {
    var selected = null;
    $.get('/basictypes', function(result){
        basic_types = result.types;
        for (var btype in result.types) {
            $('#basic_type_1').append('<option value=' + result.types[btype] + '>' + result.types[btype] + '</option>');
        }
    });
}

function add_attr() {
    var new_id = attr_num + 1;
    var new_attr = $('<p>Name: <input id="name_' + new_id + '" size="60"> <select id="basic_type_' + new_id + '"></select><br></p>');
    new_attr.attr('id', 'attr_' + new_id);
    $('#attr_' + attr_num).after(new_attr);
    for (var btype in basic_types) {
        $('#basic_type_' + new_id).append('<option value=' + basic_types[btype] + '>' + basic_types[btype] + '</option>');
    }
    attr_num = attr_num + 1;
}

function del_attr() {
    if (attr_num > 1) {
        $('#attr_' + attr_num).remove();
        attr_num = attr_num - 1;
    }
}

function register_data_type() {
    var input_data = {}
    input_data['attrs'] = {}
    var title = $('#title').val()
    input_data['title'] = title;
    for (var i = 1; i <= attr_num; i++) {
        var name = $('#name_' + i).val()
        input_data['attrs'][name] = $('#basic_type_' + i).val();
    }

    $.ajax({
        url: '/datatype',
        type: 'POST',
        data: {desc: JSON.stringify(input_data)},
        success: function(result) {
            if (result.result == 'successful') {
                alert('Register successful!\nYour data type id is ' + result.data_type_id);
            } else {
                alert('Register failed.\n' + result.info);
            }
        }
    });
}
