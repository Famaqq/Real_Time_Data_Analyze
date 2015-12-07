__author__ = 'jianxun'
# coding: UTF-8

import logging
from flask import Flask, jsonify, request, render_template
import json_wrapper
import data_center
import high_chart
import config

app = Flask(__name__)
app.config.from_object('config')
app.debug = True
__data_center = None
__task_center = None
logger = None
if __name__ == '__main__':
    __data_center = data_center.DataCenter()
    __task_center = __data_center.get_task_center()
    debug_logger = logging.getLogger(config.DEBUG_LOGGER_NAME)


@app.route('/home', methods=['GET'])
def hello():
    debug_logger.info(str(request))

    sub = request.values.get('sub')
    if sub is None:
        sub = "task.html"

    return render_template('index.html', sub=sub)


@app.route('/task.html', methods=['GET'])
def task_list():
    debug_logger.info(str(request))
    task_list = __task_center.get_tasks()

    if len(task_list) == 0:
        chart_content = high_chart.generate_chart_content(
            title='No Data',
            subtitle='No Data',
            chart_id='chart',
            data_list=[])
        return render_template('task.html', name=None, tasks={}, selected_task=None,
                           selected_function=None, chart=chart_content)

    tasks = {}
    for task in task_list:
        functions = []
        for i in xrange(1, len(task['functions']) + 1):
            functions.append(task['functions'][str(i)])
        task['functions'] = functions
        if len(task['title']) > 19:
            task['title'] = task['title'][:17] + '...'
        tasks[task['id']] = task

    name = request.values.get('name')
    if name is None:
        name = "unknown"
    selected_task = request.values.get('selected_task')
    if selected_task is None:
        selected_task = task_list[0]['id']
    else:
        selected_task = int(selected_task)

    selected_function = json_wrapper.encode(request.values.get('selected_function'), 'utf-8')
    if selected_function is not None:
        selected_function = int(selected_function)
    else:
        selected_function = 1

    result_data_type_id = tasks[selected_task]['result_data_type_id']
    chart_data = __data_center.query(result_data_type_id)
    result_data_type = __data_center.get_data_type(result_data_type_id)
    input_data_type_id = tasks[selected_task]['input_data_type_id']
    input_data_type = __data_center.get_data_type(input_data_type_id)
    # print(chart_data)
    chart_content = high_chart.generate_chart_content(
        title='Result Data Type: [' + str(result_data_type_id) + ']' + result_data_type['title'],
        subtitle='Input Data Type: [{id}]{title}'.format(
            id=input_data_type_id, title=input_data_type['title']),
        chart_id='chart',
        data_list=chart_data)
    return render_template('task.html', name=name, tasks=tasks, selected_task=selected_task,
                           selected_function=selected_function, chart=chart_content)


@app.route('/datatype.html', methods=['GET'])
def data_type_page():
    debug_logger.info(str(request))
    data_type_list = __data_center.get_data_type_list()

    if len(data_type_list) == 0:
        return render_template('data_type.html', data_types={}, selected=None)

    data_types = {}
    for data_type in data_type_list:
        data_types[data_type['id']] = data_type
    selected = request.values.get('selected')
    if selected is None:
        selected = data_type_list[0]['id']
    else:
        selected = int(selected)

    return render_template('data_type.html', data_types=data_types, selected=selected)


@app.route('/registerdatatype.html', methods=['GET'])
def get_data_type_register_form():
    return render_template('register_data_type.html')


@app.route('/registertask.html', methods=['GET'])
def get_task_register_form():
    return render_template('register_task.html')


@app.route('/basictypes', methods=['GET'])
def get_basic_types():
    basic_types = __data_center.get_basic_data_types()
    return jsonify({'types': basic_types})


@app.route('/datatypes', methods=['GET'])
def data_types():
    debug_logger.info(str(request))
    data_type_list = __data_center.get_data_type_list()

    data_types = {}
    for data_type in data_type_list:
        data_types[data_type['id']] = data_type
    return jsonify(data_types)


@app.route('/datatype', methods=['POST'])
def register_data_type():
    """register a data type
    request:
        {"desc": {"title": "Title", "attrs": {"userid": "string", "function": "string", "status": "integer", "cost": "real"}}}
    """
    try:
        data_type = json_wrapper.loads(request.values.get('desc'))
    except:
        response = jsonify({'result': 'fail', 'info': 'broken json'})
        return response
    result = __data_center.register_data_type(data_type)
    # deal response due to the result
    response = jsonify({'result': 'unhandled'})
    if result['succ']:
        response = jsonify({'result': 'successful', 'data_type_id': result['id']})
    else:
        if result['info'] == 'type_error':
            response = jsonify({'result': 'fail', 'info': 'type not accept: ' + str(result['attr'])})
        if result['info'] == 'db_error':
            response = jsonify({'result': 'fail', 'info': 'database error'})
        else:
            response = jsonify({'result': 'fail', 'info': result['info']})
    return response


@app.route('/task', methods=['POST'])
def register_task():
    """register a task
    request:
        {
            "desc": {
                    "input_data_type_id": 121, "time-range": "1m",
                    "functions": {
                        "1": {"name": "filter", "target": "status",
                            "conditions": [{"target": "cost", "operator": "bt", "param1": 100, "param2": 1000}, {}]},
                        "2": {"name": "average", "target": "cost", "tag": "avg_cost", "group_by": ["function", "status"]}
                    }
            }
        }
    """
    print(request.values.get('desc'))
    try:
        task_info = json_wrapper.loads(request.values.get('desc'))
    except:
        response = jsonify({'result': 'fail', 'info': 'broken json'})
        return response

    print(str(task_info))
    reg_result = __task_center.register_task(task_info)
    if reg_result['succ']:
        response = jsonify({'result': 'successful', 'task_id': reg_result['task_id']})
    else:
        response = jsonify({'result': 'fail', 'info': reg_result['info']})
    return response


@app.route('/data', methods=['POST'])
def put_data():
    """put a data
    request:
        {"data": {"data_type_id": 5243212,
                    "attrs": {"userid":123, "function": "GET/userinfo", "status": 200, "cost": 12.59},
                    "time" : "2015-09-01 10:50:39"
                }
        }
    """
    try:
        data = json_wrapper.loads(request.values.get('data'))
    except:
        response = jsonify({'result': 'fail', 'info': 'broken json'})
        return response

    result = __data_center.put_data(data)
    if result['succ']:
        response = jsonify({'result': 'successful'})
    else:
        response = jsonify({'result': 'fail', 'info': result['info']})
    return response


@app.route('/task', methods=['DELETE'])
def delete_task():
    """delete a task
    request:
        {
            "id": 23
        }
    """
    try:
        task_id = int(request.values.get('id'))
    except:
        response = jsonify({'result': 'fail', 'info': 'invalid id'})
        return response

    del_result = __task_center.delete_task(task_id)
    if del_result['succ']:
        response = jsonify({'result': 'successful'})
    else:
        response = jsonify({'result': 'fail', 'info': del_result['info']})
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=config.PORT_NUMBER)
