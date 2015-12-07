__author__ = 'jianxun'

import threading
import time
from datetime import datetime, timedelta
import Queue

import re
import functions
import config


class Task(threading.Thread):
    __id = None

    __data_center = None

    __data_list = None

    __result_data_type_id = None

    __functions = None

    __time_range = None

    __stop_flag = None

    def __init__(self, task_info, data_list, data_center):
        """Init the Task

        :param task_info: a dict that contains information about the task itself
        :param data_list: a Queue.Queue from which the task can get new data
        :param data_center: an instance of DataCenter to which the task can put the result
        :return:
        """
        threading.Thread.__init__(self)
        #print('init task: ' + str(task_info))
        self.__id = task_info['id']
        self.__data_center = data_center
        self.__data_list = data_list
        assert isinstance(self.__data_list, Queue.Queue)
        self.__result_data_type_id = task_info['result_data_type_id']
        self.__time_range = get_time_range(task_info['time_range'])
        self.__functions = {}
        for i in task_info['functions']:
            func_name = task_info['functions'][i]['name']
            task_info['functions'][i]['time_range'] = task_info['time_range']
            if func_name in functions.support_function_type:
                func = functions.support_function_type[func_name](task_info['functions'][i])
                self.__functions[str(i)] = func
        self.__stop_flag = False

    def run(self):
        """fetch data and do calculating & data publish. if no data to fetch, sleep 0.5s

        """
        #print('task ' + str(self.__id) + ' started with function ' + str(self.__functions['1'].id))
        while not self.__stop_flag:
            if self.__data_list.empty():
                time.sleep(0.5)
            else:
                #print('task ' + str(self.__id) + ' get data, function ' + str(self.__functions['1'].id))
                data = self.__data_list.get()
                for i in range(0, len(self.__functions)):
                    func = self.__functions[str(i + 1)]
                    data = func.calc(data)
                    if data is None:
                        break
                if data is not None:
                    self.__publish(data)

    def __publish(self, data):
        """publish the `data` to data center

        :param data: the dict represent the data
        """
        new_data = {'attrs': data['attrs'], 'data_type_id': self.__result_data_type_id,
                    'time': datetime.now().strftime(config.TIME_FORMAT)}
        #print('task {id} publish data: '.format(id=self.__id) + str(new_data))
        result = self.__data_center.put_data(new_data)
        if not result['succ']:
            print('in function ' + str(self.__id) + ': ' + str(result))

    def stop(self):
        self.__stop_flag = True


def validate(task_info, data_type):
    """ check if the data_type can be correctly calculated in the task

    :param task_info: task_info dict, e.g.
                {
                    'input_data_type_id': 121,
                    'time-range': '1m',
                    'functions': {
                        '1': {
                            'name': 'filter',
                            'target': 'status',
                            'conditions': [
                                {
                                    'target': 'cost', 'operator': 'bt', 'param1': 100, 'param2': 1000
                                }, {...}
                            ]
                        },
                        '2': {
                            'name': 'average',
                            'target': 'cost',
                            'tag': 'avg_cost',
                            'group_by': ['function', 'status']
                        }
                    }
                }
    :param data_type: data_type dict, e.g.
                {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
    :return: the result dict, in which 'result' is a boolean indicate if it is validate, if not, the 'info' would
            contain some information of the reason
    """
    result = {
        'succ': True
    }
    if not isinstance(task_info, dict):
        result['succ'] = False
        result['info'] = 'task desc should be dict'
        return result
    # check title
    if len(task_info['title']) == 0:
        result['succ'] = False
        result['info'] = 'title is empty.'
        return result
    # check basic attributes
    for key in task_structure:
        if key not in task_info:
            result['succ'] = False
            result['info'] = 'key not found: {key}'.format(key=key)
            return result
        if not task_structure[key](task_info[key]):
            result['succ'] = False
            result['info'] = 'type error: {key}'.format(key=key)
            return result
    # check time range
    pattern = re.compile("\A[1-9][0-9]{0,2}[dhms]\Z")
    if not pattern.match(task_info['time_range']):
        result['succ'] = False
        result['info'] = 'time range format not valid'.format(
            func_name=task_info['name'])
        return result
    # check each function
    temp_type = data_type
    for i in xrange(0, len(task_info['functions'])):
        if str(i + 1) not in task_info['functions']:
            result['succ'] = False
            result['info'] = 'function key error: expected {key}'.format(key=i + 1)
            return result
        temp_result = functions.validate(task_info['functions'][str(i + 1)], temp_type)
        if temp_result['succ']:
            temp_type = temp_result['result_type']
        else:
            result['succ'] = False
            result['info'] = temp_result['info']
    result['result_type'] = temp_type
    return result


def get_time_range(time_range_str):
    unit = time_range_str[len(time_range_str) - 1]
    value = int(time_range_str[:-1])
    return time_range_interpreter[unit](value)


time_range_interpreter = {
    'd': lambda x: timedelta(days=x),
    'h': lambda x: timedelta(hours=x),
    'm': lambda x: timedelta(minutes=x),
    's': lambda x: timedelta(seconds=x)
}

task_structure = {
    'input_data_type_id': lambda x: isinstance(x, int),
    'time_range': lambda x: x is None or isinstance(x, str),
    'functions': lambda x: isinstance(x, dict),
    'title': lambda x: isinstance(x, str)
}
