__author__ = 'jianxun'
# coding: UTF-8


import Queue
import threading
import time
import logging

import config
import task_center
import data_type_dao
import data_dao


class DataCenter:
    __data_types = {}

    __data_pool = None

    __incoming_data_pool = None

    __task_center = None

    __worker = None

    __logger = logging.getLogger(config.DEBUG_LOGGER_NAME)

    """Init the Data Center
    Args:
        data_info: a list of definition of data types registered by users
                sample: [{'id': 123, 'attr': ['function', 'status'], 'value_name': 'cost', 'value_type': 'real'}]
        task_info: a list of definition of tasks registered by users
                sample: [{'id': 10, 'data_id': 123, func: 'avg', 'time_range': '1m', 'tag': 'avg_cost',
                    'attr_remain': ['function', 'status'], 'group_by': ['function', 'status']}]
    """

    def __init__(self):
        data_type_list = data_type_dao.query_data_type()
        for data_type in data_type_list:
            data_id = data_type['id']
            self.__data_types[data_id] = data_type
        self.__data_pool = Queue.Queue(config.MAX_QUEUE_SIZE)
        self.__task_center = task_center.TaskCenter(self)
        self.__worker = DataWorker(self.__data_pool, self.__task_center)
        self.__worker.start()

    def put_data(self, data):
        verify_result = self.__verify(data)
        if verify_result['succ']:
            self.__data_pool.put(data)
        return verify_result

    def register_data_type(self, data_type):
        # check title
        if 'title' not in data_type:
            return {'succ': False, 'info': 'title not found.'}
        if not isinstance(data_type['title'], str):
            return {'succ': False, 'info': 'title is not string.'}
        if len(data_type['title']) == 0:
            return {'succ': False, 'info': 'title is empty.'}
        # check attributes
        for attr in data_type['attrs']:
            if len(attr) == 0:
                return {'succ': False, 'info': 'attr name is empty.'}
            if data_type['attrs'][attr] not in supported_data_type:
                logger = logging.getLogger(config.DEBUG_LOGGER_NAME)
                logger.info(
                    'attributes not accept: {name} : {type}'.format(name=attr, type=data_type['attrs'][attr]))
                return {'succ': False, 'info': 'type_error', 'attr': str(attr)}
        data_id = data_type_dao.insert_data_type(data_type)
        if isinstance(data_id, long):
            self.__data_types[data_id] = data_type
            return {'succ': True, 'id': data_id}
        logger = logging.getLogger(config.DEBUG_LOGGER_NAME)
        logger.info('register data type fail with db response: {resp}'.format(resp=data_id))
        return {'succ': False, 'info': 'db_error'}

    def query(self, data_type_id, start_time='2015-01-01 00:00:00', end_time='2020-12-31 23:59:59'):
        if data_type_id in self.__data_types:
            return data_dao.query_data(data_type_id, start_time, end_time)
        else:
            return []

    def __verify(self, data):
        result = {'succ': True}
        if not isinstance(data, dict):
            result['succ'] = False
            result['info'] = 'data should be a dict'
            return result
        for key in data_structure:
            if key not in data:
                result['succ'] = False
                result['info'] = 'key not found: {key}'.format(key=key)
                return result
            if not data_structure[key](data[key]):
                result['succ'] = False
                result['info'] = 'format error: {key}'.format(key=key)
                return result
        type_id = data['data_type_id']
        if type_id not in self.__data_types:
            result['succ'] = False
            result['info'] = 'data_type_id not found: {id}'.format(id=type_id)
            return result
        data_type = self.__data_types[type_id]
        for attr in data_type['attrs']:
            if attr not in data['attrs']:
                result['succ'] = False
                result['info'] = 'attr not found: {attr}'.format(attr=attr)
                return result
            if not supported_data_type[data_type['attrs'][attr]](data['attrs'][attr]):
                result['succ'] = False
                result['info'] = 'attr wrong with "{attr}", expect {extype} but got {rltype}' \
                    .format(attr=attr, extype=data_type['attrs'][attr], rltype=type(data['attrs'][attr]))
                return result
        return result

    def get_task_center(self):
        return self.__task_center

    def get_data_type(self, data_type_id):
        if data_type_id is None or int(data_type_id) not in self.__data_types:
            return None
        return self.__data_types[int(data_type_id)]

    def get_data_type_list(self):
        return data_type_dao.query_data_type()

    def get_basic_data_types(self):
        return [x for x in supported_data_type]

    def __del__(self):
        self.__worker.stop()


class DataWorker(threading.Thread):
    __data_pool = None

    __task_center = None

    __stop_flag = False

    def __init__(self, data_pool, t_center):
        threading.Thread.__init__(self)
        self.__data_pool = data_pool
        self.__task_center = t_center
        self.__stop_flag = False

    def run(self):
        while not self.__stop_flag:
            if self.__data_pool.empty():
                time.sleep(0.5)
            else:
                data = self.__data_pool.get()
                data_dao.insert_data(data)
                self.__task_center.put_data(data)

    def stop(self):
        self.__stop_flag = True


supported_data_type = {
    'int': lambda x: isinstance(x, int),
    'long': lambda x: isinstance(x, long),
    'float': lambda x: isinstance(x, float),
    'string': lambda x: isinstance(x, str),
    'bool': lambda x: isinstance(x, bool),
    'list': lambda x: isinstance(x, list),
    'dict': lambda x: isinstance(x, dict),
    'set': lambda x: isinstance(x, set)
}


def valid_time(x):
    try:
        time.strptime(x, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

data_structure = {
    'data_type_id': lambda x: isinstance(x, int) or isinstance(x, long),
    'attrs': lambda x: isinstance(x, dict),
    'time': lambda x: isinstance(x, str) and valid_time(x)
}
