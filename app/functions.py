__author__ = 'jianxun'

from math import *
from collections import *
from datetime import datetime, timedelta
import numpy

time_range_interpreter = {
    'd': lambda x: timedelta(days=x),
    'h': lambda x: timedelta(hours=x),
    'm': lambda x: timedelta(minutes=x),
    's': lambda x: timedelta(seconds=x)
}

support_function_type = {
    'count': lambda x: Count(x),
    'average': lambda x: Average(x),
    'percentile': lambda x: Percentile(x),
    'top': lambda x: Top(x),
    'sum': lambda x: Sum(x),
    'standard_deviation': lambda x: StandardDeviation(x),
    'filter': lambda x: Filter(x),
    'collect': lambda x: Collect(x),
    'self_calc': lambda x: SelfCalc(x)
}

data_example = {
    'int': 123,
    'long': 10000000000000000000,
    'float': 123.45,
    'string': 'abc',
    'bool': True,
    'dict': {'a': 'b'},
    'list': [1, 2, 3],
    'set': {1, 2, 3}
}

data_type_tag = {
    type(1): 'int',
    type(10000000000000000000): 'long',
    type(1.1): 'float',
    type({'1': '2'}): 'dict',
    type([]): 'list',
    type(''): 'string',
    type(True): 'bool',
    type({1, 2, 3}): 'set'
}

validation_dict = {
    'count': lambda x, y: Count.validate(x, y),
    'average': lambda x, y: Average.validate(x, y),
    'filter': lambda x, y: Filter.validate(x, y),
    'collect': lambda x, y: Collect.validate(x, y),
    'percentage': lambda x, y: Percentile.validate(x, y),
    'top': lambda x, y: Top.validate(x, y),
    'sum': lambda x, y: Sum.validate(x, y),
    'standard_deviation': lambda x, y: StandardDeviation.validate(x, y),
    'self_calc': lambda x, y: SelfCalc.validate(x, y)
}


def get_time_range(time_range_str):
    unit = time_range_str[len(time_range_str) - 1]
    value = int(time_range_str[:-1])
    return time_range_interpreter[unit](value)


def validate(function_info, data_type):
    """ check if the data_type can be correctly calculated by the function

    :param function_info: function_info dict, e.g.
                {
                    'name': 'filter',
                    'target': 'status',
                    'conditions': [
                        {
                            'target': 'cost',
                            'operator': 'bt',
                            'param1': 100,
                            'param2': 1000
                        },
                        {}
                    ]
                }
    :param data_type: data_type dict, e.g.
                {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
    :return: the result dict, in which 'result' is a boolean indicate if it is validate,
             if validate, the 'result_type' would contain the output data_type dict of the function
             if not validate, the 'error_info' would contain some reason
    """
    result = {}
    if not isinstance(function_info, dict):
        result['succ'] = False
        result['info'] = 'function info should be dict.'
        return result
    if 'name' not in function_info:
        result['succ'] = False
        result['info'] = 'validation fail: function name not found.'
        return result
    return validation_dict[function_info['name']](function_info, data_type)


class Count:
    __distinct = None

    __dict = {}

    __current = 0

    __time_range = None

    __last_time = None

    __tag = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__time_range = get_time_range(info['time_range'])
        if info['distinct'] is not None:
            self.__distinct = info['distinct']
        self.__tag = info['tag']
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or the dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        if self.__last_time is None:
            self.__last_time = datetime.now()
        if self.__distinct is None or len(self.__distinct) == 0:
            self.__current += 1
        else:
            key = ''
            # use index to ensure the order do not change ?
            for i in xrange(0, len(self.__distinct)):
                key += str(data['attrs'][self.__distinct[i]])
            if key not in self.__dict:
                self.__dict[key] = 1
                self.__current += 1
            else:
                self.__dict[key] += 1
        return_value = None
        if datetime.now() - self.__last_time > self.__time_range:
            return_value = self.get_current()

        return return_value

    def get_current(self):
        """return the current result if exists

        :return: the dict represents the data
        """
        return_value = {'data_type_id': None, 'attrs': {self.__tag: self.__current}, 'time': datetime.now()}
        for key in self.__dict:
            return_value['attrs'][key] = self.__dict[key]
        self.__dict = {}
        self.__last_time = datetime.now()
        self.__current = 0
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for count

        :param info: the function dict, e.g.
                    {
                        'name': 'count',
                        'distinct': ['function','cost'],
                        'tag': 'count_function_cost',
                        'time_range': '1m'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['distinct', 'tag']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check tag
        if not isinstance(info['tag'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "tag" is not a string.'.format(
                func_name=info['name'])
            return result
        # check distinct
        if not (isinstance(info['distinct'], list) or info['distinct'] is None):
            result['succ'] = False
            result['info'] = \
                'validation fail in function "{func_name}": "distinct" is neither None or a list.'.format(
                    func_name=info['name'])
            return result
        if info['distinct'] is not None:
            for attr in info['distinct']:
                if attr not in data_type:
                    result['succ'] = False
                    result['info'] = \
                        'validation fail in function "{func_name}", distinct "{attr}": attribute not found.'.format(
                            func_name=info['name'], attr=attr)
                    return result
        # return the result type
        result['result_type'] = {info['tag']: 'int'}
        return result


class Average:
    __sum = 0

    __count = 0

    __time_range = None

    __last_time = None

    __tag = None

    __target = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__time_range = get_time_range(info['time_range'])
        self.__tag = info['tag']
        self.__target = info['target']
        self.__sum = 0.0
        self.__count = 0
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or a dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        if self.__last_time is None:
            self.__last_time = datetime.now()
        else:
            self.__count += 1
            self.__sum += data['attrs'][self.__target]
        return_value = None
        if datetime.now() - self.__last_time > self.__time_range:
            return_value = self.get_current()

        return return_value

    def get_current(self):
        """return the current result if exists

        :return: the dict represents the data
        """
        if self.__count == 0:
            self.__count = 1
        return_value = {'data_type_id': None, 'attrs': {self.__tag: self.__sum / self.__count}, 'time': datetime.now()}
        self.__last_time = datetime.now()
        self.__count = 0
        self.__sum = 0.0
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for calc

        :param info: the function dict, e.g.
                    {
                        'name': 'average',
                        'tag': 'avg_function_cost',
                        'target': 'cost',
                        'time_range': '1m'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['target', 'tag']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check tag
        if not isinstance(info['tag'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "tag" is not a string.'.format(
                func_name=info['name'])
            return result
        # check target
        if not isinstance(info['target'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not a string.'.format(
                func_name=info['name'])
            return result
        if info['target'] not in data_type:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}", "target" not in data.'.format(
                func_name=info['name'])
            return result
        if data_type[info['target']] not in ['int', 'float', 'long']:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not numeric.'.format(
                func_name=info['name'])
            return result
        # return the result type
        result['result_type'] = {info['tag']: 'float'}
        return result


class Percentile:
    __percentage = None

    __list = None

    __time_range = None

    __last_time = None

    __target = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__time_range = get_time_range(info['time_range'])
        self.__target = info['target']
        self.__list = []
        self.__percentage = info['percentage']
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or a dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        if self.__last_time is None:
            self.__last_time = datetime.now()
        else:
            self.__list.append(data['attrs'])
        return_value = None
        if datetime.now() - self.__last_time > self.__time_range:
            return_value = self.get_current()

        return return_value

    def get_current(self):
        """return the current result if exists

        :return: the dict represents the data
        """
        num = len(self.__list)
        if num == 0:
            return None
        else:
            self.__list.sort(key=lambda x: x[self.__target])
            val = self.__list[num * self.__percentage / 100]
        return_value = {'data_type_id': None, 'attrs': val, 'time': datetime.now()}
        self.__last_time = datetime.now()
        self.__list = []
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for calc

        :param info: the function dict, e.g.
                    {
                        'name': 'percentage',
                        'target': 'cost',
                        'percentage': 95,
                        'time_range': '1m'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['target', 'tag', 'percentage']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check target
        if not isinstance(info['target'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not a string.'.format(
                func_name=info['name'])
            return result
        if info['target'] not in data_type:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}", "target" not in data.'.format(
                func_name=info['name'])
            return result
        if data_type[info['target']] not in ['int', 'float', 'long']:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not numeric.'.format(
                func_name=info['name'])
            return result
        # check percentage
        if not isinstance(info['percentage'], int):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "percentage" is not int.'.format(
                func_name=info['name'])
            return result
        if not (0 < info['percentage'] < 100):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "percentage" not int (0, 100).'.format(
                func_name=info['name'])
            return result
        # return the result type
        result['result_type'] = data_type
        return result


class Top:
    __num = None

    __list = None

    __time_range = None

    __last_time = None

    __tag = None

    __target = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__time_range = get_time_range(info['time_range'])
        self.__tag = info['tag']
        self.__target = info['target']
        self.__list = []
        self.__num = info['num']
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or a dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        if self.__last_time is None:
            self.__last_time = datetime.now()
        else:
            self.__list.append(data['attrs'][self.__target])
        return_value = None
        if datetime.now() - self.__last_time > self.__time_range:
            return_value = self.get_current()

        return return_value

    def get_current(self):
        """return the current result if exists

        :return: the dict represents the data
        """
        num = len(self.__list)
        self.__list.sort(reverse=True)
        return_value = {'data_type_id': None, 'attrs': {}, 'time': datetime.now()}
        for i in xrange(1, self.__num + 1):
            if i < num:
                return_value['attrs'][self.__tag + '_' + str(i)] = self.__list[i - 1]
            else:
                return_value['attrs'][self.__tag + '_' + str(i)] = self.__list[num - 1]
        self.__last_time = datetime.now()
        self.__list = []
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for calc

        :param info: the function dict, e.g.
                    {
                        'name': 'top',
                        'tag': 'top_cost',
                        'target': 'cost',
                        'num': 3,
                        'time_range': '1m'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['target', 'tag', 'num']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check tag
        if not isinstance(info['tag'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "tag" is not a string.'.format(
                func_name=info['name'])
            return result
        # check target
        if not isinstance(info['target'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not a string.'.format(
                func_name=info['name'])
            return result
        if info['target'] not in data_type:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}", "target" not in data.'.format(
                func_name=info['name'])
            return result
        if data_type[info['target']] not in ['int', 'float', 'long']:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not numeric.'.format(
                func_name=info['name'])
            return result
        # check num
        if not isinstance(info['num'], int):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "num" is not int.'.format(
                func_name=info['name'])
            return result
        if info['num'] < 1:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "num" less than 1.'.format(
                func_name=info['name'])
            return result
        # return the result type
        result['result_type'] = {}
        for i in xrange(1, info['num'] + 1):
            result['result_type'][info['tag'] + '_' + str(i)] = data_type[info['target']]
        return result


class Sum:
    __sum = 0

    __time_range = None

    __last_time = None

    __tag = None

    __target = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__time_range = get_time_range(info['time_range'])
        self.__tag = info['tag']
        self.__target = info['target']
        self.__sum = 0
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or a dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        if self.__last_time is None:
            self.__last_time = datetime.now()
        else:
            self.__sum += data['attrs'][self.__target]
        return_value = None
        if datetime.now() - self.__last_time > self.__time_range:
            return_value = self.get_current()

        return return_value

    def get_current(self):
        """return the current result if exists

        :return: the dict represents the data
        """
        return_value = {'data_type_id': None, 'attrs': {self.__tag: self.__sum}, 'time': datetime.now()}
        self.__last_time = datetime.now()
        self.__sum = 0
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for calc

        :param info: the function dict, e.g.
                    {
                        'name': 'average',
                        'tag': 'avg_cost',
                        'target': 'cost',
                        'time_range': '1m'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['target', 'tag']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check tag
        if not isinstance(info['tag'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "tag" is not a string.'.format(
                func_name=info['name'])
            return result
        # check target
        if not isinstance(info['target'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not a string.'.format(
                func_name=info['name'])
            return result
        if info['target'] not in data_type:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}", "target" not in data.'.format(
                func_name=info['name'])
            return result
        if data_type[info['target']] not in ['int', 'float', 'long']:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not numeric.'.format(
                func_name=info['name'])
            return result
        # return the result type
        result['result_type'] = {info['tag']: data_type[info['target']]}
        return result


class StandardDeviation:
    __list = None

    __time_range = None

    __last_time = None

    __tag = None

    __target = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__time_range = get_time_range(info['time_range'])
        self.__tag = info['tag']
        self.__target = info['target']
        self.__list = []
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or a dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        if self.__last_time is None:
            self.__last_time = datetime.now()
        else:
            self.__list.append(data['attrs'][self.__target])
        return_value = None

        if datetime.now() - self.__last_time > self.__time_range:
            return_value = self.get_current()

        return return_value

    def get_current(self):
        """return the current result if exists

        :return: the dict represents the data
        """
        val = numpy.std(self.__list)
        return_value = {'data_type_id': None, 'attrs': {self.__tag: val}, 'time': datetime.now()}
        self.__last_time = datetime.now()
        self.__list = []
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for calc

        :param info: the function dict, e.g.
                    {
                        'name': 'standard_deviation',
                        'tag': 'sd_cost',
                        'target': 'cost',
                        'time_range': '1m'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['target', 'tag']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check tag
        if not isinstance(info['tag'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "tag" is not a string.'.format(
                func_name=info['name'])
            return result
        # check target
        if not isinstance(info['target'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not a string.'.format(
                func_name=info['name'])
            return result
        if info['target'] not in data_type:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}", "target" not in data.'.format(
                func_name=info['name'])
            return result
        if data_type[info['target']] not in ['int', 'float', 'long']:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "target" is not numeric.'.format(
                func_name=info['name'])
            return result
        # return the result type
        result['result_type'] = {info['tag']: 'float'}
        return result


class Filter:
    __attr_remain = None

    __condition = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__attr_remain = info['attr_remain']
        self.__condition = info['condition']
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or a dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        temp_expression = self.__condition
        for attr in data['attrs']:
            temp_expression = temp_expression.replace('{' + attr + '}', str(data['attrs'][attr]))
        try:
            passed = eval(temp_expression)
        except:
            passed = False
        return_value = None
        if passed:
            return_value = {'data_type_id': None, 'attrs': {}, 'time': datetime.now()}
            for attr in data['attrs']:
                if len(self.__attr_remain) == 0 or attr in self.__attr_remain:
                    return_value['attrs'][attr] = data['attrs'][attr]

        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for calc

        :param info: the function dict, e.g.
                    {
                        'name': 'filter',
                        'attr_remain': ['cost', 'function'],
                        'condition': '{function}=="add" and {cost}/10>30'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['attr_remain', 'condition']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check attr_remain
        if not (isinstance(info['attr_remain'], list) or info['attr_remain'] is None):
            result['succ'] = False
            result['info'] = \
                'validation fail in function "{func_name}": "attr_remain" is neither None or a list.'.format(
                    func_name=info['name'])
            return result
        if info['attr_remain'] is not None:
            for attr in info['attr_remain']:
                if attr not in data_type:
                    result['succ'] = False
                    result['info'] = \
                        'validation fail in function "{func_name}", attr_remain "{attr}": attribute not found.'.format(
                            func_name=info['name'], attr=attr)
                    return result
        # check condition
        if not isinstance(info['condition'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "condition" is not a string.'.format(
                func_name=info['name'])
            return result
        temp_expression = info['condition']
        for attr in data_type:
            print temp_expression
            temp_expression = temp_expression.replace('{' + attr + '}', str(data_example[data_type[attr]]))
            print temp_expression
        try:
            val = eval(temp_expression)
        except:
            val = None
        if not isinstance(val, bool):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": wrong "condition".'.format(
                func_name=info['name'])
            return result

        # return the result type
        result['result_type'] = {}
        for attr in info['attr_remain']:
            result['result_type'][attr] = data_type[attr]
        return result


class SelfCalc:
    __attr_remain = None

    __tag = None

    __expression = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__attr_remain = info['attr_remain']
        self.__expression = info['expression']
        self.__tag = info['tag']
        self.id = numpy.random.random()
        # print('init count:' + str(self.id))

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or a dict represents the data
        """
        # print('{id} calc'.format(id=self.id))
        temp_expression = self.__expression
        for attr in data['attrs']:
            temp_expression = temp_expression.replace('{' + attr + '}', str(data['attrs'][attr]))
        try:
            val = eval(temp_expression)
        except:
            val = None
        return_value = None
        if val is not None:
            return_value = {'data_type_id': None, 'attrs': {self.__tag: val}, 'time': datetime.now()}
        else:
            print temp_expression
            return None
        for attr in self.__attr_remain:
            return_value['attrs'][attr] = data['attrs'][attr]
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for calc

        :param info: the function dict, e.g.
                    {
                        'name': 'self_calc',
                        'attr_remain': ['cost', 'function'],
                        'expression': 'log({cost}, 2)*10',
                        'tag': 'log_cost'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['attr_remain', 'expression']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check attr_remain
        if not (isinstance(info['attr_remain'], list) or info['attr_remain'] is None):
            result['succ'] = False
            result['info'] = \
                'validation fail in function "{func_name}": "attr_remain" is neither None or a list.'.format(
                    func_name=info['name'])
            return result
        if info['attr_remain'] is not None:
            for attr in info['attr_remain']:
                if attr not in data_type:
                    result['succ'] = False
                    result['info'] = \
                        'validation fail in function "{func_name}", attr_remain "{attr}": attribute not found.'.format(
                            func_name=info['name'], attr=attr)
                    return result
        # check tag
        if not isinstance(info['tag'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "tag" is not a string.'.format(
                func_name=info['name'])
            return result
        # check expression
        if not isinstance(info['expression'], str):
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": "formula" is not a string.'.format(
                func_name=info['name'])
            return result
        temp_expression = info['expression']
        for attr in data_type:
            temp_expression = temp_expression.replace('{' + attr + '}', str(data_example[data_type[attr]]))
        try:
            val = eval(temp_expression)
        except:
            val = None
        if val is None:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": wrong "condition".'.format(
                func_name=info['name'])
            return result
        if type(val) not in data_type_tag:
            result['succ'] = False
            result['info'] = 'validation fail in function "{func_name}": unsupported result data type {t}'.format(
                func_name=info['name'], t=type(val))
            return result

        # return the result type
        result['result_type'] = {}
        for attr in info['attr_remain']:
            result['result_type'][attr] = data_type[attr]
        result['result_type'][info['tag']] = data_type_tag[type(val)]
        return result


class Collect:
    __dict = {}

    __time_range = None

    __last_time = None

    __target = None

    id = None

    def __init__(self, info):
        """Initialize the Function

        :param info: a dict that contains information about the function itself
        :return:
        """
        self.__time_range = get_time_range(info['time_range'])
        self.__target = info['target']
        self.__dict = {}
        for attr in self.__target:
            self.__dict[attr] = []
        self.id = numpy.random.random()

    def calc(self, data):
        """do calculating and returns the result

        :param data: a dict represents the input data
        :return: None if do not have a result yet, or the dict represents the data
        """
        if self.__last_time is None:
            self.__last_time = datetime.now()
        for attr in self.__target:
            self.__dict[attr].append(data['attrs'][attr])
        return_value = None
        if datetime.now() - self.__last_time > self.__time_range:
            return_value = self.get_current()

        return return_value

    def get_current(self):
        """return the current result if exists

        :return: the dict represents the data
        """
        return_value = {'data_type_id': None, 'attrs': {}, 'time': datetime.now()}
        for key in self.__dict:
            return_value['attrs'][key] = self.__dict[key]
            self.__dict[key] = []
        self.__last_time = datetime.now()
        print return_value
        return return_value

    @staticmethod
    def validate(info, data_type):
        """check if the data_type if suitable for count

        :param info: the function dict, e.g.
                    {
                        'name': 'collect',
                        'target': ['function','cost'],
                        'time_range': '1m'
                    }
        :param data_type: the data type dict, e.g.
                    {'userid': 'string', 'function': 'string', 'status': 'integer', 'cost': 'real'}
        :return: a dict contains `succ` and either `result_type` or `info`
                 `succ`: a boolean indicates if the data_type and the function dict is validate
                 `result_type`: if `succ` is True, it contains the result data type dict
                 `info`: if `succ` is False, it contains the error information string
        """
        result = {'succ': True}
        # check params existence
        for param in ['target']:
            if param not in info:
                result['succ'] = False
                result['info'] = 'validation fail in function "{func_name}": "{param}" not found'.format(
                    func_name=info['name'], param=param)
                return result
        # check target
        if not isinstance(info['target'], list):
            result['succ'] = False
            result['info'] = \
                'validation fail in function "{func_name}": "target" is not a list.'.format(
                    func_name=info['name'])
            return result
        for attr in info['target']:
            if attr not in data_type:
                result['succ'] = False
                result['info'] = \
                    'validation fail in function "{func_name}", target "{attr}": attribute not found.'.format(
                        func_name=info['name'], attr=attr)
                return result
        # return the result type
        result['result_type'] = {}
        for attr in info['target']:
            result['result_type'][attr] = 'list'
        return result
