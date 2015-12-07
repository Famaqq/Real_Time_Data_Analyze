__author__ = 'jianxun'
# coding: UTF-8

import Queue
import copy

import config
import task_dao
import task


class TaskCenter:
    __data_center = None

    __data_distribute_map = {}

    __task_list = {}

    def __init__(self, d_center):
        self.__data_center = d_center
        task_list = task_dao.query_task()
        for task_info in task_list:
            data_type_id = task_info['input_data_type_id']
            if data_type_id in self.__data_distribute_map:
                self.__data_distribute_map[data_type_id][task_info['id']] = Queue.Queue(config.MAX_QUEUE_SIZE_PER_TASK)
            else:
                self.__data_distribute_map[data_type_id] = {
                    task_info['id']: Queue.Queue(config.MAX_QUEUE_SIZE_PER_TASK)
                }
            t = task.Task(task_info, self.__data_distribute_map[data_type_id][task_info['id']], d_center)
            self.__task_list[task_info['id']] = t
            t.start()

    def put_data(self, data):
        data_type_id = data['data_type_id']
        if data_type_id in self.__data_distribute_map:
            for task_id in self.__data_distribute_map[data_type_id]:
                #print('put data to task ' + str(task_id))
                self.__data_distribute_map[data_type_id][task_id].put(copy.deepcopy(data))

    def register_task(self, task_info):
        # validate task info
        input_data_type_id = task_info['input_data_type_id']
        input_data_type = self.__data_center.get_data_type(input_data_type_id)
        if input_data_type is None:
            return {'succ': False, 'info': 'data type not exist.'}
        validate_result = task.validate(task_info, input_data_type['attrs'])
        if not validate_result['succ']:
            return validate_result
        result_type_register_result = self.__data_center.register_data_type({'title': task_info['title'],
                                                                             'attrs': validate_result['result_type']})
        if not result_type_register_result['succ']:
            return result_type_register_result
        task_info['result_data_type_id'] = result_type_register_result['id']
        task_id = task_dao.insert_task(task_info)
        task_info['id'] = int(task_id)

        if input_data_type_id in self.__data_distribute_map:
            self.__data_distribute_map[input_data_type_id][task_info['id']] = Queue.Queue(config.MAX_QUEUE_SIZE_PER_TASK)
        else:
            self.__data_distribute_map[input_data_type_id] = {
                task_info['id']: Queue.Queue(config.MAX_QUEUE_SIZE_PER_TASK)
            }
        t = task.Task(task_info, self.__data_distribute_map[input_data_type_id][task_info['id']], self.__data_center)
        self.__task_list[task_info['id']] = t
        t.start()
        return {"succ": True, "task_id": task_id}

    def delete_task(self, task_id):
        result = {'succ': True}
        task_info = self.get_task_by_id(task_id)
        data_type_id = task_info['input_data_type_id']
        if task_id in self.__task_list:
            t = self.__task_list.pop(task_id)
            t.stop()
            task_dao.delete_task(task_id)
        else:
            result['succ'] = False
            result['info'] = 'task id not exist'
            return result
        if data_type_id in self.__data_distribute_map and task_id in self.__data_distribute_map[data_type_id]:
            self.__data_distribute_map[data_type_id].pop(task_id)
        return result

    def get_tasks(self):
        return task_dao.query_task()

    def get_task_by_id(self, task_id):
        return task_dao.query_task_by_id(task_id)

    def __del__(self):
        for task_id in self.__task_list:
            self.__task_list[task_id].stop()
