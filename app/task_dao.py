__author__ = 'jianxun'

import db_config
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
import json_wrapper

engine = create_engine('mysql://%s:%s@%s:%s/%s?charset=utf8' % (db_config.USERNAME,
                                                                db_config.PASSWORD,
                                                                db_config.HOST,
                                                                db_config.PORT,
                                                                db_config.DB),
                       encoding="utf-8", pool_recycle=7200, echo=False)

Base = declarative_base()
Session = sessionmaker()
Session.configure(bind=engine)


class TaskInfo(Base):
    __tablename__ = 'taskinfo'

    id = Column(Integer, primary_key=True)
    input_data_type_id = Column(Integer)
    time_range = Column(String)
    functions = Column(String)
    result_data_type_id = Column(Integer)
    title = Column(String)

    def to_dict(self):
        _dict = {'id': self.id, 'input_data_type_id': self.input_data_type_id, 'time_range': self.time_range,
                 'functions': self.functions, 'result_data_type_id': self.result_data_type_id, 'title': self.title}
        return _dict


def insert_task(task_info):
    session = Session()
    info = TaskInfo(input_data_type_id=task_info['input_data_type_id'], time_range=task_info['time_range'],
                    functions=json_wrapper.dumps(task_info['functions']),
                    result_data_type_id=task_info['result_data_type_id'], title=task_info['title'])
    session.add(info)
    session.commit()
    result = info.id
    session.close()
    return result


def delete_task(task_info):
    session = Session()
    info = TaskInfo(id=task_info['id'])
    session.delete(info)
    session.commit()
    session.close()


def query_task():
    session = Session()
    rows = session.query(TaskInfo.id, TaskInfo.input_data_type_id, TaskInfo.time_range, TaskInfo.functions,
                         TaskInfo.result_data_type_id, TaskInfo.title)
    result = []
    for info in rows:
        temp = {'id': int(info[0]), 'input_data_type_id': int(info[1]),
                'time_range': json_wrapper.encode(info[2], 'utf-8'),
                'functions': json_wrapper.loads(info[3]), 'result_data_type_id': int(info[4]),
                'title': json_wrapper.encode(info[5], 'utf-8')}
        result.append(temp)
    session.close()
    return result


def query_task_by_id(task_id):
    session = Session()
    rows = session.query(TaskInfo.id, TaskInfo.input_data_type_id, TaskInfo.time_range, TaskInfo.functions,
                         TaskInfo.result_data_type_id, TaskInfo.title).filter(TaskInfo.id == task_id)
    for info in rows:
        temp = {'id': int(info[0]), 'input_data_type_id': int(info[1]),
                'time_range': json_wrapper.encode(info[2], 'utf-8'),
                'functions': json_wrapper.loads(info[3]), 'result_data_type_id': int(info[4]),
                'title': json_wrapper.encode(info[5], 'utf-8')}
        result = temp
    session.close()
    return result