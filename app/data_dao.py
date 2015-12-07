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


class DataInfo(Base):
    __tablename__ = 'datainfo'

    id = Column(Integer, primary_key=True)
    data_type_id = Column(Integer)
    attrs = Column(String)
    time = Column(DateTime)

    def to_dict(self):
        _dict = {'id': self.id, 'attr': self.attr, 'data_type_id': self.data_type_id, 'time': self.time}
        return _dict


def insert_data(data):
    session = Session()
    info = DataInfo(data_type_id=data['data_type_id'], attrs=json_wrapper.dumps(data['attrs']), time=data['time'])
    session.add(info)
    session.commit()
    result = info.id
    session.close()
    return result


def query_data(data_type_id, start_time, end_time):
    session = Session()
    rows = session.query(DataInfo.id, DataInfo.attrs, DataInfo.time)\
        .filter(DataInfo.time >= start_time, DataInfo.time <= end_time, DataInfo.data_type_id == data_type_id)
    result = []
    for info in rows:
        temp = {'id': info[0], 'attrs': json_wrapper.loads(info[1]), 'time': info[2]}
        result.append(temp)
    session.close()
    return result
