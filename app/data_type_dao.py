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


class DataTypeInfo(Base):
    __tablename__ = 'datatypeinfo'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    attrs = Column(String)

    def to_dict(self):
        _dict = {'id': self.id, 'title': json_wrapper.encode(self.title, 'utf-8'),
                 'attrs': json_wrapper.loads(self.attrs)}
        return _dict


def insert_data_type(data_type):
    session = Session()
    info = DataTypeInfo(title=data_type['title'], attrs=json_wrapper.dumps(data_type['attrs']))
    session.add(info)
    session.commit()
    result = info.id
    session.close()
    return result


def query_data_type():
    session = Session()
    rows = session.query(DataTypeInfo.id, DataTypeInfo.title, DataTypeInfo.attrs)
    result = []
    for info in rows:
        temp = {'id': info[0], 'title': json_wrapper.encode(info[1], 'utf-8'), 'attrs': json_wrapper.loads(info[2])}
        result.append(temp)
    session.close()
    return result
