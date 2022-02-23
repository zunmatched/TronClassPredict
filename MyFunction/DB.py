from pandas.core.frame import DataFrame
import pymysql
import pandas as pd
import traceback, logging

def _setting():
    return { ## <- 已打碼
        'host':'***.***.***.***',
        'user':'******',
        'password':'******',
        'db': '******',
        'charset':'utf8'
    }

def select(query, args = [], toRecords = False):
    conn = pymysql.connect(**_setting())
    cursor = conn.cursor()
    cursor.execute(query, args)
    col = [x[0] for x in cursor.description]
    result = cursor.fetchall()
    result = pd.DataFrame.from_records(result).set_axis(list(col), axis=1) if len(result) > 0 else pd.DataFrame()
    if toRecords:
        result = result.to_dict('records')
    cursor.close()
    conn.close()
    return result
    pass

def selectR(query, args = []):
    return select(query, args, toRecords = True)
    pass

def operate(query, args = []):
    conn = pymysql.connect(**_setting())
    cursor = conn.cursor()
    cursor.execute(query, args)
    conn.commit()
    cursor.close()
    conn.close()
    pass


def column_in(in_list = []):
    return ','.join(['%s'] * len(in_list))