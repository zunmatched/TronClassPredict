import os, json
from datetime import datetime
# import pymysql
import pandas as pd

from MyFunction import flex, DB, push

def message_push(ignore_exist = True, test=True):
    if ignore_exist:
        message = DB.selectR('SELECT * FROM message_TronClassPredict WHERE push_menu IS NULL ORDER BY tcp_index ASC')
    else:
        message = DB.selectR('SELECT * FROM message_TronClassPredict ORDER BY tcp_index ASC')
    for m in message:
        if m['text_plan_1'] != None:
            m_flex = flex.flex_tcp_menu(m['tcp_index'], m['text_menu'])
            m_status = push.LineMessage('T11008758' if test else m['user_code'], m_flex, 'flex', 'TronClass成績預測-第{}周'.format(m['week']))
        else:
            m_status = push.LineMessage('T11008758' if test else m['user_code'], m['text_menu'], 'text', '')
        if m_status == '{"msg": "ok"}':
            now = datetime.now()
            DB.operate('UPDATE message_TronClassPredict SET push_menu = %s WHERE course_id = %s AND user_code = %s AND week = %s', args = [now, m['course_id'], m['user_code'], m['week']])
        # if test: break

if __name__ == '__main__':
    message_push(test=True)