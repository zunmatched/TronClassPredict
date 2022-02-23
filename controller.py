from datetime import datetime, timedelta
import time
import pandas as pd

from data_process import *
from message_push import message_push

def _ctrl_info(now):
    '''內部用函數：取得現在時間、符合條件的學期開始時間與的課程'''
    ## 如果有指定日期則將日期文字轉時間，否則以現在時間為準
    end_date = datetime.strptime(now, '%Y-%m-%d') if now else datetime.now()
    course = pd.read_excel('data/other/course_list.xlsx', 'course')
    semester = pd.read_excel('data/other/course_list.xlsx', 'semester')
    try:
        semester = semester[(semester['start_at'] <= end_date) & (semester['end_at'] >= end_date)].iloc[0]
        start_date = semester['start_at']
        course_list = course[course['semester'] == semester['semester']]['course_id'].to_list()
        group_list = course[course['semester'] == semester['semester']]['group'].to_list()
        week = (end_date - start_date).days // 7
        return {'start_date':start_date, 'course_list':course_list, 'group_list':group_list, 'week':week, }
    except:
        return None

def ctrl_course(now=None, ignore_exist=True):
    '''取得課程的學生名單、資源數量與線上影片長度'''
    info = _ctrl_info(now)
    if info == None: return
    ignore_exist = False ## 每周都更新
    data_course(info['course_list'], ignore_exist=ignore_exist)
    data_activity(info['course_list'], ignore_exist=ignore_exist)

def ctrl_download(now=None, ignore_exist=True):
    '''透過API下載tronclass資料'''
    info = _ctrl_info(now)
    if info == None: return
    search_type = ['user_visit', 'material', 'online_video', 'weblink', 'discussion', 'exam', 'homework']
    for w in range(info['week']):
        w_start_date = info['start_date'] + timedelta(days = 7*w  )
        w_end_date   = info['start_date'] + timedelta(days = 7*w+6)
        data_download(search_type, w_start_date, w_end_date, ignore_exist=ignore_exist)

def ctrl_clean(now=None, ignore_exist=True):
    '''整理下載的tronclass資料'''
    info = _ctrl_info(now)
    if info == None: return
    for c in info['course_list']:
        data_clean(c, info['start_date'], info['week'], ignore_exist=ignore_exist)
        data_todo(c, info['start_date'], info['week'], ignore_exist=ignore_exist)

def ctrl_predict(now=None, ignore_exist=True):
    '''透過AI模型預測分數和下周的進度'''
    info = _ctrl_info(now)
    if info == None: return
    for c in info['course_list']:
        data_predict(c, info['week'], ignore_exist=ignore_exist)
    pass

def ctrl_compose(now=None, ignore_exist=True):
    '''根據clean、predict data撰寫給要寄給學生的信件'''
    info = _ctrl_info(now)
    if info == None: return
    for c, g in zip(info['course_list'], info['group_list']):
        if g == 'exp':
            data_compose_exp(c, info['week'], ignore_exist=ignore_exist)
        else:
            data_compose_ctrl(c, info['week'], ignore_exist=ignore_exist)

def ctrl_toSQL(now=None, ignore_exist=True):
    '''根據clean、predict data撰寫給要寄給學生的信件'''
    info = _ctrl_info(now)
    if info == None: return
    for c in info['course_list']:
        data_toSQL(c, info['week'], ignore_exist=ignore_exist)

def ctrl_push(now=None, ignore_exist=True):
    message_push(ignore_exist)

def ctrl_message_prepare(now=None, ignore_exist=True):
    '''資料下載、清理、預測、訊息撰寫'''
    for _ in range(3): ## 最多嘗試3次
        try:
            ctrl_course(now, ignore_exist)
            ctrl_download(now, ignore_exist)
            ctrl_clean(now, ignore_exist)
            ctrl_predict(now, ignore_exist)
            ctrl_compose(now, ignore_exist)
            ctrl_toSQL(now, ignore_exist)
            break
        except:
            time.sleep(30)
            continue
            
def ctrl_message_push(now=None, ignore_exist=True):
    '''整合資料下載、清理、預測、訊息撰寫'''
    for _ in range(3): ## 最多嘗試3次
        try:
            ctrl_push(now, ignore_exist)
            break
        except:
            time.sleep(30)
            continue
            
## 功能測試 / 
if __name__ == '__main__':
    now = '2021-10-03'
    ignore_exist = True
    ctrl_course(now, ignore_exist)
    ctrl_download(now, ignore_exist)
    ctrl_clean(now, ignore_exist)
    ctrl_predict(now, ignore_exist)
    ctrl_compose(now, ignore_exist)
    ctrl_toSQL(now, ignore_exist)
    ## 推送
    # ctrl_push(now, ignore_exist)
    pass