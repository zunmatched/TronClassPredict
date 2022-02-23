import json,os,re, logging, math
from posixpath import sep
from datetime import datetime, timedelta
import numpy as np
import pymysql

import time
import pandas as pd

from bs4 import BeautifulSoup
from MyFunction.start_windows import *
from MyPyTorch import *
from MyFunction import DB

import base64
import hashlib
import json
import time
import torch

def data_course(course_list, ignore_exist = True):
    course = pd.read_excel('data/other/course_list.xlsx', 'course')
    driver = start_new_windows()
    os.makedirs('data/course/', exist_ok=True)

    ## type_count
    for c in course_list:
        c_file_type = 'data/course/{}_type_count.tsv'.format(c)
        if (os.path.isfile(c_file_type)) & (ignore_exist):continue
        config = pd.DataFrame()
        for type in ['chatroom', 'forum', 'homework', 'material', 'online_video', 'page', 'questionnaire', 'web_link']:
            count = 0
            page = 1
            while True:
                url = 'https://eclass.yuntech.edu.tw/api/course/{}/coursewares?conditions=%7B%22category%22:%22{}%22%7D&page={}&page_size=20'
                url = url.format(c, type, page)
                eclass = driver.request('GET',url)
                eclass = json.loads(eclass.text)
                count += len(eclass['activities'])
                if eclass['end'] >= eclass['total']:
                    break
                page += 1
                pass
            config = pd.concat([config, pd.DataFrame({'type':[type], 'count':[count]})])
            pass
        config.to_csv(c_file_type, sep='\t', index=False)
        pass

    ## video
    for c in course_list:
        c_file_video = 'data/course/{}_video.tsv'.format(c)
        if (os.path.isfile(c_file_video)) & (ignore_exist):continue
        detail = pd.DataFrame()
        page = 1
        video = []
        while True:
            url = 'https://eclass.yuntech.edu.tw/api/course/{}/coursewares?conditions=%7B%22category%22:%22{}%22%7D&page={}&page_size=20'
            url = url.format(c, 'online_video', page)
            eclass = driver.request('GET',url)
            eclass = json.loads(eclass.text)
            video += eclass['activities']
            if eclass['end'] >= eclass['total']:
                break
            page += 1
            pass
        for v in video:
            v_detail = pd.DataFrame({
                    'course_id':[v['course_id']], 
                    'video_id':[v['id']], 
                    'start_time':[v['start_time']], 
                    'end_time':[v['end_time']], 
                    'video_length':[0.0], 
                    'video_title':[v['title']], 
                })
            if len(v['uploads']) > 0:
                v_detail['video_length'] = v['uploads'][0]['videos'][0]['duration']
            elif v['data'].get('duration'):
                v_detail['video_length'] = v['data']['duration']
            else:
                while True:
                    driver.get(v['data']['link_original'])
                    time.sleep(5)
                    f_video_time = BeautifulSoup(driver.page_source, 'html.parser')
                    if(len(f_video_time.find_all('span', {'class':'ytp-ad-preview-container countdown-next-to-thumbnail'})) > 0):
                        continue
                    if(len(f_video_time.find_all('span', {'class':'ytp-ad-skip-button-icon'})) > 0):
                        continue
                    break
                f_video_time = f_video_time.find('span', {'class':"ytp-time-duration"}).getText()
                f_video_time = list(map(int, f_video_time.split(':')))
                f_video_time = sum(f_video_time[-x-1]*(60^x) for x in range(len(f_video_time)))
                if f_video_time < 1: continue
                v_detail['video_length'] = f_video_time

            detail = pd.concat([detail, v_detail])
        detail.to_csv(c_file_video, sep='\t', index=False)
    
    ## user code
    for c in course_list:
        c_file_user = 'data/course/{}_user_code.tsv'.format(c)
        if (os.path.isfile(c_file_user)) & (ignore_exist):continue
        url = 'https://eclass.yuntech.edu.tw/api/course/{}/enrollments?fields=user(id,name,user_no),roles'
        url = url.format(c)
        eclass = driver.request('GET',url)
        eclass = json.loads(eclass.text)
        user = eclass['enrollments']
        user = [v['user'] for v in user if 'student' in v['roles']]
        user = pd.DataFrame.from_records(user)
        user.rename({'id':'user_id', 'name':'user_name', 'user_no':'user_code'}, axis=1, inplace=True)
        course_code = str(course[course['course_id'] == int(c)]['course_code'].iat[0])
        user = pd.DataFrame({
            'year':[int(course_code[0:3])] * len(user), 
            'semester':[int(course_code[3:5])] * len(user), 
            'course_code':[int(course_code[5:])] * len(user),
            'user_id':user['user_id']
        }).merge(user, on = ['user_id'], how='inner')
        user.to_csv(c_file_user, sep='\t', index=False)

def data_activity(course_list, ignore_exist = True):
    driver = start_new_windows()
    for c in course_list:
        ## courseware
        os.makedirs('data/course/', exist_ok=True)
        c_file_video = 'data/course/{}_activity.tsv'.format(c)
        if (os.path.isfile(c_file_video)) & (ignore_exist):continue
        detail = pd.DataFrame()
        page = 1
        activity = []
        while True:
            url = 'https://eclass.yuntech.edu.tw/api/course/{}/coursewares?page={}&page_size=20'
            url = url.format(c, page)
            eclass = driver.request('GET',url)
            eclass = json.loads(eclass.text)
            activity += eclass['activities']
            if eclass['end'] >= eclass['total']:
                break
            page += 1
            pass
        for act in activity:
            if act['type'] in ['page', 'slide', 'interaction']:continue
            act_detail = pd.DataFrame({
                    'course_id':[c], 
                    'activity_id':[act['id']], 
                    'activity_type':[act['type']], 
                    'activity_title':[act['title']], 
                    'start_time':[act['start_time']], 
                    'end_time':[act['end_time']], 
                })
            if act['type'] == 'online_video':
                act_detail['video_length'] = 0.0
                if len(act['uploads']) > 0:
                    act_detail['video_length'] = act['uploads'][0]['videos'][0]['duration']
                elif act['data'].get('duration'):
                    act_detail['video_length'] = act['data']['duration']
                else:
                    while True:
                        driver.get(act['data']['link_original'])
                        time.sleep(5)
                        f_video_time = BeautifulSoup(driver.page_source, 'html.parser')
                        if(len(f_video_time.find_all('span', {'class':'ytp-ad-preview-container countdown-next-to-thumbnail'})) > 0):
                            continue
                        if(len(f_video_time.find_all('span', {'class':'ytp-ad-skip-button-icon'})) > 0):
                            continue
                        break
                    f_video_time = f_video_time.find('span', {'class':"ytp-time-duration"}).getText()
                    f_video_time = list(map(int, f_video_time.split(':')))
                    f_video_time = sum(f_video_time[-x-1]*(60^x) for x in range(len(f_video_time)))
                    if f_video_time < 1: continue
                    act_detail['video_length'] = f_video_time
            elif act['type'] == 'web_link':
                act_detail['activity_type'] = 'weblink'
            detail = pd.concat([detail, act_detail])
        ## homework
        page = 1
        activity = []
        while True:
            url = 'https://eclass.yuntech.edu.tw/api/courses/{}/homework-activities?page={}&page_size=20&reloadPage=false'
            url = url.format(c, page)
            eclass = driver.request('GET',url)
            eclass = json.loads(eclass.text)
            activity += eclass['homework_activities']
            if eclass['end'] >= eclass['total']:
                break
            page += 1
            pass
        for act in activity:
            act_detail = pd.DataFrame({
                    'course_id':[c], 
                    'activity_id':[act['id']], 
                    'activity_type':['homework'], 
                    'activity_title':[act['title']], 
                    'start_time':[act['start_time']], 
                    'end_time':[act['data']['submit_closed_time']], 
                    'score_percentage':[act['score_percentage']]
                })
            detail = pd.concat([detail, act_detail])
        ## exam
        page = 1
        activity = []
        while True:
            url = 'https://eclass.yuntech.edu.tw/api/courses/{}/exam-list?page={}&page_size=20&reloadPage=false'
            url = url.format(c, page)
            eclass = driver.request('GET',url)
            eclass = json.loads(eclass.text)
            activity += eclass['exams']
            if eclass['end'] >= eclass['total']:
                break
            page += 1
            pass
        for act in activity:
            act_detail = pd.DataFrame({
                    'course_id':[c], 
                    'activity_id':[act['id']], 
                    'activity_type':['exam'], 
                    'activity_title':[act['title']], 
                    'start_time':[act['submit_start_time']], 
                    'end_time':[act['end_time']], 
                    'score_percentage':[act['score_percentage']]
                })
            detail = pd.concat([detail, act_detail])
        ## discussion(forum)
        page = 1
        activity = []
        while True:
            url = 'https://eclass.yuntech.edu.tw/api/courses/{}/topic-categories?page={}&page_size=20&reloadPage=false'
            url = url.format(c, page)
            eclass = driver.request('GET',url)
            eclass = json.loads(eclass.text)
            activity += eclass['topic_categories']
            if eclass['end'] >= eclass['total']:
                break
            page += 1
            pass
        for act in activity:
            act_detail = pd.DataFrame({
                    'course_id':[c], 
                    'activity_id':[act['id']], 
                    'activity_type':['discussion'], 
                    'activity_title':[act['title']], 
                    'start_time':[act['activity'].get('start_time') if act['activity'] else None], 
                    'end_time':[act['activity'].get('end_time') if act['activity'] else None], 
                    'score_percentage':[act['activity'].get('score_percentage') if act['activity'] else None]
                })
            detail = pd.concat([detail, act_detail])
        ## final process & save
        detail = detail[~detail['activity_title'].isna()]
        detail.loc[~detail['start_time'].isna(), 'start_time'] = detail[~detail['start_time'].isna()]['start_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
        detail.loc[~detail['end_time'].isna(), 'end_time'] = detail[~detail['end_time'].isna()]['end_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
        detail.sort_values(['activity_type', 'end_time', 'start_time', 'activity_id'], inplace=True)
        detail.to_csv(c_file_video, sep='\t', index=False)


def data_download(search_type, start_date, end_date, data_path='data/raw/',ignore_exist = True):
    
    os.makedirs(data_path, exist_ok=True)
    start_date = datetime.strftime(start_date, '%Y%m%d')
    end_date = datetime.strftime(end_date, '%Y%m%d')
    all_file = dict(zip(search_type, [data_path+start_date+'_'+end_date+'_'+x+'.json' for x in search_type]))
    if (ignore_exist) and all([os.path.isfile(x) for x in all_file.values()]):return
    driver = start_new_windows()
    for s_type in all_file.keys():
        if (ignore_exist) and os.path.isfile(all_file[s_type]):continue
        timestamp = int(time.time()) ## 時間戳記
        app_key = '****************' ## <- 驗證碼：已打碼
        secret_key = '****************' ## <- 驗證碼：已打碼
        domain_name = 'http://eclass.yuntech.edu.tw' ## 域名

        ### 以天為單位分批下載資料
        s_date = datetime.strptime(start_date,'%Y%m%d')
        e_date = datetime.strptime(end_date,'%Y%m%d')

        temp_date = s_date
        hour = [0, 24]
        minute = [0, 60]

        data = []
        while temp_date <= e_date:
            search_time = []
            for h in range(len(hour)-1):
                for m in range(len(minute)-1):
                    hm = {
                        'from':str(hour[h]).zfill(2) + ':' + str(minute[m]).zfill(2) + ':00', 
                        'to'  :str(hour[h+1]-1).zfill(2) + ':' + str(minute[m+1]-1).zfill(2) + ':59', 
                    }
                    search_time += [hm]
                    pass
                pass
            for hms in search_time:
                print(temp_date.strftime('%Y%m%d-')+hms['from'], s_type,  end=' ')
                timestamp = int(time.time())
                url_partial = "/external-api/v1/org/1/learning-analysis/export/json?type={}&app_key={}&ts={}&start={}Z&end={}Z"\
                .format(s_type, app_key, timestamp, temp_date.strftime('%Y-%m-%dT') + hms['from'], temp_date.strftime('%Y-%m-%dT') + hms['to'])
                token = base64.urlsafe_b64encode(hashlib.md5(
                    (url_partial + secret_key).encode()).digest()).decode()

                # 參數 token 一定要加在網址的最後面
                _url = '{}{}&token={}'.format(domain_name, url_partial, token)
                # print(_url) 

                r = driver.request('GET', _url)
                print('status', str(r.status_code), end=' ')
                if (r.status_code == 502) :
                    time.sleep(5)
                    r = driver.request('GET', _url)
                elif (r.status_code == 500) :
                    break
                else:
                    dd = json.loads(r.text)['data']
                    print('len', str(len(dd)))
                    data = data + dd
                    pass
                pass
            if (r.status_code == 500) and (len(hms) == 1):
                hour = [*range(25)]
                minute = [0, 15, 30, 45, 60]
            elif (r.status_code == 500) and (len(hms) > 1):
                raise
            else:
                temp_date = temp_date + timedelta(days=1)

        new_data= json.dumps({'data':data}).encode()
        
        with open(all_file[s_type], 'wb') as file:
                for content in [new_data]:
                    file.write(content)

def data_clean(course_id, start_date, week, data_from = 'data/raw/', ignore_exist=True):
    logging.info('now working data clean')
    # week_until_now = (end_date-start_date).days//7
    #處理週次
    week_list=[]
    for i in range(week):
        week_list+=[(start_date + timedelta(days=(i)*7)).strftime('%Y%m%d')+'_'+(start_date + timedelta(days=(i)*7+6)).strftime('%Y%m%d')]

    logging.info('course = ',course_id)
    logging.info(week_list)
    activity_type_count = pd.read_csv('data/course/'+str(course_id) + '_type_count.tsv', sep='\t') ## activity count
    video_count = pd.read_csv('data/course/'+str(course_id)+ '_video.tsv', sep='\t') ## video_ count
    ## 務必注意輸出出來的week_list跟資料夾內檔案內容是否相符!!!!
    for week_n,this_week in enumerate(week_list): ## for執行到目前這週就可以了
        week_n +=1
        file_every = 'data/clean/{}w{}_every.csv'.format(str(course_id), str(week_n))
        file_accum = 'data/clean/{}w{}_accum.csv'.format(str(course_id), str(week_n))
        if (os.path.isfile(file_every) & os.path.isfile(file_accum)) & ignore_exist:
            continue
        os.makedirs('data/clean', exist_ok=True)
        logging.info('week_n = ',week_list)
        ###### 逐個資料類型進行清理  #########
        final_info = pd.read_csv('data/course/'+str(course_id)+'_user_code'+'.tsv', sep='\t')
        final_info['week'] = week_n
        final_table = final_info[['user_code']]
        logging.info(final_table)
        ###### user_visit #####
        with open(data_from+this_week+'_user_visit.json') as f:
            data = json.load(f)
            f.close()

        log_data = pd.DataFrame(data['data'])
        log_data = log_data[log_data['course_id'] == course_id]
        log_data = log_data[log_data['user_code'].isin(final_table['user_code'])]
        log_data.drop_duplicates(inplace=True)

        if len (log_data) == 0:continue
        log_data['visit_duration'].mask(log_data['visit_duration'] < 0, 0, inplace = True)
        group_log_data = log_data.groupby(['user_code','activity_type']).aggregate({'visit_duration':['count','sum']})

        group_log_data = group_log_data['visit_duration'].reset_index()

        group_log_data_count = group_log_data.pivot(index='user_code',columns='activity_type',values='count').fillna(0)
        group_log_data_sum = group_log_data.pivot(index='user_code',columns='activity_type',values='sum').fillna(0)

        group_log_data_count.columns = 'user_visit_count_' + group_log_data_count.columns
        group_log_data_sum.columns = 'user_visit_sum_' + group_log_data_sum.columns

        final_table = final_table.merge(group_log_data_count,on='user_code',how='left').fillna(0)
        final_table = final_table.merge(group_log_data_sum,on='user_code',how='left').fillna(0)

        #### Material ####

        with open(data_from+this_week+'_material.json') as f:
            data = json.load(f)
            f.close()
            
        log_data = pd.DataFrame(data['data'])
        log_data = log_data[log_data['course_id'] == course_id]
        log_data = log_data[log_data['user_code'].isin(final_table['user_code'])]
        log_data.drop_duplicates(inplace=True)

        group_log_data = log_data.groupby('user_code').aggregate({
            'action_type':['count']})
        group_log_data = group_log_data['action_type']
        group_log_data.columns=['material_count']

        final_table = final_table.merge(group_log_data,on='user_code',how='left').fillna(0)
        final_table['material_count_dividedby_count'] = final_table['material_count'] /activity_type_count[activity_type_count.type=='material']['count'].values[0]

        #### discussion ####

        with open(data_from+this_week+'_discussion.json') as f:
            data = json.load(f)
            f.close()
            
        log_data = pd.DataFrame(data['data'])
        log_data = log_data[log_data['course_id'] == course_id]
        log_data = log_data[log_data['user_code'].isin(final_table['user_code'])]
        log_data.drop_duplicates(inplace=True)

        if len(log_data) ==0:
            ## a fake data
            log_data = pd.DataFrame({"action_type": "create", "comment_id": None, "content": None, "course_code": "109025412", "course_id": 46629, "created_time": "2021-02-28 14:32:03", "dep_code": "GDO", "dep_id": 308, "enrollment_role": 10, "forum": None, "forum_id": 59718, "group_id": None, "is_deleted": 0, "module_id": None, "org_id": 1, "reply_id": None, "syllabus_id": None, "target_user_code": None, "title": "\u670d\u52d9\u8a2d\u8a08\u500b\u4eba\u4f5c\u696d", "topic_id": 57221, "topic_type": 1, "updated_time": None, "user_code": "T5465456465465465", "user_id": 123456789789798},index=[0])
            
            
        group_log_data = log_data.groupby(['user_code','action_type']).aggregate({
            'comment_id':['count']})


        group_log_data = group_log_data['comment_id'].reset_index(drop=False)

        group_log_data = group_log_data.pivot(index='user_code',columns='action_type',values='count').fillna(0)
        try:
            group_log_data.columns=['discussion_create','discussion_like','discussion_read','discussion_update']
        except ValueError:
            for c in ['create','like','read','update']:
                if c not in group_log_data.columns:
                    group_log_data[c]=0
            group_log_data.columns=['discussion_create','discussion_like','discussion_read','discussion_update']

        final_table = final_table.merge(group_log_data,on='user_code',how='left').fillna(0)
        final_table['discussion_create_dividedby_count'] = final_table['discussion_create'] /activity_type_count[activity_type_count.type=='forum']['count'].values[0]
        final_table['discussion_like_dividedby_count'] = final_table['discussion_like'] /activity_type_count[activity_type_count.type=='forum']['count'].values[0]
        final_table['discussion_read_dividedby_count'] = final_table['discussion_read'] /activity_type_count[activity_type_count.type=='forum']['count'].values[0]
        final_table['discussion_update_dividedby_count'] = final_table['discussion_update'] /activity_type_count[activity_type_count.type=='forum']['count'].values[0]

        #### weblink ####

        with open(data_from+this_week+'_weblink.json') as f:
            data = json.load(f)
            f.close()

        log_data = pd.DataFrame(data['data'])
        log_data = log_data[log_data['course_id'] == course_id]
        log_data = log_data[log_data['user_code'].isin(final_table['user_code'])]

        if len(log_data)>0:    
            group_log_data = log_data.groupby('user_code').aggregate({
                'action_type':['count']})

            group_log_data = group_log_data['action_type']
            group_log_data.columns=['weblink_count']
            final_table = final_table.merge(group_log_data,on='user_code',how='left').fillna(0)
            final_table['weblink_count_dividedby_count'] = final_table['weblink_count'] /activity_type_count[activity_type_count.type=='web_link']['count'].values[0]

        else:
            logging.info('weblink '+this_week +' is empty.')
            final_table['weblink_count']=0

        ##### online_video ####

        with open(data_from+this_week+'_online_video.json') as f:
            data = json.load(f)
            f.close()

        log_data = pd.DataFrame(data['data'])
        log_data = log_data[log_data['course_id'] == course_id]
        log_data = log_data[log_data['user_code'].isin(final_table['user_code'])]
        log_data = log_data[log_data['duration'].notna()]
        log_data = log_data[log_data['duration'] > 0]
        log_data.drop_duplicates(inplace=True)
        ## 影片點擊log數
        ## 總觀看時間
        group_log_data = log_data.groupby('user_code').aggregate({'duration':['count','sum']})
        group_log_data = group_log_data['duration']
        group_log_data.columns = ['online_video_total_count','online_video_total_time']
        final_table = final_table.merge(group_log_data,on='user_code',how='left').fillna(0)
        ## 總觀看時間/影片支度
        final_table['online_video_total_time_by_VideoCount'] = final_table['online_video_total_time'] /activity_type_count[activity_type_count.type=='online_video']['count'].values[0]
        ## 總觀看時間/影片時間長
        final_table['online_video_total_time_by_VideoLength'] = final_table['online_video_total_time'] /video_count.video_length.sum()

        group_log_data = log_data.groupby('user_code').aggregate({'time':'nunique'})
        group_log_data['time'] = group_log_data['time'] * 60
        group_log_data.columns = ['online_video_real_time']
        final_table = final_table.merge(group_log_data,on='user_code',how='left').fillna(0)
        ## 總觀看時間/影片支度
        final_table['online_video_real_time_by_VideoCount'] = final_table['online_video_real_time'] /activity_type_count[activity_type_count.type=='online_video']['count'].values[0]
        ## 總觀看時間/影片時間長
        final_table['online_video_real_time_by_VideoLength'] = final_table['online_video_real_time'] /video_count.video_length.sum()

                #####
                # 有觀看的影片支數
        group_log_data = log_data.groupby(['user_code']).aggregate({
                'activity_id':pd.Series.nunique})
        group_log_data.columns=['online_video_unique_count']
        final_table = final_table.merge(group_log_data,on='user_code',how='left').fillna(0)

                ####
                #有觀看的影片支數/總支數
        final_table['online_video_unique_count_by_VideoCount'] = final_table['online_video_unique_count'] /activity_type_count[activity_type_count.type=='online_video']['count'].values[0]

                #####
                # 早上下午晚上時間
        def split_time(time_str):
            hour = int(time_str[11:13])
            if hour <8:
                return 'morning'
            elif hour >=8 and hour <18:
                return 'noon'
            elif hour >= 18 and hour <=24:
                return 'night'

        log_data['day_type'] = log_data.time.apply(split_time)
        group_log_data = log_data.groupby(['user_code','day_type']).aggregate({
                'duration':['count','sum']})
        group_log_data = group_log_data['duration']
        group_log_data = group_log_data.reset_index(drop=False)
        group_log_data_count = group_log_data.pivot(index='user_code',columns='day_type',values = 'count').fillna(0)
        group_log_data_sum = group_log_data.pivot(index='user_code',columns='day_type',values = 'sum').fillna(0)
        group_log_data_count.columns = 'online_video_count_by_day_type_'+group_log_data_count.columns
        group_log_data_sum.columns = 'online_video_time_by_day_type_'+group_log_data_sum.columns
        final_table = final_table.merge(group_log_data_count,on='user_code',how='left').fillna(0)
        final_table = final_table.merge(group_log_data_sum,on='user_code',how='left').fillna(0)

        #####
        # 平日假日
        def split_holiday(time_str):
            week_day = datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S').weekday()
            return 'workday' if week_day<5 else 'holiday'

        log_data['holiday_type'] = log_data.time.apply(split_holiday)
        group_log_data = log_data.groupby(['user_code','holiday_type']).aggregate({
                'duration':['count','sum']})
        group_log_data = group_log_data['duration']
        group_log_data = group_log_data.reset_index(drop=False)
        group_log_data_count = group_log_data.pivot(index='user_code',columns='holiday_type',values = 'count').fillna(0)
        group_log_data_sum = group_log_data.pivot(index='user_code',columns='holiday_type',values = 'sum').fillna(0)
        group_log_data_count.columns = 'online_video_count_by_holiday_type_'+group_log_data_count.columns
        group_log_data_sum.columns = 'online_video_time_by_holiday_type_'+group_log_data_sum.columns
        final_table = final_table.merge(group_log_data_count,on='user_code',how='left').fillna(0)
        final_table = final_table.merge(group_log_data_sum,on='user_code',how='left').fillna(0)

        ## unwant columns
        final_table.drop(columns=['discussion_update','discussion_update_dividedby_count'],inplace=True)
        final_table =final_info.merge(final_table, on=['user_code'])
        final_table.to_csv(file_every,index=False)

        ## combine last week record (accum)
        if week_n > 1:
            data_accum_lw = pd.read_csv('data/clean/{}w{}_accum.csv'.format(str(course_id), str(week_n-1)))
            cl_remove = final_info.columns.tolist()
            cl_remove.remove('user_code')
            data_accum_lw.drop(cl_remove, axis=1, inplace = True)
            final_table.drop(cl_remove, axis=1, inplace = True)
            final_table = pd.concat([data_accum_lw, final_table]).fillna(0).groupby(['user_code'], as_index=False).sum()
            final_table = final_info.merge(final_table, on=['user_code'])
        final_table.to_csv(file_accum,index=False)
        print(str(course_id), 'week',str(week_n), 'done')
                # final_table

def duration_processer(df):
    df_duration = df.apply(lambda x: json.dumps([y for y in range(int(x['start_at']), int(x['end_at']))]), axis=1)
    df_lenght = df['video_length'].iat[0]
    duration = []
    for d in df_duration.tolist():
        duration += json.loads(d)
    duration = len(np.unique(duration))
    return duration / df_lenght

def video_processer(course, start_date, week):
    week_list=[]
    for w in range(week):
        week_list+=[(start_date + timedelta(days=(w)*7)).strftime('%Y%m%d')+'_'+(start_date + timedelta(days=(w)*7+6)).strftime('%Y%m%d')]
    user_list = pd.read_csv('data/course/{}_user_code.tsv'.format(course), sep='\t')
    act_list = pd.read_csv('data/course/{}_activity.tsv'.format(course), sep='\t')
    act_list = act_list[act_list['activity_type'] == 'online_video'][['activity_id', 'video_length']]
    act_list = act_list[(act_list['video_length'].isna()) | (act_list['video_length'] > 0)]
    data_comp = pd.DataFrame()
    for week_range in week_list:
        with open('data/raw/{}_online_video.json'.format(week_range)) as f:
            data = json.load(f)
        f.close()
        data = pd.DataFrame(data['data'])
        if len(data) == 0: continue
        data = data[data['course_id'] == course]
        data = data[data['action_type'] == 'play']
        data = data[data['duration'] > 0]
        if len(data) == 0: continue
        data = data[data['user_code'].isin(user_list['user_code'])]
        data = data[['activity_id', 'user_code', 'start_at', 'end_at']]
        data_comp = pd.concat([data_comp, data])
    if len(data_comp) == 0: return pd.DataFrame()
    data_comp = data_comp.merge(act_list, on = ['activity_id'])
    data_comp = data_comp.groupby(['activity_id', 'user_code'], as_index=False).apply(lambda x: pd.Series({'complete':duration_processer(x)}))
    data_comp = data_comp[data_comp['complete'] >= 0.8][['activity_id', 'user_code']]
    return data_comp.drop_duplicates()

def data_todo(course, start_date, week, ignore_exist = True):
    week_list=[]
    for w in range(week):
        week_list+=[(start_date + timedelta(days=(w)*7)).strftime('%Y%m%d')+'_'+(start_date + timedelta(days=(w)*7+6)).strftime('%Y%m%d')]
    user_list = pd.read_csv('data/course/{}_user_code.tsv'.format(course), sep='\t')
    act_list = pd.read_csv('data/course/{}_activity.tsv'.format(course), sep='\t')
    act_list['start_time'] = act_list['start_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') if (type(x) == str) else None)
    act_list['end_time'] = act_list['end_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') if (type(x) == str) else None)
    act_list = act_list[~act_list['activity_title'].isna()]
    act_list = act_list[(act_list['video_length'].isna()) | (act_list['video_length'] > 0)]
    act_chinese = {
        'material':'參考檔案', 
        'online_video':'影音教材(每部至少看完80%)', 
        'weblink':'線上連結', 
        'homework':'作業', 
        'exam':'線上測驗', 
        'discussion':'討論', 
    }
    act_list = act_list[act_list['activity_type'].isin(act_chinese.keys())]
    user_list_0 = user_list[['user_code']]
    user_list_0['tmp'] = 1
    data_comp_0 = act_list[['activity_id']]
    data_comp_0['tmp'] = 1
    data_comp_0 = data_comp_0.merge(user_list_0, on = ['tmp'])
    data_comp_0.sort_values(['user_code', 'activity_id'], inplace=True)
    data_comp_0.drop(columns=['tmp'], inplace=True)
    for week_num, week_range in enumerate(week_list, start=1):
        data_comp = pd.DataFrame({'activity_id':[], 'user_code':[]})
        file_complete = 'data/todo/{}w{}_complete.xlsx'.format(course, week_num)
        file_todolist = 'data/todo/{}w{}_todolist.xlsx'.format(course, week_num)
        if (os.path.isfile(file_complete) & os.path.isfile(file_todolist)) & ignore_exist: continue
        for act_type in act_chinese.keys():
            if act_type == 'online_video': ## 影片的資料要分開處理
                data = video_processer(course, start_date, week_num)
                data_comp = pd.concat([data_comp, data])
                continue
            with open('data/raw/{}_{}.json'.format(week_range, act_type)) as f:
                data = json.load(f)
            f.close()
            data = pd.DataFrame(data['data'])
            if len(data) == 0: continue
            data = data[data['course_id'] == course]
            ## 針對個別的項目做修改
            if act_type == 'exam':
                data.rename(columns={'exam_id':'activity_id'}, inplace=True)
            if act_type == 'discussion':
                data.rename(columns={'forum_id':'activity_id'}, inplace=True)
            elif act_type == 'discussion':
                data = data[data['action_type'].isin(['create', 'edit'])]
            if len(data) == 0: continue
            data = data[['activity_id', 'user_code']]
            data_comp = pd.concat([data_comp, data])
        data_comp = data_comp.drop_duplicates()
        data_comp = data_comp[data_comp['user_code'].isin(user_list['user_code'])]
        if week_num > 1:
            data_comp_lw = pd.read_excel('data/todo/{}w{}_complete.xlsx'.format(course, week_num-1))
            data_comp_lw = pd.melt(data_comp_lw, id_vars=['activity_id'], var_name='user_code', value_name='complete')
            data_comp_lw = data_comp_lw[data_comp_lw['complete'] == 1]
            data_comp_lw = data_comp_lw[['activity_id', 'user_code']]
            data_comp = pd.concat([data_comp_lw, data_comp]).drop_duplicates()
        data_comp = data_comp_0.merge(data_comp, how = 'left', on = ['activity_id', 'user_code'], indicator=True)
        data_comp['complete'] = [int(x == 'both') for x in data_comp['_merge']]
        data_comp = data_comp.drop(columns=['_merge'])
        data_comp_save = data_comp[data_comp['complete'] == 1].drop_duplicates()
        # data_comp_save.reset_index(drop=True, inplace=True)
        data_comp_save = data_comp_save.pivot('activity_id', 'user_code', 'complete')
        data_comp_save.mask(data_comp_save.isna(), 0, inplace=True)
        data_comp_save.reset_index(inplace=True)
        os.makedirs('data/todo', exist_ok=True)
        data_comp_save.to_excel(file_complete, index=False)
        ## to-do list
        data_todo = data_comp[data_comp['complete'] == 0].drop_duplicates()
        data_todo = data_todo.merge(act_list, on = ['activity_id'])
        data_todo = data_todo[data_todo['start_time'].isna() | (data_todo['start_time'] <= (start_date + timedelta(days=(week_num)*7+7)))]
        data_todo = data_todo[data_todo['end_time'].isna() | (data_todo['end_time'] >= (start_date + timedelta(days=(week_num)*7)))]
        data_todo.sort_values(['end_time', 'start_time', 'activity_title'], inplace=True)
        data_todo['todo_list'] = data_todo.apply(lambda x: '> ' + x['activity_title'] + \
            ('' if x.isna()['end_time'] else '\n  截止時間：{}'.format(datetime.strftime(x['end_time'], "%Y-%m-%d %H:%M"))) +\
            ('' if x.isna()['score_percentage'] else '\n  分數占比：{}%'.format(x['score_percentage'])), axis=1)
        data_todo = data_todo.groupby(['user_code', 'activity_type'], as_index=False).apply(lambda x:pd.Series({
            'todo_list':act_chinese[x['activity_type'].iat[0]] + '：\n' + '\n'.join(x['todo_list'].tolist()[:5]) + ('\n> ...'if len(x['todo_list']) > 5 else '')
        }))
        data_todo = data_todo.groupby(['user_code'], as_index=False).apply(lambda x:pd.Series({
            'todo_list':'\n\n'.join(x['todo_list'].tolist()), 
        }))
        data_todo = user_list[['user_code']].merge(data_todo, how = 'left', on = ['user_code'])
        data_todo['todo_list'] = data_todo['todo_list'].apply(lambda x: x if len(x)>0 else '目前沒有新的代辦事項，你做得真棒~!!')
        data_todo.to_excel(file_todolist, index=False)
    data_comp_0
    pass

def data_predict(course, week, ignore_exist = True):
    file_result = 'data/result/{}w{}_result.csv'.format(str(course),str(week))
    if os.path.isfile(file_result) & ignore_exist:return
    os.makedirs('data/result', exist_ok=True)
    c_name = pd.read_excel('data/other/course_list.xlsx', 'course')
    c_name = c_name[c_name['course_id'] == course]['course_name'].iat[0]
    c_model_score = 'model_pytorch/ScoreRangePredict_{}.pt'.format(c_name)
    c_model_score = c_model_score if os.path.isfile(c_model_score) else 'model_pytorch/ScoreRangePredict.pt'
    c_model_score = torch.load(c_model_score)
    c_model_advice = 'model_pytorch/Advice_{}.pt'.format(c_name)
    c_model_advice = c_model_advice if os.path.isfile(c_model_advice) else 'model_pytorch/Advice.pt'
    c_model_advice = torch.load(c_model_advice)
    c_model_effect = 'model_pytorch/ScorePredict_{}.pt'.format(c_name)
    c_model_effect = c_model_effect if os.path.isfile(c_model_effect) else 'model_pytorch/ScorePredict.pt'
    c_model_effect = torch.load(c_model_effect)
    # c_model_effect = ScorePredict()
    c_user = pd.read_csv('data/course/{}_user_code.tsv'.format(str(course)), sep='\t')
    c_user = c_user[['user_code', 'user_name']]
    c_data = pd.read_csv('data/clean/{}w{}_accum.csv'.format(str(course),str(week)))
    c_data = c_user[['user_code']].merge(c_data, on=['user_code'])
    c_user = c_user.merge(c_model_score.result_df2df(c_data), on=['user_code'])
    c_user = c_user.merge(c_model_effect.result_df2df(c_data), on=['user_code'])
    c_data['score'] = c_user['score_thisWeek']
    c_user = c_user.merge(c_model_advice.result_df2df(c_data), on=['user_code'])
    ## 存檔
    c_user.to_csv(file_result,index=False)
    pass

def kGroup(s, n, if_dup):
    s1 = pd.DataFrame({'col':s.tolist()})
    s1['index1'] = [x for x in range(len(s))]
    s1.sort_values('col', inplace=True)
    s1['index2'] = [(x / len(s) * n) // 1 for x in range(len(s))]
    s2 = s1.groupby(['col'], as_index=False).agg({'index2':if_dup})
    s1['index2'] = [s2[s2['col'] == x]['index2'].iat[0] for x in s1['col']]
    s1.sort_values('index1', inplace=True)
    return s1['index2']

def feature_rank(n, x_data):
    x_text = n[[bool(re.match('text_', x)) for x in n.index]].tolist()
    if n['feature'] in x_data.columns.tolist():
        x_data = x_data[n['feature']].fillna(0)
        x_data_1 = kGroup(x_data, len(x_text), 'min')
        x_data_1 = pd.Series([n['chinese']+': '+n['text_'+str(int(x))] for x in x_data_1])
        return x_data_1
    else: 
        return pd.Series([None]*len(x_data), index=x_data.index)

def feature_advice(n, x_data, a_type):
    x_text = n[[bool(re.match('text_', x)) for x in n.index]].tolist()
    if n['feature']+'_{}'.format(a_type) in x_data.columns.tolist():
        x_data = x_data[n['feature']+'_{}'.format(a_type)].fillna(0)
        x_data_1 = kGroup(x_data, len(x_text), 'min')
        x_data_1 = pd.Series(['* '+n['text_'+str(int(x))] for x in x_data_1])
        if n['unit'] == 'time':
            x_m = x_data // 60
            x_h = x_m // 60
            x_m = x_m % 60
            x_t = ['(還差{}小時{}分鐘)'.format(h, m) if h > 0 else '(還差{}分鐘)'.format(m) for h, m in zip(x_h, x_m)]
            x_data_1 = pd.Series([x+y for x, y in zip(x_data_1.tolist(), x_t)])
            x_data_1.where((x_m > 0)|(x_h > 0), inplace=True)
            pass
        x_data_1.loc[x_data < 1] = None
        return x_data_1
    else: 
        return pd.Series([None]*len(x_data), index=x_data.index)

def advice_process(data_user, data_result, a_type):
    if len(data_user['advice'+'_{}'.format(a_type)]) == 0:
        return '你目前做的很好！請繼續保持喔～！'
    # score_tw = data_result[data_result['user_code'] == data_user['user_code']]['score_thisWeek'].iat[0]
    # score_nw = data_result[data_result['user_code'] == data_user['user_code']]['score_nextWeek'].iat[0]
    return data_user['advice'+'_{}'.format(a_type)]
    # ifNot = score_tw - score_nw
    # if ifNot < 0.5:
    #     return data_user['advice'+'_{}'.format(a_type)]
    # ifNot = '{:.1f}分'.format(ifNot) if (ifNot < 10) else '10分以上'
    # ifNot = '\n\n如果沒達成以上條件，下週的預測分數可能會下降' + ifNot
    # return data_user['advice'+'_{}'.format(a_type)] + ifNot
    pass

def data_compose_exp(course, week, ignore_exist=True):
    text_menu = '''哈囉{user_name}：
我是你的AI自主學習管理經紀人-茄爸！
茄爸按照你到第{week}週在課程平台上的學習表現，要來預測你在「{course_name}」課程的通過機率囉！(*ﾟ∀ﾟ*)燈燈燈....

----------------------------
【茄爸給{user_name}的學習行為診斷】
(代表你在班上的相對位置)

{rank}
----------------------------
【茄爸給{user_name}的學習表現預測】

預測成績落點：{score_range}分
預測修課班排：前{rankUp}%
本週預測模型準確度：{accuracy}
----------------------------

茄爸在這邊給你一些下週的學習建議方案：
> 方案一、希望期末能提高至{score_range_1}分。(預估提升排名至前{rankUp_1}%)
> 方案二、希望期末以80分穩穩地通過。(預估提升排名至前{rankUp_2}%)
> 方案三、這週我想休息一下。{ignore}

接下來每週都會提供你的學習處方箋，讓我們下週見囉！
by 關心你的 茄爸'''

    text_plan_1 = '''【茄爸給{user_name}的第{week}週學習處方箋：方案一】
如果你希望期末能提高至{score_range_1}分的話，下面是我們給你未來一週在課程平台上的學習建議：
----------------------------
{advice_1}
----------------------------
預估提升排名至前{rankUp_1}%'''

    text_plan_2 = '''【茄爸給{user_name}的第{week}週學習處方箋：方案二】
如果你希望期末能穩穩地通過這門課(以80分為目標)，下面是我們給你未來一週在課程平台上的學習建議：
----------------------------
{advice_2}
----------------------------
預估提升排名至前{rankUp_2}%'''

    text_plan_3 = '''【茄爸給{user_name}的第{week}週學習處方箋：方案三】
這週{user_name}想休息一下。

茄爸理解你的選擇，這週進度就先暫停一下吧。
等到{user_name}有空了，記得再回到課程平台一起討論學習喔~!!'''

    text_intro = '''{user_name}您好：
我是你的AI學習管理經紀人茄爸，蒐集1851筆歷年同學線上修課資料，近2年內已主動發出上千筆線上學習診斷報告，是你的支持型陪伴系統。

我的AI運作架構，是目前診斷同學真實學習數據並在早期預測你學習表現的科學化工具。
同學可以每週定期完成診斷、獲取建議、預測期末成績與揭露預測精準度等資訊。
過去某些同學，因為不熟悉如何進行非同步遠距學習，且未能精準應用AI學習診斷報告，延誤調整線上學習策略的時機。

本週起發送的學習診斷報告，所有的建議選擇『沒有絕對最好的』，只有『最適合您的』便是最好的選擇。
{user_name}可以了解後，再選擇一個最想要『最適合您的』決定。

適用對象：經AI評估可以透過調整線上學習策略，而提高期末成績表現或避免不及格者。'''

    file_message = 'data/message/{}w{}_message.xlsx'.format(str(course),str(week))
    if os.path.isfile(file_message) & ignore_exist:return
    os.makedirs('data/message', exist_ok=True)
    data_accum  = pd.read_csv('data/clean/{}w{}_accum.csv'.format(str(course),str(week)))
    data_result = pd.read_csv('data/result/{}w{}_result.csv'.format(str(course),str(week)))
    text_rank   = pd.read_excel('data/other/text_material.xlsx', 'rank')
    text_advice = pd.read_excel('data/other/text_material.xlsx', 'advice')
    course_name = pd.read_excel('data/other/course_list.xlsx', 'course')
    course_name = course_name[course_name['course_id'] == course]['course_name'].iat[0]
    accuracy = pd.read_excel('data/other/accuracy.xlsx')
    if course_name in accuracy['course_name'].tolist():
        accuracy = accuracy[(accuracy['week'] == week) & (accuracy['course_name'] == course_name)]['accuracy_opti'].iat[0]
    else:
        accuracy = accuracy[(accuracy['week'] == week)]['accuracy_orig'].mean()
    accuracy = round(accuracy*100, 1)
    accuracy = str(accuracy)+'%' if accuracy < 90 else r'90.0%以上'
    
    data_user = data_result[['user_code', 'user_name', 'score_thisWeek']]
    data_user.rename(columns={'score_thisWeek':'score'}, inplace=True)
    data_user['week'] = week
    data_user['score'] = data_user['score'].mask(data_user['score'] > 99, 99)
    data_user['score_range'] = [math.floor(x / 5) for x in data_user['score']]
    data_user['score_range'] = ['{}~{}'.format(x*5, x*5+4) for x in data_user['score_range']]
    data_user['rankUp'] =  data_user['score'].apply(lambda x: math.floor((data_result['score_thisWeek'] >= x).mean() * 20))
    data_user['rankUp'] =  ['{}~{}'.format(x*5, x*5+5) for x in data_user['rankUp']]
    data_accum = data_user[['user_code']].merge(data_accum, on = ['user_code'])
    data_accum['score'] = data_result['score_thisWeek']
    data_accum['ignore'] = data_result['score_thisWeek'] - data_result['score_nextWeek']
    data_accum['ignore'] = data_accum['ignore'].mask(data_accum['ignore'] > 10, 10)
    data_rank = text_rank.apply(lambda x: feature_rank(x, data_accum), axis=1)
    data_user['rank'] = data_rank.apply(lambda x: '\n'.join(x.dropna().tolist()))
    data_advice_1 = text_advice.apply(lambda x: feature_advice(x, data_result, 1), axis=1)
    data_user['advice_1'] = data_advice_1.apply(lambda x: '\n'.join(x.dropna().tolist()))
    data_user['advice_1'] = data_user.apply(lambda x: advice_process(x, data_result, 1), axis=1)
    data_advice_2 = text_advice.apply(lambda x: feature_advice(x, data_result, 2), axis=1)
    data_user['advice_2'] = data_advice_2.apply(lambda x: '\n'.join(x.dropna().tolist()))
    data_user['advice_2'] = data_user.apply(lambda x: advice_process(x, data_result, 2), axis=1)
    data_user['ignore'] = data_accum.apply(lambda x: '(如果這週沒有上線，期末的預測分數可能會下降{:.1f}分以上)'.format(x['ignore']) if (x['ignore'] >= 0.5) else '', axis = 1)
    data_user['score_1'] = [(x + 10) for x in data_result['score_thisWeek']]
    data_user['score_1'] = data_user['score_1'].mask(data_user['score_1'] > 99, 99)
    data_user['rankUp_1'] =  data_user['score_1'].apply(lambda x: math.floor((data_result['score_thisWeek'] >= x).mean() * 20))
    data_user['rankUp_1'] =  ['{}~{}'.format(x*5, x*5+5) for x in data_user['rankUp_1']]
    data_user['score_range_1'] = [math.floor(x / 5) for x in data_user['score_1']]
    data_user['score_range_1'] = ['{}~{}'.format(x*5, x*5+4) for x in data_user['score_range_1']]
    data_user['score_2'] =  80
    data_user['rankUp_2'] =  data_user['score_2'].apply(lambda x: math.floor((data_result['score_thisWeek'] >= x).mean() * 20))
    data_user['rankUp_2'] =  ['{}~{}'.format(x*5, x*5+5) for x in data_user['rankUp_2']]
    data_user['course_name'] = course_name
    data_user['accuracy'] = accuracy
    data_message = data_user[['user_code', 'week', 'score']]
    data_message['text_menu'] = [text_menu.format(**x) for x in data_user.to_dict('records')]
    data_message['text_plan_1'] = [text_plan_1.format(**x) for x in data_user.to_dict('records')]
    data_message['text_plan_2'] = [text_plan_2.format(**x) for x in data_user.to_dict('records')]
    data_message['text_plan_3'] = [text_plan_3.format(**x) for x in data_user.to_dict('records')]
    data_todo = pd.read_excel('data/todo/{}w{}_todolist.xlsx'.format(course, week))
    data_message = data_message.merge(data_todo, on = 'user_code')
    data_message.to_excel(file_message, index=False)
    ## 茄爸預測簡介
    file_message_0 = 'data/message/{}w{}_message.xlsx'.format(str(course),str(0))
    if os.path.isfile(file_message_0) & ignore_exist:return
    data_intro = data_user[['user_code']]
    data_intro['week'] = 0
    data_intro['text_menu'] = [text_intro.format(**x) for x in data_user.to_dict('records')]
    data_intro.to_excel(file_message_0, index=False)

def data_compose_ctrl(course, week, ignore_exist=True):
    text_menu = '''哈囉{user_name}：
我是你的AI自主學習管理經紀人-茄爸！
茄爸按照你到第{week}週在課程平台上的學習表現，要來預測你在「{course_name}」課程的通過機率囉！(*ﾟ∀ﾟ*)燈燈燈....

----------------------------
【茄爸給{user_name}的學習行為診斷】
(代表你在班上的相對位置)

{rank}
----------------------------
【茄爸給{user_name}的學習表現預測】

預測成績落點：{score_range}分
預測修課班排：前{rankUp}%
本週預測模型準確度：{accuracy}
----------------------------
如果你希望期末能穩穩地通過這門課(以80分為目標)，下面是我們給你未來一週在課程平台上的學習建議：

{advice_2}

預估提升排名至前{rankUp_2}%{ignore}
----------------------------

【免責聲明】
茄爸根據學長姊的學習行為資料做成預測模型，建議只是預測模型提供的結果。
要怎麼自我調整學習，還是要靠自己做決定！茄爸不負責喔！ (人´ω`๑)

接下來每週都會提供你的學習處方箋，讓我們下週見囉！
by 關心你的 茄爸'''

    text_intro = '''{user_name}您好：
我是你的AI學習管理經紀人茄爸，蒐集1851筆歷年同學線上修課資料，近2年內已主動發出上千筆線上學習診斷報告，是你的支持型陪伴系統。

我的AI運作架構，是目前診斷同學真實學習數據並在早期預測你學習表現的科學化工具。
同學可以每週定期完成診斷、獲取建議、預測期末成績與揭露預測精準度等資訊。
過去某些同學，因為不熟悉如何進行非同步遠距學習，且未能精準應用AI學習診斷報告，延誤調整線上學習策略的時機。

本週起發送的學習診斷報告，所有的建議選擇『沒有絕對最好的』，只有『最適合您的』便是最好的選擇。
{user_name}可以了解後，再選擇一個最想要『最適合您的』決定。

適用對象：經AI評估可以透過調整線上學習策略，而提高期末成績表現或避免不及格者。'''

    file_message = 'data/message/{}w{}_message.xlsx'.format(str(course),str(week))
    if os.path.isfile(file_message) & ignore_exist:return
    os.makedirs('data/message', exist_ok=True)
    data_accum  = pd.read_csv('data/clean/{}w{}_accum.csv'.format(str(course),str(week)))
    data_result = pd.read_csv('data/result/{}w{}_result.csv'.format(str(course),str(week)))
    text_rank   = pd.read_excel('data/other/text_material.xlsx', 'rank')
    text_advice = pd.read_excel('data/other/text_material.xlsx', 'advice')
    course_name = pd.read_excel('data/other/course_list.xlsx', 'course')
    course_name = course_name[course_name['course_id'] == course]['course_name'].iat[0]
    accuracy = pd.read_excel('data/other/accuracy.xlsx')
    if course_name in accuracy['course_name'].tolist():
        accuracy = accuracy[(accuracy['week'] == week) & (accuracy['course_name'] == course_name)]['accuracy_opti'].iat[0]
    else:
        accuracy = accuracy[(accuracy['week'] == week)]['accuracy_orig'].mean()
    accuracy = round(accuracy*100, 1)
    accuracy = str(accuracy)+'%' if accuracy < 90 else r'90.0%以上'
    
    data_user = data_result[['user_code', 'user_name', 'score_thisWeek']]
    data_user.rename(columns={'score_thisWeek':'score'}, inplace=True)
    data_user['week'] = week
    data_user['score'] = data_user['score'].mask(data_user['score'] > 99, 99)
    data_user['score_range'] = [math.floor(x / 5) for x in data_user['score']]
    data_user['score_range'] = ['{}~{}'.format(x*5, x*5+4) for x in data_user['score_range']]
    data_user['rankUp'] =  data_user['score'].apply(lambda x: math.floor((data_result['score_thisWeek'] >= x).mean() * 20))
    data_user['rankUp'] =  ['{}~{}'.format(x*5, x*5+5) for x in data_user['rankUp']]
    data_accum = data_user[['user_code']].merge(data_accum, on = ['user_code'])
    data_accum['score'] = data_result['score_thisWeek']
    data_accum['ignore'] = data_result['score_thisWeek'] - data_result['score_nextWeek']
    data_accum['ignore'] = data_accum['ignore'].mask(data_accum['ignore'] > 10, 10)
    data_rank = text_rank.apply(lambda x: feature_rank(x, data_accum), axis=1)
    data_user['rank'] = data_rank.apply(lambda x: '\n'.join(x.dropna().tolist()))
    data_advice_1 = text_advice.apply(lambda x: feature_advice(x, data_result, 1), axis=1)
    data_user['advice_1'] = data_advice_1.apply(lambda x: '\n'.join(x.dropna().tolist()))
    data_user['advice_1'] = data_user.apply(lambda x: advice_process(x, data_result, 1), axis=1)
    data_advice_2 = text_advice.apply(lambda x: feature_advice(x, data_result, 2), axis=1)
    data_user['advice_2'] = data_advice_2.apply(lambda x: '\n'.join(x.dropna().tolist()))
    data_user['advice_2'] = data_user.apply(lambda x: advice_process(x, data_result, 2), axis=1)
    data_user['ignore'] = data_accum.apply(lambda x: '(如果這週沒有上線，期末的預測分數可能會下降{:.1f}分以上)'.format(x['ignore']) if (x['ignore'] >= 0.5) else '', axis = 1)
    data_user['score_1'] = [(x + 10) for x in data_result['score_thisWeek']]
    data_user['score_1'] = data_user['score_1'].mask(data_user['score_1'] > 99, 99)
    data_user['rankUp_1'] =  data_user['score_1'].apply(lambda x: math.floor((data_result['score_thisWeek'] >= x).mean() * 20))
    data_user['rankUp_1'] =  ['{}~{}'.format(x*5, x*5+5) for x in data_user['rankUp_1']]
    data_user['score_range_1'] = [math.floor(x / 5) for x in data_user['score_1']]
    data_user['score_range_1'] = ['{}~{}'.format(x*5, x*5+4) for x in data_user['score_range_1']]
    data_user['score_2'] =  80
    data_user['rankUp_2'] =  data_user['score_2'].apply(lambda x: math.floor((data_result['score_thisWeek'] >= x).mean() * 20))
    data_user['rankUp_2'] =  ['{}~{}'.format(x*5, x*5+5) for x in data_user['rankUp_2']]
    data_user['course_name'] = course_name
    data_user['accuracy'] = accuracy
    data_message = data_user[['user_code', 'week', 'score']]
    data_message['text_menu'] = [text_menu.format(**x) for x in data_user.to_dict('records')]
    data_message['text_plan_1'] = None
    data_message['text_plan_2'] = None
    data_message['text_plan_3'] = None
    data_message['todo_list'] = None
    data_message.to_excel(file_message, index=False)
    ## 茄爸預測簡介
    file_message_0 = 'data/message/{}w{}_message.xlsx'.format(str(course),str(0))
    if os.path.isfile(file_message_0) & ignore_exist:return
    data_intro = data_user[['user_code']]
    data_intro['week'] = 0
    data_intro['text_menu'] = [text_intro.format(**x) for x in data_user.to_dict('records')]
    data_intro.to_excel(file_message_0, index=False)

def data_toSQL(course, week, ignore_exist = True):
    file_message = 'data/message/{}w{}_message.xlsx'.format(str(course),str(week))
    if not os.path.isfile(file_message):return
    file_message = pd.read_excel(file_message)
    file_message = pd.concat([pd.read_excel('data/message/{}w{}_message.xlsx'.format(course, 0)), file_message])
    for c in file_message.columns:
        file_message[c] = file_message[c].mask(file_message[c].isna(), None)

    query_insert = '''INSERT INTO message_TronClassPredict (text_menu, text_plan_1, text_plan_2, text_plan_3, todo_list, course_id, user_code, week) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
    query_update = '''UPDATE message_TronClassPredict SET text_menu = %s, text_plan_1 = %s, text_plan_2 = %s, text_plan_3 = %s, todo_list = %s WHERE course_id = %s AND user_code = %s AND week = %s'''

    for x in file_message.to_dict('records'):
        try:
            DB.operate(query_insert, (x['text_menu'], x['text_plan_1'], x['text_plan_2'], x['text_plan_3'], x['todo_list'], course, x['user_code'], x['week']))
        except:
            if not ignore_exist:
                DB.operate(query_update, (x['text_menu'], x['text_plan_1'], x['text_plan_2'], x['text_plan_3'], x['todo_list'], course, x['user_code'], x['week']))
    
if __name__ == '__main__':
    def ctrl_info(now):
        '''內部用函數：取得現在時間、符合條件的學期開始時間與的課程'''
        end_date = datetime.now()if now == None else datetime.strptime(now, '%Y-%m-%d')
        course = pd.read_excel('data/other/course_list.xlsx', 'course')
        semester = pd.read_excel('data/other/course_list.xlsx', 'semester')
        try:
            semester = semester[(semester['start_at'] <= end_date) & (semester['end_at'] >= end_date)].iloc[0]
            start_date = semester['start_at']
            course_list = course[course['semester'] == semester['semester']]['course_id'].to_list()
            week = (end_date - start_date).days // 7
            return {'start_date':start_date,'end_date':end_date,'course_list':course_list,'week':week}
        except:
            return None

    semester = pd.read_excel('data/other/course_list.xlsx', 'semester')
    for end_at in semester['end_at']:
        end_at = datetime.strftime(end_at, '%Y-%m-%d')
        info = ctrl_info(end_at)
        data_course(info['course_list'], ignore_exist = False)
        data_activity(info['course_list'], ignore_exist = False)
        for c in info['course_list']:
            data_todo(c, info['start_date'], info['end_date'], info['week'], ignore_exist = True)
            pass

    for w in range(1, 18):
        # data_predict(53049, w, ignore_exist=False)
        data_compose_exp(53049, w, ignore_exist = False)
        data_toSQL(53049, w, ignore_exist = False)
    pass