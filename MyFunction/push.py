import requests
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication 
import smtplib
import re
from datetime import *

def LineMessage(id, message, mtype = 'text', title = ''):
    r_post=requests.post('https://aistudy.yuntech.edu.tw:******/******',data=json.dumps({ ## <- 已打碼
        'type':mtype,
        'title':title, 
        'message':message,
        'student_id':id
    }))
    return(r_post.text)

def sendEmail(email_to, subject, text, files = [], email_from = '******@gemail.yuntech.edu.tw', password_from = '******', host = 'smtp.gmail.com', port = '587'): ## <- 已打碼
    '''寄送附有檔案的email'''
    ## 構築email
    try:
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = email_to
        ## 加入正文
        msg.attach(MIMEText(text, 'plain'))
        ## 加入附件
        for f in files:
            part_attach1 = MIMEApplication(open(f,'rb').read()) ## 開啟附件
            part_attach1.add_header('Content-Disposition','attachment',filename=re.compile(r'(^.+\\|^.+/)').sub('', f)) ## 為附件命名
            msg.attach(part_attach1)   #新增附件
        ## 連接、登入伺服器
        with smtplib.SMTP(host=host, port=port) as smtp: 
            smtp.ehlo() ## 驗證SMTP伺服器
            smtp.starttls() ## 建立加密傳輸
            smtp.login(email_from, password_from)
            ## 寄送email
            smtp.sendmail(email_from, email_to, msg.as_string())
            pass
        pass
    except:
        print('SendEmail to {}: Failed'.format(str(email_to)))
        return False
    else:
        print('SendEmail to {}: Successful'.format(str(email_to)))
        return True
    pass