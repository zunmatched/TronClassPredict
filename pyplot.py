import os, random

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# import torch
# import torch.nn as nn

# from MyPyTorch import *

os.makedirs('model_pytorch', exist_ok=True)
os.makedirs('figure', exist_ok=True)

data_week = pd.read_excel('data/other/accuracy.xlsx')

plt.figure(0, figsize=(8, 6))
for c in data_week['course_name'].drop_duplicates():
    c_data_week = data_week[data_week['course_name'] == c]
    plt.plot(c_data_week['week'].tolist(), c_data_week['accuracy_orig'].tolist(), label = c)
plt.title('標準模型準確率')
plt.ylabel('Accuracy')
plt.xlabel('Week')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False 
plt.ylim(-0.1,1.1)
plt.xlim(1,18)
plt.grid()
plt.legend(loc = 'lower right')
plt.savefig('figure/ScorePredict_AccuracyPerWeek_stnadard.png', dpi=300)#儲存圖片

plt.figure(1, figsize=(8, 6))
for c in data_week['course_name'].drop_duplicates():
    c_data_week = data_week[data_week['course_name'] == c]
    plt.plot(c_data_week['week'].tolist(), c_data_week['accuracy_opti'].tolist(), label = c)
plt.title('優化模型準確率')
plt.ylabel('Accuracy')
plt.xlabel('Week')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False 
plt.ylim(-0.1,1.1)
plt.xlim(1,18)
plt.grid()
plt.legend(loc = 'lower right')
plt.savefig('figure/ScorePredict_AccuracyPerWeek_optimized.png', dpi=300)#儲存圖片

plt.figure(2, figsize=(8, 6))
for c in data_week['course_name'].drop_duplicates():
    c_data_week = data_week[data_week['course_name'] == c]
    plt.plot(c_data_week['week'].tolist(), c_data_week['accuracy_diff'].tolist(), label = c)
plt.title('標準、優化模型準確率差異')
plt.ylabel('Accuracy')
plt.xlabel('Week')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False 
plt.ylim(-0.3,0.3)
plt.xlim(1,18)
plt.grid()
plt.legend(loc = 'lower right')
plt.savefig('figure/ScorePredict_AccuracyPerWeek_different.png', dpi=300)#儲存圖片