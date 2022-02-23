import pandas as pd
import torch
from torch import nn

def NOF(path = 'model_pytorch/normalize.csv'):
    '''NOF: num of features'''
    return len(pd.read_csv(path))
    pass

def normalize(n, x_data):
    if n.name in x_data.columns.tolist():
        x_data.fillna(0, inplace=True)
        return (x_data[n.name] - n['min']) / n['std']
    else: 
        return pd.Series([0]*len(x_data), index=x_data.index)

def normalize_reverse(n, x_data):
    if n.name in x_data.columns.tolist():
        x_data = (x_data[n.name]*n['std']) + n['min']
        x_data.astype('int')
        return x_data.astype('int')
    else: 
        return pd.Series([0]*len(x_data), index=x_data.index)

class ScoreRangePredict(nn.Module):
    '''根據TronClass紀錄預測學生成績'''
    def __init__(self):
        super(ScoreRangePredict, self).__init__()
        self.d0 = nn.Linear(NOF(), 128)
        self.d1 = nn.Linear(128, 128)
        self.d2 = nn.Linear(128, 64)
        self.d3 = nn.Linear(64, 64)
        self.d4 = nn.Linear(64, 32)
        self.d5 = nn.Linear(32, 32)
        self.d6 = nn.Linear(32, 20)
        self.d7 = nn.Linear(20, 20)
        self.pr0 = nn.PReLU()
        self.pr1 = nn.PReLU()
        self.pr2 = nn.PReLU()
        self.pr3 = nn.PReLU()
        self.pr4 = nn.PReLU()
        self.pr5 = nn.PReLU()
        self.pr6 = nn.PReLU()
        self.pr7 = nn.Softmax(dim=1)
    def forward(self, xb):
        xb = self.pr0(self.d0(xb))
        xb = self.pr1(self.d1(xb))
        xb = self.pr2(self.d2(xb))
        xb = self.pr3(self.d3(xb))
        xb = self.pr4(self.d4(xb))
        xb = self.pr5(self.d5(xb))
        xb = self.pr6(self.d6(xb))
        xb = self.pr7(self.d7(xb))
        return xb
    def result_df2df(self, x_data, path = 'model_pytorch/normalize.csv'):
        user = pd.DataFrame({'user_code':x_data['user_code'].tolist()})
        n_data = pd.read_csv(path)
        n_data.set_index('col', inplace=True)
        x_data = n_data.apply(lambda n: normalize(n, x_data), axis = 1).T
        y_data = self.forward(torch.tensor(x_data.to_numpy(), dtype=torch.float))
        r = list(y_data.size())[1]
        r = int((100 / r) // 1)
        y_data = [int(x) for x in torch.argmax(y_data, dim=1)]
        user['score_range'] = [str(y*r)+'~'+str(y*r+r-1) for y in y_data]
        return user

class ScoreRangePredict_MOE(nn.Module):
    '''根據TronClass紀錄預測學生成績'''
    def __init__(self, model_one, model_all='model_pytorch/ScoreRangePredict.pt'):
        super(ScoreRangePredict_MOE, self).__init__()
        self.m_one = torch.load(model_one)
        self.m_all = torch.load(model_all)
        for param in self.m_one.parameters():
            param.requires_grad = False
        for param in self.m_all.parameters():
            param.requires_grad = False
        self.d0 = nn.Linear(NOF(), 16)
        self.d1 = nn.Linear(16, 8)
        self.d2 = nn.Linear(8, 4)
        self.d3 = nn.Linear(4, 2)
        self.pr0 = nn.PReLU()
        self.pr1 = nn.PReLU()
        self.pr2 = nn.PReLU()
        self.pr3 = nn.Softmax(dim=1)
    def forward(self, xb):
        x_one = self.m_one(xb)
        x_all = self.m_all(xb)
        xb = self.pr0(self.d0(xb))
        xb = self.pr1(self.d1(xb))
        xb = self.pr2(self.d2(xb))
        xb = self.pr3(self.d3(xb))
        xb_one, xb_all = torch.split(xb, 1, dim=1)
        xb = (x_one * xb_one) + (x_all * xb_all)
        return xb
    def result_df2df(self, x_data, path = 'model_pytorch/normalize.csv'):
        user = pd.DataFrame({'user_code':x_data['user_code'].tolist()})
        n_data = pd.read_csv(path)
        n_data.set_index('col', inplace=True)
        x_data = n_data.apply(lambda n: normalize(n, x_data), axis = 1).T
        y_data = self.forward(torch.tensor(x_data.to_numpy(), dtype=torch.float))
        r = list(y_data.size())[1]
        r = int((100 / r) // 1)
        y_data = [int(x) for x in torch.argmax(y_data, dim=1)]
        user['score_range'] = [str(y*r)+'~'+str(y*r+r-1) for y in y_data]
        return user

class Range2Score(nn.Module):
    '''分數範圍 => 分數'''
    def __init__(self):
        super(Range2Score, self).__init__()
        self.d0 = nn.Linear(20, 20)
        self.d1 = nn.Linear(20, 20)
        self.d2 = nn.Linear(20, 10)
        self.d3 = nn.Linear(10, 10)
        self.d4 = nn.Linear(10, 10)
        self.d5 = nn.Linear(10, 1)
        self.pr0 = nn.PReLU()
        self.pr1 = nn.PReLU()
        self.pr2 = nn.PReLU()
        self.pr3 = nn.PReLU()
        self.pr4 = nn.PReLU()
        self.pr5 = nn.Hardsigmoid()
    def forward(self, xb):
        xb = self.pr0(self.d0(xb))
        xb = self.pr1(self.d1(xb))
        xb = self.pr2(self.d2(xb))
        xb = self.pr3(self.d3(xb))
        xb = self.pr4(self.d4(xb))
        xb = self.pr5(self.d5(xb))
        xb = xb * 100
        return xb

class ScorePredict(nn.Module):
    def __init__(self, file_srp = 'model_pytorch/ScoreRangePredict.pt', file_r2s = 'model_pytorch/Range2Score.pt', srp_requires_grad = False):
        super(ScorePredict, self).__init__()
        self.srp = torch.load(file_srp)
        self.r2s = torch.load(file_r2s)
        for param in self.srp.parameters():
            param.requires_grad = srp_requires_grad
    def forward(self, xb):
        xb = self.srp(xb)
        xb = self.r2s(xb)
        return xb
    def save_r2s(self, file_r2s = 'model_pytorch/Range2Score.pt'):
        torch.save(self.r2s, file_r2s)
    def result_df2df(self, x_data, path = 'model_pytorch/normalize.csv'):
        user = pd.DataFrame({'user_code':x_data['user_code'].tolist()})
        n_data = pd.read_csv(path)
        n_data.set_index('col', inplace=True)
        x_data_0 = n_data.apply(lambda n: normalize(n, x_data), axis = 1).T
        x_data_1 = x_data.copy()
        x_data_1['week'] = x_data_1['week'].apply(lambda x: x + 1)
        x_data_1 = n_data.apply(lambda n: normalize(n, x_data_1), axis = 1).T
        y_data_0 = self.forward(torch.tensor(x_data_0.to_numpy(), dtype=torch.float))
        y_data_1 = self.forward(torch.tensor(x_data_1.to_numpy(), dtype=torch.float))
        user['score_thisWeek'] = [float(x[0]) for x in y_data_0.detach().numpy()]
        user['score_nextWeek'] = [float(x[0]) for x in y_data_1.detach().numpy()]
        return user

class Advice(nn.Module):
    '''根據TronClass紀錄預測學生成績'''
    def __init__(self, bottleneck):
        super(Advice, self).__init__()
        # self.de0 = nn.Linear(4, 4)
        # self.de1 = nn.Linear(4, 4)
        self.de2 = nn.Linear(4, 2)
        self.de3 = nn.Linear(2, 14)
        # self.de0act = nn.PReLU()
        # self.de1act = nn.PReLU()
        self.de2act = nn.PReLU()
        self.de3act = nn.ReLU()
    def forward(self, xb):
        # xb = self.de0act(self.de0(xb))
        # xb = self.de1act(self.de1(xb))
        xb = self.de2act(self.de2(xb))
        xb = self.de3act(self.de3(xb))
        return xb
    def result_df2df(self, x_data0):
        x_data1 = x_data0.copy()
        x_data1['score'] += 10
        x_data1['score'] = x_data1['score'].mask(x_data1['score'] > 99, 99)
        user = pd.DataFrame({'user_code':x_data1['user_code'].tolist()})
        feature = pd.read_excel('data/other/text_material.xlsx', sheet_name='advice')['feature']
        n_data = pd.read_csv('model_pytorch/normalize.csv')
        n_data.set_index('col', inplace=True)
        x_data1['week'] += 1
        x_data = x_data1[['score']]
        x_data1 = n_data.apply(lambda n: normalize(n, x_data1), axis = 1).T
        x_data = pd.concat([x_data, x_data1[['year', 'semester', 'week']]], axis=1)
        x_data1 = x_data1.loc[:, x_data1.columns.isin(feature)]
        x_data = torch.tensor(x_data.to_numpy(), dtype=torch.float)
        x_data1 = torch.tensor(x_data1.to_numpy(), dtype=torch.float)
        y_data1 = self.forward(x_data) - x_data1
        y_data1 = pd.DataFrame(y_data1.detach().numpy())
        y_data1.columns = feature.to_list()
        # print(y_data1)
        y_data1 = n_data.apply(lambda n: normalize_reverse(n, y_data1), axis = 1).T
        y_data1 = y_data1.loc[:, y_data1.columns.isin(feature)]
        y_data1.columns = [y + '_1' for y in y_data1.columns]

        x_data2 = x_data0.copy()
        x_data2['score'] = 80
        user = pd.DataFrame({'user_code':x_data2['user_code'].tolist()})
        feature = pd.read_excel('data/other/text_material.xlsx', sheet_name='advice')['feature']
        n_data = pd.read_csv('model_pytorch/normalize.csv')
        n_data.set_index('col', inplace=True)
        x_data2['week'] += 1
        x_data = x_data2[['score']]
        x_data2 = n_data.apply(lambda n: normalize(n, x_data2), axis = 1).T
        x_data = pd.concat([x_data, x_data2[['year', 'semester', 'week']]], axis=1)
        x_data2 = x_data2.loc[:, x_data2.columns.isin(feature)]
        x_data = torch.tensor(x_data.to_numpy(), dtype=torch.float)
        x_data2 = torch.tensor(x_data2.to_numpy(), dtype=torch.float)
        y_data2 = self.forward(x_data) - x_data2
        y_data2 = pd.DataFrame(y_data2.detach().numpy())
        y_data2.columns = feature.to_list()
        # print(y_data2)
        y_data2 = n_data.apply(lambda n: normalize_reverse(n, y_data2), axis = 1).T
        y_data2 = y_data2.loc[:, y_data2.columns.isin(feature)]
        y_data2.columns = [y + '_2' for y in y_data2.columns]
        return pd.concat([user, y_data1, y_data2], axis=1)


def accuracy(out, yb):
    '''針對成績預測模型，利用R平方得到準確率'''
    return torch.mean((torch.argmax(out, dim=1) == torch.argmax(yb, dim=1)).float())

def accuracy_check(c, col, x_data_raw, y_data, n_data, model):
    c = c[col]
    c_index = (x_data_raw[col] == c).all(axis=1)
    c_x = n_data.apply(lambda n: normalize(n, x_data_raw[c_index]), axis = 1).T
    c_y = y_data[c_index]
    c_x = torch.tensor(c_x.to_numpy(), dtype=torch.float)
    c_y = torch.tensor(c_y.to_numpy(), dtype=torch.float)
    c_accuracy = float(accuracy(model(c_x), c_y))
    return c_accuracy

def whereMax(s):
    return pd.Series([(1.0 if s1 == max(s) else 0.0)for s1 in s])

