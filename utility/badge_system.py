import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn import preprocessing
from sklearn import metrics
import time
from datetime import datetime
import matplotlib as mpl
mpl.rcParams['font.sans-serif'] = ['KaiTi']
mpl.rcParams['font.serif'] = ['KaiTi']

course_log_data_path=r"D:\tmp_data\badges\course_log_data.csv"
syllabus_csv_path=r"D:\tmp_data\badges\课程大纲顺序2.csv"
video_len_csv_path=r"D:\tmp_data\badges\视频时长表.csv" #视频时长表.csv

df:pd.DataFrame # = pd.read_csv(course_log_data_path, encoding='gbk')
def get_full_table(path=course_log_data_path, encoding='gbk', course_id=373, class_id=370):
    # 读取
    df = pd.read_csv(path, encoding=encoding)
    df = df[(df['课程id'] == course_id) & (df['班级id'] == class_id)]
    # 清除访问时间小于5秒的数据
    df = df[df['持续时间(秒)'] > 5]
    # 进入时间修正
    df['离开时间(时间戳)'] = df['进入时间(时间戳)'] + df['持续时间(秒)'] * 1000
    import time

    out = []
    in2 = []
    for item in list(df['离开时间(时间戳)']):
        time_local = time.localtime(int(int(item) / 1000))
        t = time.strftime("%Y-%m-%dT%H:%M:%S", time_local)
        out.append(t)
    df['离开时间(ISO)'] = out

    # 原始进入时间对不上
    for item in list(df['进入时间(时间戳)']):
        time_local = time.localtime(int(int(item) / 1000))
        t = time.strftime("%Y-%m-%dT%H:%M:%S", time_local)
        in2.append(t)
    df['进入时间(ISO)2'] = in2

    # 视频观看行为汇总-观看次数、学习时长
    df_video = df[df['学习活动类型'] == 'video']
    df_video = df_video[['用户id', '学习活动id', '进入时间(ISO)2', '持续时间(秒)']]
    cols = ['用户id', '视频观看次数']
    # 总和 平均
    dft = df_video.groupby(['用户id'])['持续时间(秒)'].describe(percentiles=[0.25, 0.75]).reset_index()
    dft.rename(columns={'count': '视频观看次数', 'sum': '视频学习总时长', '25%': '视频观看1分位数', '75%': '视频观看3分位数'}, inplace=True)
    dft2 = df_video.groupby(['用户id'])['持续时间(秒)'].agg([('视频学习总时长(秒)', 'sum')]).reset_index()
    df_all_video = dft[cols]
    df_all_video = pd.merge(left=dft2, right=dft[cols], on=['用户id'], how='left')

    # 拼接反复观看次数
    a = df_video.groupby(['用户id', '学习活动id']).count().reset_index()
    a = a[a['进入时间(ISO)2'] >= 2].groupby(['用户id']).count().reset_index()
    a.rename(columns={'进入时间(ISO)2': '反复观看次数'}, inplace=True)
    df_all_video = pd.merge(left=df_all_video, right=a[['用户id', '反复观看次数']], on=['用户id'], how='left')
    df_all_video['反复观看次数'] = df_all_video['反复观看次数'].fillna(0)

    # 拼接视频观看比大于50%的行为次数
    df_length = pd.read_csv(video_len_csv_path)
    df_video['标准时长'] = df_video['学习活动id'].map(dict(zip(list(df_length['活动id']), list(df_length['时长（秒）']))))
    df_video['观看时长比'] = df_video['持续时间(秒)'] / df_video['标准时长']
    high_watch = df_video[df_video['观看时长比'] > 0.5].groupby(['用户id']).count().reset_index()
    high_watch.rename(columns={'进入时间(ISO)2': '高观看时长比'}, inplace=True)
    df_all_video = pd.merge(left=df_all_video, right=high_watch[['用户id', '高观看时长比']], on=['用户id'], how='left')
    df_all_video['高观看时长比'] = df_all_video['高观看时长比'].fillna(0)

    df_all = df_all_video.copy()  # 总表

    # 学习持续行为汇总-在线时长
    online_time = df[['用户id', '持续时间(秒)']].groupby(['用户id']).sum().reset_index()
    online_time.rename(columns={'持续时间(秒)': '在线总时长(秒)'}, inplace=True)
    df_all = pd.merge(left=df_all, right=online_time, on=['用户id'], how='left')  # 拼接在线时长

    # 问题解决行为
    df_quiz = df[(df['学习活动类型'] == 'assignment')]
    df_quiz = df_quiz[['用户id', '学习活动id', '进入时间(ISO)2', '进入时间(时间戳)', '持续时间(秒)']]

    cols = ['用户id', '提交次数', '答题用时(秒)']
    dft = df_quiz.groupby(['用户id'])['持续时间(秒)'].describe(percentiles=[0.25, 0.75]).reset_index()
    dft.rename(columns={'count': '提交次数', 'mean': '平均答题时长', '25%': '答题时长1分位数', '75%': '答题时长3分位数'}, inplace=True)
    dft['答题用时(秒)'] = dft['提交次数'] * dft['平均答题时长']
    df_all_quiz = dft[cols].copy()

    df_all = pd.merge(left=df_all, right=df_all_quiz, on=['用户id'], how='left')  # # 拼接答题行为

    # 社交协作行为
    df_discuss = df[df['学习活动类型'] == 'discussion']
    df_discuss = df_discuss[['用户id', '学习活动id', '进入时间(ISO)2', '持续时间(秒)']]

    cols = ['用户id', '评论区访问次数']
    dft = df_discuss.groupby(['用户id'])['持续时间(秒)'].describe(percentiles=[0.25, 0.75]).reset_index()
    dft.rename(columns={'count': '评论区访问次数', 'mean': '评论区平均停留时间', '25%': '评论区平均停留时间1分位数', '75%': '评论区平均停留时间3分位数'},
               inplace=True)
    dft2 = df_discuss.groupby(['用户id'])['持续时间(秒)'].agg([('评论区停留时间(秒)', 'sum')]).reset_index()
    df_all_discuss = pd.merge(left=dft2, right=dft[cols], on=['用户id'], how='left')

    df_all = pd.merge(left=df_all, right=df_all_discuss, on=['用户id'], how='left')  # # 拼接社交协作行为

    df_all.fillna(0, inplace=True)
    return df_all, df
medals = {'学习持续':['用户等级LV'+str(i) for i in range(1, 11)],
          '学习投入':['初来乍到', '勤练基础', '勤学苦练', '挑灯夜战', '层层深入', '挑灯夜战', '柳暗花明', '渐入佳境', '持续输入', '以此为始'],
          '问题解决':['问题解决的第一步', '反思的开始', '我学“废”了', 'debug debug debug', '轻而易举', '好、快、准！', 'Yo!答题王', '快速修正', '纠错专家', '迎接新开始'],
          '社交协作':['初次发声', '助人为乐', 'Open mind', '思维碰撞', '今日快乐', '共同体！', '听我一句', '社交达人', '谈笑风生', '大讨论家'],
          '单元进度':['新的开始（第1单元）', '顺利入门（第2单元）', '简单运用（第3单元）', '数据结构（第4单元）', '复杂运用（第5单元）', '讲述者1（第6单元）', '讲述者2（第7单元）', '小试牛刀（第8单元）', '新的领域（第9单元）', '图像识别（第10单元）', '高级运用（第11单元）']}
full_table,df = get_full_table()
point_table = {'学习持续': ['在线总时长(秒)'],
               '行为投入': ['视频学习总时长(秒)', '反复观看次数', '高观看时长比'],
               '问题解决': ['提交次数', '答题用时(秒)'],
               '社交协作': ['评论区访问次数', '评论区停留时间(秒)']}
def cal_online(user_id, point_gap=30):
    temp = full_table[['用户id']+point_table['学习持续']].copy()
    temp['积分'] = temp[point_table['学习持续'][0]] / 60 / point_gap
    return '学习持续', int(temp[temp['用户id']==user_id]['积分'])
def cal_engagement(user_id, time_point=10, review_point=2, video_watching=1):
    # 输入id，输出学习投入总分和各小项分
    # 每看十分钟 得1分
    # 每次重复观看 得2分
    # 每次看超过50%的内容，得1分
    total_time, review, video_watching = point_table['行为投入']
    temp = full_table[['用户id', total_time, review, video_watching]].copy()
    temp['时长积分'] = temp[total_time] / 60 / time_point
    temp['反复观看积分'] = temp[review] * 2
    temp['时长比积分'] = temp[video_watching]
    p1, p2, p3 = temp[temp['用户id']==user_id][['时长积分', '反复观看积分', '时长比积分']].values[0]
    return '学习投入', int(p1+p2+p3), int(p1), int(p2), int(p3)
def cal_prob_solving(user_id, submit_point=3, stay_gap=10):
    # 输入id，输出问题解决总分和各小项分
    # 每次提交得3分
    # 答题每停留10分钟 计1分
    sub_count, time_stay = point_table['问题解决']
    temp = full_table[['用户id', sub_count, time_stay]].copy()
    temp['提交积分'] = temp[sub_count] * submit_point
    temp['时长积分'] = temp[time_stay] / 60 / stay_gap
    p1, p2 = temp[temp['用户id']==user_id][['提交积分', '时长积分']].values[0]
    return '问题解决', int(p1+p2), int(p1), int(p2)
def cal_social(user_id, view_point=1, stay_gap=5):
    # 输入id，输出社交协作总分和各小项分
    # 每次访问讨论区计1分
    # 每停留5分钟 计1分
    view_count, time_stay = point_table['社交协作']
    temp = full_table[['用户id', view_count, time_stay]].copy()
    temp['访问积分'] = temp[view_count] * view_point
    temp['时长积分'] = temp[time_stay] / 60 / stay_gap
    p1, p2 = temp[temp['用户id']==user_id][['访问积分', '时长积分']].values[0]
    return '社交协作', int(p1+p2), int(p1), int(p2)

# 单元对应学习活动字典
df_unit = pd.read_csv(syllabus_csv_path, encoding='gbk')
unit_dict = {}
for item in list(df_unit['单元'].drop_duplicates()):
    unit_dict[item] = list(df_unit[df_unit['单元']==item]['学习活动id'])

# 单元check point
def unit_passed(user_id):
    # 输入用户id
    # 如果单元内活动用户全部都访问了 视为通过
    # 输出用户所有通过的单元
    user_activits = list(df[df['用户id']==user_id]['学习活动id'].drop_duplicates())
    passed_unit = []
    for unit in unit_dict.keys():
        if set(unit_dict[unit]).intersection(set(user_activits)) == set(unit_dict[unit]):
            passed_unit.append(unit)
    return passed_unit
def get_medal(user_id, online_gap=10, engagement_gap=30, prob_solving_gap=40, social_gap=3):
    online_result = cal_online(user_id)
    online_level = round(online_result[1] / online_gap)
    print('用户: %d 的学习持续积分为 %d ，学习持续等级为: %d' % (user_id, online_result[1], online_level))

    engagement_result = cal_engagement(user_id)
    engagement_level = round(engagement_result[1] / engagement_gap)
    print('用户: %d 的学习投入积分为 %d ，学习投入等级为: %d， 时长积分为：%d, 反复观看积分为: %d, 时长比积分为: %d' % (
    user_id, engagement_result[1], engagement_level, engagement_result[2], engagement_result[3], engagement_result[4]))

    prob_solving_result = cal_prob_solving(user_id)
    prob_solving_level = round(prob_solving_result[1] / prob_solving_gap)
    print('用户: %d 的问题解决积分为 %d ，问题解决等级为: %d， 提交积分为：%d, 停留积分为: %d' % (
    user_id, prob_solving_result[1], prob_solving_level, prob_solving_result[2], prob_solving_result[3]))

    social_result = cal_social(user_id)
    social_level = round(social_result[1] / social_gap)
    print('用户: %d 的社交协作积分为 %d ，社交协作等级为: %d， 访问积分为：%d, 停留积分为: %d' % (
    user_id, social_result[1], social_level, social_result[2], social_result[3]))

    unit_passed_result = unit_passed(user_id)

    my_medals = []
    for name, level in zip(medals.keys(), [online_level, engagement_level, prob_solving_level, social_level]):
        if name == '单元进度':
            pass
        print(name, level)
        my_medals += medals[name][:min(level - 1, len(medals[name]))]

    for item in unit_passed_result:
        my_medals.append(medals['单元进度'][item])
    print('获得的奖牌：', my_medals)
    return my_medals


def my_medals(user_id, log_data_path=course_log_data_path, course_id=373, class_id=370, syllabus_path=syllabus_csv_path):
    # 积分项
    point_table = {'学习持续': ['在线总时长(秒)'],
                   '行为投入': ['视频学习总时长(秒)', '反复观看次数', '高观看时长比'],
                   '问题解决': ['提交次数', '答题用时(秒)'],
                   '社交协作': ['评论区访问次数', '评论区停留时间(秒)']}
    # 奖章集合
    medals = {'学习持续': ['用户等级LV' + str(i) for i in range(1, 11)],
              '学习投入': ['初来乍到', '勤练基础', '勤学苦练', '挑灯夜战', '层层深入', '挑灯夜战', '柳暗花明', '渐入佳境', '持续输入', '以此为始'],
              '问题解决': ['问题解决的第一步', '反思的开始', '我学“废”了', 'debug debug debug', '轻而易举', '好、快、准！', 'Yo!答题王', '快速修正', '纠错专家',
                       '迎接新开始'],
              '社交协作': ['初次发声', '助人为乐', 'Open mind', '思维碰撞', '今日快乐', '共同体！', '听我一句', '社交达人', '谈笑风生', '大讨论家'],
              '单元进度': ['新的开始（第1单元）', '顺利入门（第2单元）', '简单运用（第3单元）', '数据结构（第4单元）', '复杂运用（第5单元）', '讲述者1（第6单元）', '讲述者2（第7单元）',
                       '小试牛刀（第8单元）', '新的领域（第9单元）', '图像识别（第10单元）', '高级运用（第11单元）']}

    df_unit = pd.read_csv(syllabus_path, encoding='gbk')
    unit_dict = {}
    for item in list(df_unit['单元'].drop_duplicates()):
        unit_dict[item] = list(df_unit[df_unit['单元'] == item]['学习活动id'])

    full_table = get_full_table(path=log_data_path, course_id=course_id, class_id=class_id)
    final_medal = get_medal(user_id=user_id)
    return final_medal

def find_userID(user_name:str)->int:
    rows=df[df['用户名']==user_name]['用户id']
    return rows.unique()[0]

# badges=my_medals(user_id=7803, log_data_path=course_log_data_path, course_id=373, class_id=370, syllabus_path=syllabus_csv_path)