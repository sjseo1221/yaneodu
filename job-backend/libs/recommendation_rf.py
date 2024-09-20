import numpy as np
import pandas as pd
from tqdm import tqdm
#from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
#from sklearn.neighbors import KNeighborsClassifier
#from sklearn.model_selection import train_test_split
#from sklearn.metrics import accuracy_score

import datetime as dt
import psycopg2
from config import *
#from services import ServiceMethodBase
#from libs.sql_manager import execute_sql_query


def recommendation_rf(event): # 
    if event == 0:
        print('')
        pass

    if event == 1:
        query = 'delete from rank_list'
        with psycopg2.connect(sql_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(query)



    # Data Tables
    table_list = ['user','career_history','current_job','company']

    # Table Column Name
    user_id_list = ['user_id','user_name','address_id','preferred_industry_id','preferred_jobgroup_id','gender',\
        'age','univ_name','degree','toeic','GPA','certificate','working_ysno']
    career_history_id_list = ['num_record','user_id','startdate','enddate','comp_id']
    current_job_id_list = ['current_job_id','user_id','comp_id','jobgroup_id']
    company_id_list= ['comp_id','industry_id','comp_name']


    str_to_var = {'user':user_id_list, 'career_history':career_history_id_list,\
            'current_job':current_job_id_list, 'company':company_id_list}
            

    # Data Loading
    ## user의 경우 user로 불러올 경우 안 불러짐. "user"로 불러야 불러짐
    df={}
    for i in table_list:    
        query = f'''
                select * from "{i}"
            '''
        with psycopg2.connect(sql_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
        
        df[i] = pd.DataFrame(rows, columns=str_to_var[i])
        print( f'{i} is loaded')
    print('** Data Loading is completed!**')

    user = df['user'].copy()
    career_history = df['career_history'].copy()
    current_job = df['current_job'].copy()
    company = df['company'].copy() 


    ## GPA를 object로 끌고 오는 문제 발생하여 float로 강제 변경
    user['GPA'] = user['GPA'].astype('float')

    ## Categorical Data Label Encoding
    gender_map = {'F':0, 'M':1}
    univ_map = {'KAIST':0, '서울대학교':1, '고려대학교':2, '연세대학교':3}
    degree_map = {'대졸':0, '석사':1, '박사':2}

    user['gender'] = user['gender'].map(gender_map)
    user['univ_name'] = user['univ_name'].map(univ_map)
    user['degree'] = user['degree'].map(degree_map)

    # 경력 산출
    career_history['career'] = (pd.to_datetime(career_history['enddate']) \
                                        - pd.to_datetime(career_history['startdate']))
    career_history['career'] = (career_history['career'] / pd.Timedelta(1, unit='d'))/365

    # 경력 합산
    career_history_sum = career_history.groupby('user_id')['career'].sum().reset_index()
    career_history_sum = career_history_sum.round({'career':1})

    # user와 career_history join
    user_career = pd.merge(user, career_history_sum, how='left', on='user_id')

    # user_career와 current_job join
    current_job = current_job.drop(columns=['current_job_id'])
    user_combined = pd.merge(user_career, current_job, how='left', on='user_id')
    # user_combined와 company join
    user_combined = pd.merge(user_combined, company[['comp_id','industry_id']], how='left', on='comp_id')

    #경력 유무에 따른 sorting
    user_hired = user_combined.loc[user_combined['working_ysno']=='Y'].copy()
    user_not_hired = user_combined.loc[user_combined['working_ysno']=='N'].copy()
    user_not_hired = user_not_hired.sort_values(by='user_id').reset_index(drop=True)

    ## Null 값 조정 - company_id, jobgroup_id 하나라도 없으면 Row 삭제
    user_hired = user_hired.dropna(subset=['comp_id','jobgroup_id'], how='any')

    ## Column 순서 정리
    user_hired = user_hired[['gender', 'age', 'univ_name', 'degree','toeic', 'GPA', \
                            'certificate', 'career', 'jobgroup_id', 'industry_id','comp_id']].copy()
    user_not_hired_id = user_not_hired[['user_id']]
    user_not_hired = user_not_hired[['gender', 'age', 'univ_name', 'degree','toeic', 'GPA', \
                            'certificate', 'career', 'preferred_jobgroup_id', 'preferred_industry_id','comp_id']].copy()
    # Column 명 변경 Preferred_industry_id, preferred_jobgroup_id => preferred 제거
    user_not_hired.rename(columns={'preferred_jobgroup_id':'jobgroup_id','preferred_industry_id':'industry_id'}, inplace=True)

    x = user_hired.drop(columns=['comp_id'],axis=1)
    y = user_hired['comp_id']

    x_test = user_not_hired.drop(columns=['comp_id'], axis=1)

    # 더미 인풋이라 숫자가 안 나와서 tree 숫자 올려놓음
    ## 실제 데이터에서는 500개 까지 필요없을 것
    rfc = RandomForestClassifier(n_estimators=500)
    rfc.fit(x, y)
    pred_rfc = rfc.predict(x_test)
    y_pred_proba = rfc.predict_proba(x_test)

    prob = pd.DataFrame(y_pred_proba, columns=(range(1,21)))
    prob_stacked = prob.stack().reset_index()
    prob_stacked.sort_values(by=['level_0',0], ascending=[True, False], inplace=True)
    
    prob_rank = []
    rank = []
    for i in prob_stacked['level_0'].drop_duplicates():
        prob_rank.append(prob_stacked.loc[prob_stacked['level_0']==i].head(5))    
    recommendation_final = pd.concat(prob_rank)
    recommendation_final = pd.merge(recommendation_final, user_not_hired_id, how='left', left_on='level_0',right_index=True)
    recommendation_final.drop(columns='level_0', inplace=True)
    recommendation_final.rename(columns={'level_1':'comp_id',0:'score'}, inplace=True)
    recommendation_final['rank'] = recommendation_final.groupby('user_id')['score'].rank(method='first')

    recommendation_final['rank'] = recommendation_final['rank'].astype(int)
    recommendation_final = recommendation_final[['user_id','comp_id','score','rank']]

    conn = psycopg2.connect(sql_conn_str)
    cursor = conn.cursor()

    for i in tqdm(recommendation_final.index):
        user_id = recommendation_final['user_id'][i]
        comp_id = recommendation_final['comp_id'][i]
        score = recommendation_final['score'][i]
        rank = recommendation_final['rank'][i]

        query = f'''
            insert into rank_list (
                user_id, comp_id, score, rank)
            VALUES(
                '{user_id}',
                '{comp_id}',
                '{score}',
                '{rank}'
            )
            '''
        cursor.execute(query)
        conn.commit()
    conn.close()
    print('Job recommendation ranks are generated!! ')

##################################################################################################
if __name__=='__main__':
    sql_conn_str = f'host={SQL_SERVER_ADDRESS} port={SQL_SERVER_PORT} ' \
                f'dbname={SQL_DATABASE} user={SQL_USER_ID} password={SQL_USER_PW}'


    now = dt.datetime.now().isoformat()

    print(f'[{now}] Job recommendation ranks are now generated...')
    recommendation_rf(1)
    
