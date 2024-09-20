import pdb
import numpy as np
import pandas as pd
import psycopg2
import datetime as dt
from config import *


def representation_score_generator(event): # 
    if event == 0:
        print('')
        pass

    if event == 1:
        '''query = 'delete from representation_score'
        with psycopg2.connect(sql_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(query)'''

        # Data Tables
        table_list = ['address','career_history','company','current_job','industry','jobgroup','tutor','user']

        # Table Column Name
        address_id_list = ['address_id','city','district','street','latitude','longitude']
        career_history_id_list = ['num_record','user_id','startdate','enddate','comp_id']
        company_id_list= ['comp_id','industry_id','comp_name']
        current_job_id_list = ['current_job_id','user_id','comp_id','jobgroup_id']
        industry_id_list = ['industry_id','industry_name']
        jobgroup_id_list = ['jobgroup_id','jobgroup_name']
        tutor_id_list = ['tutor_id','user_id','rating']
        user_id_list = ['user_id','user_name','address_id','preferred_industry_id','preferred_jobgroup_id','gender',\
            'age','univ_name','degree','toeic','GPA','certificate','working_ysno']

        str_to_var = {'address':address_id_list, 'career_history':career_history_id_list,'company':company_id_list, \
                'current_job':current_job_id_list,'industry':industry_id_list,'jobgroup':jobgroup_id_list,\
                'tutor':tutor_id_list,'user':user_id_list}
        

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
        print('** Data Loading Is Completed!**')

        address = df['address'].copy()
        career_history = df['career_history'].copy()
        company = df['company'].copy()
        current_job = df['current_job'].copy()
        industry = df['industry'].copy()
        jobgroup = df['jobgroup'].copy()
        tutor = df['tutor'].copy()
        user = df['user'].copy()

        ## GPA를 object로 끌고 오는 문제 발생하여 float로 강제 변경
        user['GPA'] = user['GPA'].astype('float')


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


        #유저 대표값 산출
        user_hired_combined = user_combined.loc[user_combined['working_ysno']=='Y']

        ## Null 값 조정 - company_id, jobgroup_id 하나라도 없으면 Row 삭제
        user_hired_combined = user_hired_combined.dropna(subset=['comp_id','jobgroup_id'], how='any')
        ## 데이터 Type에 따라 데이터 정리
        ### 명목형 데이터: gender, univ_name, degree => 최빈값
        ### 수치형 데이터: age, toeic, GPA, certificate, career -> Median 사용
        categorical_feature = ['gender','univ_name','degree']
        numerical_feature = ['age','toeic','GPA','certificate','career']

        row = []
        for i in range(4):
            for j in range(20):
                row.append([i+1, j+1])
        representation_freq_score = pd.DataFrame(row, columns=['jobgroup_id','comp_id'])


        for feature in categorical_feature:
            user_hired_feature = user_hired_combined.groupby(['jobgroup_id', 'comp_id', feature]).size()\
                                .reset_index().sort_values(by=[0],ascending=False)
            user_hired_feature.drop_duplicates(subset=['jobgroup_id','comp_id'], keep='first', inplace=True)
            user_hired_feature = user_hired_feature.iloc[:,:-1]
            representation_freq_score = pd.merge(representation_freq_score, user_hired_feature, how='left',\
                                    on=['jobgroup_id','comp_id'])

        user_hired_numerical = user_hired_combined.groupby(['jobgroup_id','comp_id'])[numerical_feature]\
                                .median().reset_index()
        representation_score = pd.merge(representation_freq_score, user_hired_numerical, how='outer',\
                                on=['jobgroup_id','comp_id'])

        representation_score = pd.merge(representation_score, company[['comp_id','industry_id']], how='left',on='comp_id')


        representation_score.dropna(how='any', inplace=True)

        representation_score['age'] = representation_score['age'].astype(int)

        conn = psycopg2.connect(sql_conn_str)
        cursor = conn.cursor()

        for i in representation_score.index:
            jobgroup_id = representation_score['jobgroup_id'][i]
            comp_id = representation_score['comp_id'][i]
            gender = representation_score['gender'][i]
            univ_name = representation_score['univ_name'][i]
            degree = representation_score['degree'][i]
            age = representation_score['age'][i]
            toeic = representation_score['toeic'][i]
            GPA = representation_score['GPA'][i]
            certificate = representation_score['certificate'][i]
            career = representation_score['career'][i]
            industry_id = representation_score['industry_id'][i]

            query = f'''
                insert into representation_score (
                    jobgroup_id, industry_id, comp_id, gender, univ_name, degree, age, toeic, GPA, certificate, career
                    )
                VALUES(
                    '{jobgroup_id}',
                    '{industry_id}',
                    '{comp_id}',
                    '{gender}', 
                    '{univ_name}',
                    '{degree}',
                    '{age}',
                    '{toeic}',
                    '{GPA}',
                    '{certificate}',
                    '{career}'
                )
                '''

            cursor.execute(query)
            conn.commit()
        conn.close()
    print('Calculation for representation score completed!!')


if __name__=='__main__':
    sql_conn_str = f'host={SQL_SERVER_ADDRESS} port={SQL_SERVER_PORT} ' \
        f'dbname={SQL_DATABASE} user={SQL_USER_ID} password={SQL_USER_PW}'

    now = dt.datetime.now().isoformat()

    print(f'[{now}] Representation Score is generating...')
    print(sql_conn_str)
    representation_score_generator(1)


