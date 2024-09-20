import math
from tqdm import tqdm
import numpy as np
import pandas as pd
from pandas.core.algorithms import rank
from sklearn import cluster
from sklearn.cluster import KMeans, DBSCAN
from sklearn import preprocessing
import datetime as dt
import psycopg2
from config import *
import decimal


def select_optimal_tutor(tutors, center_point):
    distance = []
    for idx, tutor in tutors.iterrows():
        
        #print( f'latitude : \n{tutor['latitude']}'); #decimal.Decimal
        #print( f'center_point : \n{decimal.Decimal(center_point[0])}'); #numpy.float64
        #print( f'tlongitudetor : \n{(tutor['longitude']-decimal.Decimal(center_point[1]))**2}')

        #print( f'distance : \n{distance}')
        #print( f'sqrt : \n{math.sqrt( (tutor['latitude']-decimal.Decimal(center_point[0]))**2 + (tutor['longitude']-decimal.Decimal(center_point[1]))**2 )}')

        #distance.append(math.sqrt( (tutor['latitude']-center_point[0])**2 + (tutor['longitude']-center_point[1])**2 ))
        distance.append(math.sqrt( (tutor['latitude']-decimal.Decimal(center_point[0]))**2 + (tutor['longitude']-decimal.Decimal(center_point[1]))**2 ))

    tutors['distance'] = distance
    tutors['normed_distance'] = 1 - (tutors['distance'] - tutors['distance'].min()) / (tutors['distance'].max() - tutors['distance'].min())
    tutors.loc[tutors['normed_distance'].isna(), 'normed_distance'] = 0

    tutors['normed_rating'] = (tutors['rating'] - tutors['rating'].min()) / (tutors['rating'].max() - tutors['rating'].min())

    tutors['total_score'] = (tutors['normed_distance'] + tutors['normed_rating'] ) / 2

    tutors = tutors.sort_values(by=['total_score'], ascending=False)

    return tutors.iloc[0]

def clustering_study_group(event): 
    if event == 0:
        print('')
        pass

    if event == 1:
        query = 'delete from study_group'
        with psycopg2.connect(sql_conn_str) as conn:
            cur = conn.cursor()
            cur.execute(query)

    # Data Tables
    table_list = ['user','rank_list', 'address','tutor','current_job','company']

    # Table Column Name
    user_id_list = ['user_id','user_name','address_id','preferred_industry_id','preferred_jobgroup_id','gender',\
        'age','univ_name','degree','toeic','GPA','certificate','working_ysno']
    rank_id_list = ['user_id','comp_id','score','rank']
    address_id_list = ['address_id','city','district','street','latitude','longitude']
    tutor_id_list = ['tutor_id','user_id','rating']
    current_job_id_list = ['current_job_id','user_id','comp_id','jobgroup_id']
    company_id_list= ['comp_id','industry_id','comp_name']


    str_to_var = {'user':user_id_list, 'rank_list':rank_id_list, 'address':address_id_list,\
                    'tutor':tutor_id_list, 'current_job':current_job_id_list, 'company':company_id_list}
            


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
    rank_list = df['rank_list'].copy()
    address = df['address'].copy()
    tutor = df['tutor'].copy()
    current_job = df['current_job'].copy()
    company = df['company'].copy()

    tutor['rating'] = tutor['rating'].astype('float')


    # print( f'user : {user}');
    # print( f'rank_list : {rank_list}');
    # print( f'address : {address}');
    # print( f'tutor : {tutor}');
    # print( f'current_job : {current_job}');
    # print( f'company : {company}');##


    ## rank_list에는 구직자만 있기 때문에 working_ysno가 필요없지만 안전장치로 불러옴
    user_not_hired = user.loc[user['working_ysno']=='N'][['user_id','address_id']]

    user_tutor = user.drop(columns=['preferred_industry_id','preferred_jobgroup_id', 'working_ysno'])

    # print( f'rank_list : \n{rank_list}');
    # print( f'user_not_hired : \n{user_not_hired}');

    user_address = pd.merge(rank_list, user_not_hired, how='left', on='user_id' )

    # print( f'user_address : \n{user_address}');
    # print( f'address : \n{address}');

    user_address_lat_long = pd.merge(user_address, address, how='left', on='address_id')

    ## highest recommendation sorting
    highest_rank = 5
    # print( f'user_address_lat_long : \n{user_address_lat_long}');
    user_ranked = user_address_lat_long[user_address_lat_long['rank']== highest_rank]


    target_company = [i for i in range(13,14)]

    study_recommendation_by_comp = []
    not_serviced = []
    clustering_labels =[]
    #print( f'target_company : {target_company}'); 1~20까지
    for i in target_company:
        print(f'Study group for comp_id = {i} is generating')
        # print( f'user_ranked : {user_ranked}');
        target_user = user_ranked[user_ranked['comp_id'] == i].copy()
        print( f'target_user33 : \n{target_user}'); #해당 회사를 지망하는 타겟 유저들
        target_user_position = target_user[['latitude', 'longitude']] #타겟 유저들의 위치
        ## 1차 Clustering with DBSCAN
        scaler = preprocessing.MinMaxScaler()
        # print( f'scaler : {scaler}');
        print( f'target_user_position : {target_user_position}');
        target_user_position_for_clustering = scaler.fit_transform(target_user_position)


        model = DBSCAN(eps=0.001, metric='cosine',min_samples=3)
        clustering = model.fit(target_user_position_for_clustering) #클러스터링 타겟 데이터
        print( f'clustering : \n{clustering}'); 

        clustering_result = clustering.labels_ + 1

        print( f'clustering_labels_ : \n{clustering.labels_}'); 
        
        target_user['meeting_group'] = clustering_result
        target_user_not_serviced = target_user.loc[target_user['meeting_group']==0]
        
        target_user = target_user.loc[target_user['meeting_group']!=0]
        print( f'target_user44 : \n{target_user}');
        # print( f'i : {i}');
        # print( f'target_user55 : \n{target_user}');
        
        clustering_labels.append(clustering_result)
        not_serviced.append(target_user_not_serviced)
        # Cluster 순서 변경    
        print( f'value_counts : \n{target_user['meeting_group'].value_counts()}');
        order = pd.DataFrame(target_user['meeting_group'].value_counts())
        order_sorted = order.sort_values(by='meeting_group')
        order_num = len(order) + 1 
        print( f'order_num : \n{order_num}');

        #??????s
        order_sorted['adjusted_group'] = [j for j in range(1,order_num)]
        group_map = dict(zip(order_sorted.index, order_sorted['adjusted_group']))
        target_user['meeting_group'] = target_user['meeting_group'].map(group_map)
        print( f'target_user22 : \n{target_user}')
        ## 2차 Clustering with K-means

        max_clustering = clustering_result.max()
        # print( f'clustering_result_max : \n{clustering_result.max()}')
        # print( f'clustering_result88 : \n{clustering_result}')
        print( f'max_clustering_1 : \n{max_clustering+1}')
        
        n_ideal_user_per_meeting = 5
        current_cluster_num = 1

        clustering_result = pd.DataFrame({})

        print( f'clustering_result : \n{clustering_result}');
        # print( f'clustering_result13 : \n{clustering_result[clustering_result['user_id']==13]}');

        for j in range(1, max_clustering+1):
            #print('탔니')
            target_user_for_clustering = target_user[target_user['meeting_group'] == j][['latitude', 'longitude', 'score',  'user_id']].copy()
            n_target_user = len(target_user_for_clustering)
            print( f'n_target_user : \n{n_target_user}');
            if n_target_user <=n_ideal_user_per_meeting:
                cluster_ids = [0 for j in range(n_target_user)]
                target_user_for_clustering['final_meeting_group'] = cluster_ids
                target_user_for_clustering['final_meeting_group'] = target_user_for_clustering['final_meeting_group'] + current_cluster_num
                current_cluster_num += 1

                #clustering_result = clustering_result.append(target_user_for_clustering[['final_meeting_group','user_id']])
                clustering_result = pd.concat([clustering_result, target_user_for_clustering[['final_meeting_group','user_id']]], ignore_index=True)

                continue
            
            else:
                n_cluster = math.floor(n_target_user / n_ideal_user_per_meeting)

                target_user_features_for_clustering = scaler.fit_transform(target_user_for_clustering[['latitude', 'longitude','score']])

                model_kmean = KMeans(n_clusters=n_cluster, init='k-means++', random_state=12)
                cluster_ids = model_kmean.fit_predict(target_user_features_for_clustering)
                print( f'cluster_ids : \n{cluster_ids}');
                target_user_for_clustering['final_meeting_group'] = cluster_ids + current_cluster_num
                current_cluster_num += (cluster_ids.max() + 1)

                #clustering_result = clustering_result.append(target_user_for_clustering[['final_meeting_group','user_id']])      
                clustering_result = pd.concat([clustering_result, target_user_for_clustering[['final_meeting_group','user_id']]], ignore_index=True)  
                print( f'clustering_result22 : \n{clustering_result}')


        
        print( f'target_user 전: \n{target_user}')

        if target_user.empty==False : target_user = pd.merge(target_user, clustering_result, on='user_id')

        print( f'target_user 후: \n{target_user}')

        user_for_tutor = pd.merge(tutor, current_job[['user_id','comp_id']], how='left', on='user_id')
        user_for_tutor = pd.merge(user_for_tutor, user_tutor, how='left', on='user_id')
        user_for_tutor = pd.merge(user_for_tutor, address[['address_id','latitude','longitude']], how='left', on='address_id')

        tutor_for_meeting = user_for_tutor[user_for_tutor['comp_id']== i].copy()

        if target_user.empty==False :
            user_cluster_tutor = []
            for j in range(1, target_user['final_meeting_group'].max()+1):
                target_user_group = target_user[target_user['final_meeting_group']==j].copy()

                user_location = target_user_group[['latitude','longitude']]
                center_point = [user_location['latitude'].mean(), user_location['longitude'].mean()]

                select_tutor = select_optimal_tutor(tutor_for_meeting, center_point)
                target_user_group['tutor_id'] = select_tutor['tutor_id']
                user_cluster_tutor.append(target_user_group)
            cluster_final = pd.concat(user_cluster_tutor, axis=0)
     
    #    study_recommendation_sub = cluster_final[['user_id','comp_id','score','address_id','city','district',\
    #                                            'latitude','longitude','final_meeting_group','tutor_id']]
        study_recommendation_sub = cluster_final[['user_id','comp_id','meeting_group','final_meeting_group','tutor_id']]

        study_recommendation_by_comp.append(study_recommendation_sub)
        print(f'Study group for {i} is just generated!!')
        # print( f'study_recommendation_sub13 : \n{study_recommendation_sub[study_recommendation_sub['user_id']==13]}');

    print( f'clustering_labels : \n{clustering_labels}');
    

    study_group = pd.concat(study_recommendation_by_comp, axis=0)
    study_group = study_group.sort_values(by=['user_id'])
    study_group = study_group.reset_index(drop=True)
    user_not_serviced = pd.concat(not_serviced, axis=0)

    #clustering_labels
    s = len(study_group)
    n = len(user_not_serviced)

    print( f'study_group len : \n{len(study_group)}');

    print('All study groups are formed!')

    conn = psycopg2.connect(sql_conn_str)
    cursor = conn.cursor()
    print( f'study_group22 : \n{study_group['user_id']}');
    print( f'study_group13 : \n{study_group[study_group['user_id']==13]}');
    


    for i in tqdm(study_group.index):
        user_id = study_group['user_id'][i]
        meeting_group = study_group['meeting_group'][i]
        final_meeting_group = study_group['final_meeting_group'][i]
        tutor_id = study_group['tutor_id'][i]
        comp_id = study_group['comp_id'][i]


        query = f'''
            insert into study_group (
                user_id, meeting_group, final_meeting_group, tutor_id, comp_id)
            VALUES(
                '{user_id}',
                '{meeting_group}',
                '{final_meeting_group}',
                '{tutor_id}',
                '{comp_id}'
            )
            '''
        cursor.execute(query)
        conn.commit()
    conn.close()
    print('Upload is completed!!')
    print(f'Total {s} people are allocated to study group.')
    print(f'For {n} people, services are not available due to long distance.')
    print('Study hard for better life!')
    print('Good luck')


##################################################################################################
if __name__=='__main__':
    sql_conn_str = f'host={SQL_SERVER_ADDRESS} port={SQL_SERVER_PORT} ' \
                f'dbname={SQL_DATABASE} user={SQL_USER_ID} password={SQL_USER_PW}'


    now = dt.datetime.now().isoformat()

    print(f'[{now}] Uploading study group to servier...')
    clustering_study_group(1)

