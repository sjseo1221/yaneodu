import pdb
import psycopg2
#from config import *
from services import ServiceMethodBase
from libs.sql_manager import execute_sql_query

class score_list(ServiceMethodBase):
    def __init__(self):
        super().__init__()


    def process(self, event): 

        params = event.get('queryStringParameters', None)

        query = '''
                select a.user_id as user_id, /*유저ID*/
                b.user_name as user_name, /*유저명*/
                c.comp_name as comp_name, /*추천회사*/
                a.rank as rank /*추천등급*/
                from rank_list a, "user" b, company c
                where a.user_id = b.user_id
                and a.comp_id = c.comp_id
        '''

        user_id = ''

        if params is not None:
            user_id = params.get('user_id', None)
        else:
            user_id = None

        if user_id is not None:
            query += f'''
                and a.user_id = '{user_id}'
            '''

        results = execute_sql_query(query)


        return results

def main(event, context):
    return score_list.run(event, context)