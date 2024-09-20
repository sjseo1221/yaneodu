import pdb
import psycopg2
from services import ServiceMethodBase
from libs.sql_manager import execute_sql_query

class study_group(ServiceMethodBase):
    def __init__(self):
        super().__init__()


    def process(self, event): 

        params = event.get('queryStringParameters', None)
        

        query = f'''
                select b.user_id ,
                b.user_name as select, /*유저명*/
                c.comp_name as comp_name, /*추천회사*/
                a.final_meeting_group as final_meeting_group, /*소모임ID*/
                d.latitude as latitude, /*위도*/
                d.longitude as longitude, /*경도*/
                d.district as district, /*주소*/
                a.tutor_id, /*멘토ID*/
                e.user_name as user_name /*멘토명*/
                from study_group a, "user" b, company c, address d,
                     (select a.tutor_id, a.user_id, b.user_name
                        from tutor a, "user" b
                        where a.user_id = b.user_id) e
                where a.user_id = b.user_id
                and   a.comp_id = c.comp_id
                and   b.address_id = d.address_id
                and   a.tutor_id = e.tutor_id
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
#
#        response = {
#            "statusCode": 200,
#            "body": "study_group"
#        }
#
#        print(response)
#
#        return response
#
def main(event, context):
    return study_group.run(event, context)