from services import ServiceMethodBase
from libs.sql_manager import execute_sql_query

class UserList(ServiceMethodBase):
    def __init__(self):
        super().__init__()

    def process(self, event):
        params = event.get('queryStringParameters', None)

        query = """
            select
            a.user_id as user_id, /*유저ID*/
            a.user_name as user_name, /*유저명*/
            b.district as address_id, /*주소*/
            c.industry_name as preferred_industry_id, /*희망산업*/
            d.jobgroup_name as preferred_jobgroup_id, /*희망직군*/
            case
                when a.gender = 'F' then '여'
                when a.gender = 'M' then '남'
            end as gender, /*성별*/
            a.age as age, /*나이*/
            a.univ_name as univ_name, /*졸업학교*/
            a.degree as degree, /*학위*/
            a.toeic as toeic, /*토익점수*/
            a."GPA" as gpa, /*학점*/
            a.certificate as certificate, /*자격증수*/
            a.working_ysno as working_ysno /*경력여부*/
            from "user" a, address b, industry c, jobgroup d
            where  a.address_id = b.address_id
            and a.preferred_industry_id = c.industry_id
            and a.preferred_jobgroup_id = d.jobgroup_id
        """
        
        user_id = ''

        if params is not None:
            user_id = params.get('user_id', None)
        else:
            user_id = None

        if user_id is not None:
            query += f'''
                and a.user_id = '{user_id}'
            '''

        query += f'''
            fetch first 100 rows only
        ''' 

        result = execute_sql_query(query)

        return result

        #response = {
        #    "statusCode": 200,
        #    "body": "user_list"
        #}

        #print(response)

        #return response

def main(event, context):
    return UserList.run(event, context)
