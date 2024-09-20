from services import ServiceMethodBase
from libs.sql_manager import execute_sql_query

class MentorList(ServiceMethodBase):
    def __init__(self):
        super().__init__()

    def process(self, event):
        params = event.get('queryStringParameters', None)

        query = """
            select a.tutor_id, 
                b.user_name,
                a.rating,
                d.comp_name,
                f.industry_name,
                g.jobgroup_name,
                e.district

            from tutor a, "user" b, current_job c, company d,
                address e, industry f, jobgroup g
            where a.user_id = b.user_id
            and a.user_id = c.user_id
            and c.comp_id = d.comp_id
            and b.address_id = e.address_id
            and d.industry_id = f.industry_id
            and c.jobgroup_id = g.jobgroup_id
        """

        tutor_id = ''

        if params is not None:
            tutor_id = params.get('tutor_id', None)
        else:
            tutor_id = None

        if tutor_id is not None:
            query += f'''
                and a.tutor_id = '{tutor_id}'
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
    return MentorList.run(event, context)
