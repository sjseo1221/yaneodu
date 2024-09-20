import pdb
import psycopg2
#from config import *
from services import ServiceMethodBase
from libs.sql_manager import execute_sql_query

class company_prefer_list(ServiceMethodBase):
    def __init__(self):
        super().__init__()


    def process(self, event): 
        query = '''
                select d.jobgroup_name as jobgroup_id, /*직군명*/
                    c.industry_name as industry_id, /*산업명*/
                    b.comp_name as comp_id, /*회사명*/
                    case
                        when a.gender = 'F' then '여' 
                        when a.gender = 'M' then '남'
                    end as gender, /*성별*/
                    a.univ_name as univ_name, /*졸업학교*/
                    a.degree as degree, /*학위*/
                    a.age as age, /*나이*/
                    a.toeic as toeic, /*토익점수*/
                    left(text(a.GPA), 5) as gpa, /*학점*/
                    a.certificate as certificate, /*자격증수*/
                    left(text(a.career), 5) as career /*경력*/
                from representation_score a, company b, industry c, jobgroup d
                where a.comp_id = b.comp_id
                and a.industry_id = c.industry_id
                and a.jobgroup_id = d.jobgroup_id
            '''
        results = execute_sql_query(query)

    

        return results

def main(event, context):
    return company_prefer_list.run(event, context)