import mysql.connector
from flask import Flask, request
import numpy as np
import pandas as pd
import sklearn.metrics 

app = Flask(__name__)

job_columns = ['id',
 'latitude',
 'longitude',
 'job_name',
 'job_description',
 'intersts',
 'distance_from_central']

cdt_columns = ['name', 'id', 'degree', 'field_of_study', 'university', 'graduation_year', 'courses',
           'health_history', 'intersts', 'car', 'house', 'job', 'city', 'country', 'date_of_birth', 
           'family_members', 'company_coords']

jobs = pd.read_csv('job_opp.csv')

@app.route('/', methods=['GET'])
def return_data():
    
    id = request.args.get('id')
    cnx = mysql.connector.connect(
                    host="rm-l4vtsmuu203976dh4qo.mysql.me-central-1.rds.aliyuncs.com",
                    user="admin_account",
                    password="Admin@2023",
                    database="mysql"
            )
    cursor = cnx.cursor()

    # query = f"SELECT * FROM new_schema.apartment_data"
    # cursor.execute(query)
    # jobs = cursor.fetchall()
    # jobs = dict(zip(job_columns, np.array(jobs).T))
    # jobs = pd.DataFrame(data=jobs)
    # jobs = jobs[jobs['availability'] == 'available']

    query = f"SELECT * FROM new_schema.data WHERE id = \"{id}\""
    cursor.execute(query)
    cdt = cursor.fetchall()
    cdt = dict(zip(cdt_columns, np.array(cdt).T))
    cdt = pd.DataFrame(data=cdt).iloc[0]

    # split the coords into two
    cdt['latitute'] = float(cdt['company_coords'].split(',')[0].replace('[', ''))
    cdt['longitude'] = float(cdt['company_coords'].split(',')[1].replace(']', ''))
    cdt.drop('company_coords', inplace=True)
    cdt_interests = np.array([v for k,v in eval(cdt['intersts']).items()])
    jobs_values =jobs['intersts'].apply(lambda x: np.array([v for k,v in eval(x).items()])).to_numpy()
    jobs_values = [i - cdt_interests for i in jobs_values]
    dot_jobs_cdt = np.linalg.norm(jobs_values, axis=0)
    sorted_jobs = np.argsort(dot_jobs_cdt)[::5]
    jobs.iloc[sorted_jobs]
    return jobs.iloc[sorted_jobs].to_dict(orient='records')
    
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=80, debug=True)