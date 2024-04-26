from datetime import datetime
# from airflow import DAG 
# from airflow.operators.python import PythonOpeartor 

# default_args = {
#     'owner': 'pupurington',
#     'start_date': datetime(2024, 4, 26, 10, 10)
# }

def get_data():
    import requests
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res 

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                        f"{location['city']}, {location['state']}, {location['country']}"              
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['birthday'] = res['dob']['date']
    data['registrated_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data 


def stream_data():
    import json 
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent =3))


# with DAG('user_automation',
#          default_args = default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
    
#     streaming_task = PythonOpeartor(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data();