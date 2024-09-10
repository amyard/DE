import airflow
import airflow.utils.dates
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.db import provide_session
from airflow.utils.task_group import TaskGroup
from airflow.models import XCom
from airflow import DAG

import pathlib
import requests
import glob
import os

HEADER = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0'}
LINK = 'https://unsplash.com/napi/search/photos?query={0}&per_page=1&page=1&xp='


@provide_session
def cleanup_xcom(task_id, session=None, **context):
    # https://stackoverflow.com/questions/46707132/how-to-delete-xcom-objects-once-the-dag-finishes-its-run-in-airflow
    dag = context["dag"]
    dag_id = dag._dag_id
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id, XCom.task_id == task_id).delete()


dag = DAG(
    dag_id="project_unsplash",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    max_active_runs=1,
    on_success_callback=cleanup_xcom,
    template_searchpath="/tmp"  # defines where to look for the templates
)


def _get_connection(my_query, **context):
    pathlib.Path(f"./tmp/images/{my_query}/").mkdir(parents=True, exist_ok=True)
    query_link = LINK.format(my_query)
    # print(query_link)
    try:
        response = requests.get(query_link, headers=HEADER)
    except:
        response.raise_for_status()
        print("There was a connection error while retrieving the data.")
    json_response = response.json()
    results = json_response['results'][0]
    context["task_instance"].xcom_push(key="json_response", value=results)


def _get_metadata(my_query, **context):
    response = context["task_instance"].xcom_pull(
        task_ids=f"get_connection_{my_query}", key="json_response"
    )
    response_id = response['id']
    image_url = response['urls']['raw']

    user = response['user']
    username = user['username']
    name = user['name']
    portfolio_url = user['portfolio_url']
    total_likes = user['total_likes']
    total_photos = user['total_photos']

    with open(f"/tmp/test_schema_{my_query}.sql", "w") as f:
        f.write(
            f"INSERT INTO {my_query} VALUES ("
            f"'{response_id}', '{username}', '{name}', '{portfolio_url}', '{image_url}', '{total_likes}', '{total_photos}'"
            ");\n"
        )


def _fetch_image(my_query, **context):
    response = context["task_instance"].xcom_pull(
        task_ids=f"get_connection_{my_query}", key="json_response"
    )
    response_id = response['id']
    image_url = response['urls']['raw']

    timestamp = response_id
    target_file = f"./tmp/images/{my_query}_{timestamp}.jpg"
    with open(target_file, "wb") as f:
        try:
            image_response = requests.get(image_url, stream=True)
            f.write(image_response.content)
        except:
            print("There was a connection error while retrieveing the image.")
    print(f"Downloaded {image_url} to {target_file}")


def _blend_images(my_query):
    pathlib.Path(f"./tmp/images/blended/").mkdir(parents=True, exist_ok=True)
    query1, query2 = my_query
    first_images = glob.glob(f'./tmp/images/{query1}/*.jpg')
    second_images = glob.glob(f'./tmp/images/{query2}/*.jpg')

    latest_first_image = max(first_images, key=os.path.getctime)
    latest_second_image = max(second_images, key=os.path.getctime)
    print(latest_first_image)
    print(latest_second_image)

    # img1 = cv2.imread(latest_first_image)
    # img2 = cv2.imread(latest_second_image)
    #
    # img1 = cv2.resize(img1, (600, 600))
    # img2 = cv2.resize(img2, (600, 600))
    #
    # alpha = 0.3
    # blended_img = cv2.addWeighted(img1, alpha, img2, 1 - alpha, 0.0)
    #
    # img1_id = latest_first_image.split('.')[1].split('_')[1]
    # img2_id = latest_second_image.split('.')[1].split('_')[1]
    # image_path = f'./tmp/images/blended/blended_{img1_id}_{img2_id}.jpg'
    # cv2.imwrite(image_path, blended_img)


def generate_tasks(my_query, dag=dag):
    get_connection = PythonOperator(
        task_id=f"get_connection_{my_query}",
        python_callable=_get_connection,
        op_kwargs={'my_query': my_query},
        dag=dag,
    )

    create_pet_table = PostgresOperator(
        task_id=f"create_table_{my_query}",
        postgres_conn_id="my_postgres",
        params={"tablename": f"{my_query}"},
        sql="sql/test_schema.sql",
    )

    fetch_metadata = PythonOperator(
        task_id=f"fetch_metadata_{my_query}",
        python_callable=_get_metadata,
        op_kwargs={'my_query': my_query},
        dag=dag,
    )

    fetch_image = PythonOperator(
        task_id=f"fetch_image_{my_query}",
        python_callable=_fetch_image,
        op_kwargs={'my_query': my_query},
        dag=dag,
    )

    write_to_postgres = PostgresOperator(
        task_id=f"write_to_postgres_{my_query}",
        postgres_conn_id="my_postgres",
        # params={"tablename": f"{my_query}"},
        sql=f"test_schema_{my_query}.sql",
        dag=dag,
    )

    clean_xcom = PythonOperator(
        task_id=f"clean_xcom",
        python_callable=cleanup_xcom,
        op_kwargs={'my_query': f"fetch_metadata_{my_query}"},
        provide_context=True,
        # dag=dag
    )

    get_connection >> create_pet_table >> fetch_metadata >> write_to_postgres >> fetch_image >> clean_xcom
    return get_connection, fetch_image


dummy_start = DummyOperator(
    task_id='start',
    dag=dag
)

my_query = ['lion', 'africa']
blend_images = PythonOperator(
    task_id="blend_images",
    python_callable=_blend_images,
    op_kwargs={'my_query': my_query},
    dag=dag
)
for query in my_query:
    with TaskGroup(query, tooltip=f"Tasks for processing {query}", dag=dag):
        a, b = generate_tasks(my_query=query, dag=dag)
    dummy_start >> a
    b >> blend_images