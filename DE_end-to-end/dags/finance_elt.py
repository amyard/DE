import os
import logging
from pendulum import datetime
from datetime import timedelta, datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Any, List, Dict, Union

from airflow.models import XCom
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.db import provide_session


dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)


AZURE_FINANCE_CONTAINER_NAME: str = os.environ.get('AZURE_FINANCE_CONTAINER_NAME')
AZURE_CONTAINER_ARCHIVE: str = os.environ.get('AZURE_CONTAINER_ARCHIVE')
AZURE_STORAGE_ACCOUNT_NAME: str = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_BLOB_STORAGE_CONN: str ='delme-storage-account'
POSTGRES_CONN_ID: str = "delme-postgresql"
POKE_INTERVAL: int = 1 * 10


@provide_session
def cleanup_xcom(task_id, session=None, **context):
    # https://stackoverflow.com/questions/46707132/how-to-delete-xcom-objects-once-the-dag-finishes-its-run-in-airflow
    dag = context["dag"]
    dag_id = dag._dag_id
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id, XCom.task_id==task_id).delete()


default_args = {
    'owner': 'delme',
    'execution_timeout':timedelta(seconds=65),
}

@dag(
    start_date=datetime(2024, 9, 3),
    schedule='@daily',
    catchup=False,
    tags=['financial-elt', 'ingestion', 'delme']
)
def finance_generate_and_upload():
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._models import PublicAccess
    from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
    from helpers.finance_elt.create_mock_data import generate_mock_data

    LOCAL_FOLDER_PATH: str = "dags/helpers/finance_elt/mock_data"

    @task
    def generate_mock_data_task() -> None:
        generate_mock_data(LOCAL_FOLDER_PATH)

    @task
    def prepare_kwargs_for_replacement_task(container_name: str) -> List[Dict[str, str]]:
        list_of_kwargs = []
        for file_name in os.listdir(LOCAL_FOLDER_PATH):
            logging.info(f"file_name: {file_name}")

            stripe_type: str = 'charge' if "charge" in file_name else 'satisfaction'
            kwarg_dict: Dict[str, str] = {
                'file_path': f"{LOCAL_FOLDER_PATH}/{file_name}",
                'blob_name': f"{stripe_type}/{file_name}"
            }
            list_of_kwargs.append(kwarg_dict)

        return list_of_kwargs

    upload_data_kwargs: List[Dict[str, str]] = prepare_kwargs_for_replacement_task(AZURE_FINANCE_CONTAINER_NAME)
    generate_mock_data_task() >> upload_data_kwargs

    @task
    def create_storage_container() -> None:
        def get_conn_string_from_conn_id(conn_id: str) -> str:
            from airflow.hooks.base import BaseHook

            connection = BaseHook.get_connection(conn_id)
            extra = connection.extra_dejson
            connection_string = extra.get('connection_string', '')
            return connection_string

        def get_connection(conn_id: str) -> Optional[BlobServiceClient]:
            try:
                blob_client: Optional[BlobServiceClient] = BlobServiceClient.from_connection_string(conn_id)
                logging.info("Azure Container Client exists")
                return blob_client
            except Exception as ex:
                logging.error(f"Connection failed: {str(ex)}")
                return None

        def create_container(blob_client: Optional[BlobServiceClient], container_name: str) -> bool:
            """
            Create container 'container_name' if not exists
            """
            try:
                containers = blob_client.list_containers()
                if container_name not in containers:
                    blob_client.create_container(container_name, public_access=PublicAccess.CONTAINER)
                    logging.info(f"Container '{container_name}' created'.")
                return True
            except Exception as ex:
                logging.error(f"Error with creating container '{container_name}': {str(ex)}")
                return False

        conn_string = get_conn_string_from_conn_id(AZURE_BLOB_STORAGE_CONN)
        blob_service_client: Optional[BlobServiceClient] = get_connection(conn_string)
        [create_container(blob_service_client, AZURE_FINANCE_CONTAINER_NAME), create_container(blob_service_client, AZURE_CONTAINER_ARCHIVE)]

        blob_service_client.close()

    upload_mock_data = LocalFilesystemToWasbOperator.partial(
        wasb_conn_id = AZURE_BLOB_STORAGE_CONN,
        task_id='upload_mock_data',
        container_name=AZURE_FINANCE_CONTAINER_NAME,
        load_options={"overwrite": True}
    ).expand_kwargs(upload_data_kwargs)

    create_storage_container() >> upload_mock_data

finance_generate_and_upload()


@dag(
    start_date=datetime(2024, 9, 4),
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    on_success_callback=cleanup_xcom,
    tags=['financial-elt', 'elt', 'delme'],
)
def finance_elt():
    @task_group(group_id='phase_1_wait_for_blobs')
    def phase_1_wait_for_blobs() -> None:
        wait_for_satisfaction=WasbPrefixSensor(
            task_id="wait_for_blobs_charge",
            wasb_conn_id=AZURE_BLOB_STORAGE_CONN,
            container_name=AZURE_FINANCE_CONTAINER_NAME,
            poke_interval=POKE_INTERVAL,
            timeout=1 * 30,
            mode='reschedule',
            prefix='charge/'
        )

        wait_for_charge=WasbPrefixSensor(
            task_id="wait_for_blobs_satisfaction",
            wasb_conn_id=AZURE_BLOB_STORAGE_CONN,
            container_name=AZURE_FINANCE_CONTAINER_NAME,
            poke_interval=POKE_INTERVAL,
            timeout=1 * 30,
            mode='reschedule',
            prefix='satisfaction/'
        )

        [wait_for_charge, wait_for_satisfaction]

    @task_group(group_id='phase_2_get_blob_names')
    def phase_2_get_blob_names():
        def get_blob_names_from_container(blob_name: str, **kwargs: Any) -> List[str]:
            hook: WasbHook = WasbHook(wasb_conn_id=AZURE_BLOB_STORAGE_CONN)
            blob_service_client = hook.blob_service_client

            if blob_service_client is None:
                raise ValueError("Blob service client is not initialized.")

            container_client = blob_service_client.get_container_client(AZURE_FINANCE_CONTAINER_NAME)
            blobs = container_client.list_blobs(name_starts_with=blob_name)
            blob_names: List[str] = [blob.name for blob in blobs]
            return blob_names

        @task(task_id="get_blob_names_for_charge")
        def get_blob_names_for_charge(blob_name: str) -> List[str]:
            return get_blob_names_from_container(blob_name)

        @task(task_id="get_blob_names_for_satisfaction")
        def get_blob_names_for_satisfaction(blob_name: str) -> List[str]:
            return get_blob_names_from_container(blob_name)

        @task(task_id="combine_blob_names")
        def combine_blob_names(charge_blob_names: List[str], satisfaction_blob_names: List[str]) -> List[str]:
            logging.info(f"Charge Blob Names: {charge_blob_names}")
            logging.info(f"Satisfaction Blob Names: {satisfaction_blob_names}")
            return charge_blob_names + satisfaction_blob_names

        # Define task dependencies
        charge_blob_names = get_blob_names_for_charge('charge/')
        satisfaction_blob_names = get_blob_names_for_satisfaction('satisfaction/')
        combined_blob_names = combine_blob_names(charge_blob_names, satisfaction_blob_names)

        return combined_blob_names

    @task_group(group_id='phase_3_create_table_if_not_exists')
    def phase_3_create_table_if_not_exists() -> None:
        create_charge_table = PostgresOperator(
            task_id=f"create_charge_table",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="sql/in_charge.sql",
            params={ 'table_name': 'in_charge' }
        )

        create_satisfaction_table = PostgresOperator(
            task_id=f"create_satisfaction_table",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="sql/customer_satisfaction.sql",
            params={'table_name': 'customer_satisfaction'}
        )

        create_model_training_table = PostgresOperator(
            task_id=f"create_model_training_table",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="sql/model_training.sql",
            params={'table_name': 'model_training'}
        )

        [create_charge_table, create_satisfaction_table, create_model_training_table]

    @task(task_id='save_data_from_storage_to_db')
    def save_data_from_storage_to_db(blob_name: Optional[str]) -> Optional[str]:
        import requests
        import tempfile
        import os

        if blob_name is None:
            return None

        table_name: str = 'in_charge' if 'charge/' in blob_name else 'customer_satisfaction'

        # Download the file from the URL
        url: str = f'https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_FINANCE_CONTAINER_NAME}/{blob_name}'
        response = requests.get(url)

        if response.status_code == 200:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name

            # Now use the local file path for the copy_expert method
            hook: PostgresHook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            hook.copy_expert(
                sql=f"COPY {table_name} FROM stdin WITH DELIMITER as ',' CSV HEADER",
                filename=temp_file_path
            )

            # Clean up the temporary file
            os.remove(temp_file_path)

            return blob_name
        else:
            raise Exception(f"Failed to download file: {response.status_code} - {response.text}")


    @task(task_id='move_blob_to_archive')
    def move_blob_to_archive(source_blob_path: str) -> None:
        hook: WasbHook = WasbHook(wasb_conn_id=AZURE_BLOB_STORAGE_CONN)
        blob_service_client = hook.blob_service_client

        source_blob = blob_service_client.get_blob_client(container=AZURE_FINANCE_CONTAINER_NAME, blob=source_blob_path)
        dest_blob = blob_service_client.get_blob_client(container=AZURE_CONTAINER_ARCHIVE, blob=f'financial_data/date_{datetime.now().strftime("%Y-%m-%d")}/{source_blob_path}')

        dest_blob.start_copy_from_url(source_blob.url, requires_sync=True)
        copy_properties = dest_blob.get_blob_properties().copy

        if copy_properties.status != "success":
            dest_blob.abort_copy(copy_properties.id)
            raise Exception(
                f"Unable to copy blob %s with status %s" % (source_blob_path, copy_properties.status)
            )
        source_blob.delete_blob()
        logging.info(f'Blob "{source_blob_path}" deleted.')

    store_calculated_data_for_ml_training = PostgresOperator(
        task_id='store_calculated_data_for_ml_training',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            WITH cte AS (
                SELECT
                    customer_id,
                    ROUND(AVG(amount_captured), 2) AS avg_amount_captured
                FROM in_charge
                WHERE status = 'succeeded' 
                    AND outcome_network_status = 'approved_by_network' 
                    AND paid = true
                GROUP BY customer_id
            )
            
            INSERT INTO model_training (
                customer_id, 
                customer_satisfaction_speed, 
                customer_satisfaction_product, 
                customer_satisfaction_service, 
                product_type, 
                avg_amount_captured
            )
            SELECT DISTINCT
                s.customer_id,
                s.customer_satisfaction_speed,
                s.customer_satisfaction_product,
                s.customer_satisfaction_service,
                s.product_type,
                c.avg_amount_captured
            FROM cte c 
            INNER JOIN customer_satisfaction s ON s.customer_id = c.customer_id
            ON CONFLICT (customer_id)
            DO UPDATE SET
                customer_satisfaction_speed = EXCLUDED.customer_satisfaction_speed,
                customer_satisfaction_product = EXCLUDED.customer_satisfaction_product,
                customer_satisfaction_service = EXCLUDED.customer_satisfaction_service,
                product_type = EXCLUDED.product_type,
                avg_amount_captured = EXCLUDED.avg_amount_captured;
        """
    )

    start = EmptyOperator(task_id='start')

    @task(task_id="finish")
    def finish():
        logging.warning(f'FINISHHHHHHHHHH ')

    get_blob_names: List[str] = phase_2_get_blob_names()
    saved_blob_ready_to_move = save_data_from_storage_to_db.partial().expand(blob_name = get_blob_names)
    move_blob_to_archive = move_blob_to_archive.partial().expand(source_blob_path = saved_blob_ready_to_move)

    start >> phase_1_wait_for_blobs() >> get_blob_names >> phase_3_create_table_if_not_exists() >> saved_blob_ready_to_move >> move_blob_to_archive >> store_calculated_data_for_ml_training >> finish()

finance_elt()

import pandas as pd
import numpy as np

def feature_data_process(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler, OneHotEncoder

    y = df["avg_amount_captured"]
    X = df.drop(columns=["avg_amount_captured"])[
        [
            "customer_satisfaction_speed",
            "customer_satisfaction_product",
            "customer_satisfaction_service",
            "product_type",
        ]
    ]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    y_train_df = pd.DataFrame({"avg_amount_captured": y_train})
    y_train_df.index = X_train.index

    y_test_df = pd.DataFrame({"avg_amount_captured": y_test})
    y_test_df.index = X_test.index

    numeric_columns = [
        "customer_satisfaction_speed",
        "customer_satisfaction_product",
        "customer_satisfaction_service",
    ]

    scaler = StandardScaler()
    X_train[numeric_columns] = scaler.fit_transform(X_train[numeric_columns])
    X_test[numeric_columns] = scaler.transform(X_test[numeric_columns])

    onehot_encoder = OneHotEncoder(sparse_output=False, drop="first")
    onehot_encoder.fit(X_train[["product_type"]])

    product_type_train = onehot_encoder.transform(X_train[["product_type"]])
    product_type_test = onehot_encoder.transform(X_test[["product_type"]])
    product_type_df_train = pd.DataFrame(
        product_type_train,
        columns=onehot_encoder.get_feature_names_out(["product_type"]),
    )
    product_type_df_test = pd.DataFrame(
        product_type_test,
        columns=onehot_encoder.get_feature_names_out(["product_type"]),
    )

    index_X_train = X_train.index
    index_X_test = X_test.index

    X_train = pd.concat(
        [X_train.reset_index(drop=True), product_type_df_train.reset_index(drop=True)],
        axis=1,
    ).drop(columns=["product_type"])

    X_test = pd.concat(
        [X_test.reset_index(drop=True), product_type_df_test.reset_index(drop=True)],
        axis=1,
    ).drop(columns=["product_type"])

    X_train.index = index_X_train
    X_test.index = index_X_test

    return {
        "X_train": X_train,
        "X_test": X_test,
        "y_train_df": y_train_df,
        "y_test_df": y_test_df,
    }

def train_model(feature_data, model_class, hyper_parameters) -> Dict[str, Union[str, float, pd.DataFrame]]:
    from sklearn.metrics import r2_score

    print(f"Training model: {model_class.__name__}")

    X_train = feature_data["X_train"]
    X_test = feature_data["X_test"]
    y_train = feature_data["y_train_df"]["avg_amount_captured"]
    y_test = feature_data["y_test_df"]["avg_amount_captured"]

    y_train = y_train.loc[X_train.index]
    y_test = y_test.loc[X_test.index]

    model = model_class(**hyper_parameters)
    model.fit(X_train, y_train)

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    r2_train = r2_score(y_train, y_pred_train)
    r2_test = r2_score(y_test, y_pred_test)

    if hasattr(model, "feature_importances_"):
        feature_imp = pd.DataFrame(
            {
                "Feature": X_train.columns,
                "Importance": model.feature_importances_,
            }
        )
        feature_imp_coef = feature_imp.sort_values(by="Importance", ascending=False)
    elif hasattr(model, "coef_"):
        feature_coef = pd.DataFrame(
            {"Feature": X_train.columns, "Coefficient": np.ravel(model.coef_)}
        )
        feature_imp_coef = feature_coef.sort_values(by="Coefficient", ascending=False)
    else:
        feature_imp_coef = None

    y_train_df = y_train.to_frame()
    y_pred_train_df = pd.DataFrame(y_pred_train, columns=["y_pred_train"])
    y_test_df = y_test.to_frame()
    y_pred_test_df = pd.DataFrame(y_pred_test, columns=["y_pred_test"])

    return {
        "model_class_name": model_class.__name__,
        "r2_train": r2_train,
        "r2_test": r2_test,
        "feature_imp_coef": feature_imp_coef,
        "y_train_df": y_train_df,
        "y_pred_train_df": y_pred_train_df,
        "y_test_df": y_test_df,
        "y_pred_test_df": y_pred_test_df,
    }


@dag(
    default_args=default_args,
    start_date=datetime(2024, 9, 4),
    schedule="@daily",
    catchup=False,
    tags=['financial-ml', 'ml', 'delme'],
)
def finance_ml():
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import LinearRegression, RidgeCV, Lasso
    import matplotlib.pyplot as plt
    import seaborn as sns

    @task
    def fetch_data_from_db() -> pd.DataFrame:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        query = "SELECT * FROM model_training;"
        df = pd.read_sql(query, conn)
        return df

    @task(task_id='feature_data_task')
    def feature_data_task(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return feature_data_process(df)

    @task(task_id='train_model_task')
    def train_model_task(feature_data: Dict[str, pd.DataFrame], model_class: Any, hyper_parameters: Dict[str, Any] = {}) -> Dict[str, Any]:
        model_results = train_model(
            feature_data=feature_data,
            model_class=model_class,
            hyper_parameters=hyper_parameters,
        )
        return model_results

    df = fetch_data_from_db()
    feature_data = feature_data_task(df)
    model_results = train_model_task.partial(
        feature_data=feature_data
    ).expand_kwargs(
        [
            {
                "model_class": RandomForestRegressor,
                "hyper_parameters": {"n_estimators": 2000},
            },
            {"model_class": LinearRegression},
            {
                "model_class": RidgeCV,
                "hyper_parameters": {"alphas": [0.1, 1.0, 10.0]},
            },
            {
                "model_class": Lasso,
                "hyper_parameters": {"alpha": 2.0},
            },
        ]
    )

    @task
    def plot_model_results(model_results: Dict[str, Any]) -> None:
        plot_dir = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags/helpers/finance_elt/plots")
        if not os.path.exists(plot_dir):
            os.makedirs(plot_dir)

        model_class_name = model_results["model_class_name"]
        y_train_df = model_results["y_train_df"]
        y_pred_train_df = model_results["y_pred_train_df"]
        y_test_df = model_results["y_test_df"]
        y_pred_test_df = model_results["y_pred_test_df"]
        r2_train = model_results["r2_train"]
        r2_test = model_results["r2_test"]

        y_train_df.reset_index(drop=True, inplace=True)
        y_pred_train_df.reset_index(drop=True, inplace=True)
        y_test_df.reset_index(drop=True, inplace=True)
        y_pred_test_df.reset_index(drop=True, inplace=True)

        test_comparison = pd.concat([y_test_df, y_pred_test_df], axis=1)
        test_comparison.columns = ["True", "Predicted"]

        train_comparison = pd.concat([y_train_df, y_pred_train_df], axis=1)
        train_comparison.columns = ["True", "Predicted"]

        sns.set_style("white")
        plt.rcParams["font.size"] = 12

        fig, axes = plt.subplots(1, 2, figsize=(14, 7))

        sns.scatterplot(
            ax=axes[0],
            x="True",
            y="Predicted",
            data=train_comparison,
            color="black",
            marker="x",
        )
        axes[0].plot(
            [train_comparison["True"].min(), train_comparison["True"].max()],
            [train_comparison["True"].min(), train_comparison["True"].max()],
            "--",
            linewidth=1,
            color="red",
        )
        axes[0].grid(True, linestyle="--", linewidth=0.5)
        axes[0].set_title(f"Train Set: {model_class_name}")
        axes[0].text(0.1, 0.9, f"R2: {r2_train}", transform=axes[0].transAxes)

        sns.scatterplot(
            ax=axes[1],
            x="True",
            y="Predicted",
            data=test_comparison,
            color="black",
            marker="x",
        )
        axes[1].plot(
            [test_comparison["True"].min(), test_comparison["True"].max()],
            [test_comparison["True"].min(), test_comparison["True"].max()],
            "--",
            linewidth=1,
            color="red",
        )
        axes[1].grid(True, linestyle="--", linewidth=0.5)
        axes[1].set_title(f"Test Set: {model_class_name}")
        axes[1].text(0.1, 0.9, f"R2: {r2_test}", transform=axes[1].transAxes)

        fig.suptitle("Predicted vs True Values", fontsize=16)

        file_path = os.path.join(plot_dir, f"{model_class_name}_plot_results.png")
        plt.tight_layout()
        plt.savefig(file_path)
        logging.info(f"Plot results saved to {file_path}")


    plot_model_results.expand(model_results=model_results)


    feature_data_task

finance_ml()