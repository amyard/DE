import os
import logging
from datetime import timedelta
from pendulum import datetime
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, RidgeCV, Lasso
import numpy as np
from typing import Dict, Any, Union

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID: str = "delme-postgresql"


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

    onehot_encoder = OneHotEncoder(sparse=False, drop="first")
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


default_args = {
    'owner': 'me',
    'execution_timeout':timedelta(seconds=65),
}
@dag(
    default_args=default_args,
    start_date=datetime(2024, 9, 4),
    schedule="@daily",
    catchup=False,
    tags=['financial-ml', 'ml', 'delme'],
)
def finance_ml():
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
        import matplotlib.pyplot as plt
        import seaborn as sns

        plot_dir = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags/helpers/plots")
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