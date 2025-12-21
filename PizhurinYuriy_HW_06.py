import os
import csv
import logging
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


def write_csv(path: str, header: list, rows: list):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


SQL_TOP3_MIN = """
WITH totals AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0)::numeric(14,2) AS total_amount
    FROM public.customer c
    LEFT JOIN public.orders o
        ON o.customer_id = c.customer_id
    LEFT JOIN public.order_items oi
        ON oi.order_id = o.order_id
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT first_name, last_name, total_amount
FROM totals
ORDER BY total_amount ASC, customer_id ASC
LIMIT 3;
"""


def export_top3_min_to_file(**context):
    hook = PostgresHook(postgres_conn_id="homework_06")
    rows = hook.get_records(SQL_TOP3_MIN)
    out_path = os.path.join("/opt/airflow/data_files", "top3_min_total.csv")
    write_csv(out_path, ["first_name", "last_name", "total_amount"], rows)
    logger.info(f"Saved TOP-3 MIN to {out_path}, rows={len(rows)}")


SQL_TOP3_MAX = """
WITH totals AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0)::numeric(14,2) AS total_amount
    FROM public.customer c
    LEFT JOIN public.orders o
        ON o.customer_id = c.customer_id
    LEFT JOIN public.order_items oi
        ON oi.order_id = o.order_id
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT first_name, last_name, total_amount
FROM totals
ORDER BY total_amount DESC, customer_id ASC
LIMIT 3;
"""


def export_top3_max_to_file(**context):
    hook = PostgresHook(postgres_conn_id="homework_06")
    rows = hook.get_records(SQL_TOP3_MAX)
    out_path = os.path.join("/opt/airflow/data_files", "top3_max_total.csv")
    write_csv(out_path, ["first_name", "last_name", "total_amount"], rows)
    logger.info(f"Saved TOP-3 MAX to {out_path}, rows={len(rows)}")


def on_failure_callback(context):    
    error_message = f"""
    Задача завершилась с ошибкой!
    """
    logger.error(error_message)


def on_success_callback(context):    
    success_message = f"""
    Задача выполенеа успешно!
    """
    logger.info(success_message)


def count_rows_from_hook(**context):

    hook = PostgresHook(postgres_conn_id="homework_06")
    stats = hook.get_records("""
        SELECT COUNT(*) as all_rows
        FROM product;
    """)
    total = stats[0]
    print(f"All row from Product:  {total}")

def count_rows_from_hook2(**context):

    hook = PostgresHook(postgres_conn_id="homework_06")
    stats = hook.get_records("""
        SELECT COUNT(*) as all_rows
        FROM customer;
    """)
    total = stats[0]
    print(f"All row from Customer!!! :  {total}")


def check_database_connection():    
    try:        
        postgres_conn_id = 'homework_06'
        
        logger.info(
            f"Проверка подключения к PostgreSQL с conn_id: {postgres_conn_id}")

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT 1;")
        db_name = cursor.fetchone()

        logger.info(f"Подключение успешно!")
        logger.info(f"База данных: {db_name[0]}")

        cursor.close()
        conn.close()

        return True

    except Exception as ex:
        logger.error(f"Ошибка подключения к базе данных: {str(ex)}")
        raise AirflowException(f"Database connection failed: {str(ex)}")


def log_final_results():
    try:
        logger.info(f"Создание таблицы агрегированных метрик")

        count_rows = """ 
        SELECT COUNT(*) as all_rows
        FROM website_daily_metrics; 
        """

        pg_hook = PostgresHook(postgres_conn_id="homework_06")
        pg_hook.run(count_rows)

        logger.info(f"Таблица web_logs_metrics создана/проверена")

    except Exception as ex:
        logger.error(
            f"Ошибка при создании таблицы агрегированных метрик: {str(ex)}")
        raise AirflowException(
            f"Aggregated table creation failed: {str(ex)}")

with DAG(
    dag_id="pizhurin_homework_06_dag",
    start_date=datetime(2025, 12, 18),    
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': on_failure_callback,
        'on_success_callback': on_success_callback
        },
    tags=["pizhurin", "hw_06", "postgres"],
) as dag:

    check_db_connection_task = PythonOperator(
        task_id='check_database_connection',
        python_callable=check_database_connection
    )

    drop_product_table = PostgresOperator(
        task_id="drop_product_table",
        postgres_conn_id="homework_06",
        sql="""
            DROP TABLE IF EXISTS public.order_items CASCADE;
            DROP TABLE IF EXISTS public.orders CASCADE;
            DROP TABLE IF EXISTS public.product CASCADE;
            DROP TABLE IF EXISTS public.customer CASCADE;
        """,
    )

    after_drop_product  = BashOperator(
        task_id='after_drop_product',
        bash_command='echo "[STAGE] DROP completed"',
    )

    prepare_tables = PostgresOperator(
        task_id="prepare_tables",
        postgres_conn_id="homework_06",
        sql="""
            CREATE TABLE IF NOT EXISTS public.customer (
                customer_id integer,
                first_name text,
                last_name text,
                gender text,
                dob date,
                job_title text,
                job_industry_category text,
                wealth_segment text,
                deceased_indicator boolean,
                owns_car boolean,
                address text,
                postcode text,
                state text,
                country text,
                property_valuation integer
            );

            CREATE TABLE IF NOT EXISTS public.product (
                product_id integer,
                brand text,
                product_line text,
                product_class text,
                product_size text,
                list_price numeric(10,2),
                standard_cost numeric(10,2)
            );

            CREATE TABLE IF NOT EXISTS public.orders (
                order_id integer,
                customer_id integer,
                order_date date,
                online_order text,
                order_status text
            );

            CREATE TABLE IF NOT EXISTS public.order_items (
                order_item_id integer,
                order_id integer,
                product_id integer,
                quantity numeric(10,2),
                item_list_price_at_sale numeric(10,2),
                item_standard_cost_at_sale numeric(10,2)
            );
            """,
    )

    with TaskGroup(group_id='load_files', dag=dag) as load_files_group:

        load_product = PostgresOperator(
            task_id="load_product",
            postgres_conn_id="homework_06",
            sql=f"""
                    COPY public.product
                    FROM '/tmp/data_files/product.csv'
                    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
                """,
        )

        load_customer = PostgresOperator(
            task_id="load_customer",
            postgres_conn_id="homework_06",
            sql=f"""
                    COPY public.customer
                    FROM '/tmp/data_files/customer.csv'
                    WITH (FORMAT CSV, HEADER true, DELIMITER ';');
                """,
        )

        load_orders = PostgresOperator(
            task_id="load_orders",
            postgres_conn_id="homework_06",
            sql=f"""
                    COPY public.orders
                    FROM '/tmp/data_files/orders.csv'
                    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
                """,
        )

        load_order_items = PostgresOperator(
            task_id="load_order_items",
            postgres_conn_id="homework_06",
            sql=f"""
                    COPY public.order_items
                    FROM '/tmp/data_files/order_items.csv'
                    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
                """,
        )

    with TaskGroup(group_id='count_rows_group', dag=dag) as count_rows_group:
        
        count_rows_customer = PythonOperator(
            task_id='count_rows_customer',
            python_callable=count_rows_from_hook2,
            provide_context=True,
        )

        count_rows_product = PythonOperator(
            task_id='count_rows_product',
            python_callable=count_rows_from_hook,
            provide_context=True,
        )

    log_all_counts = BashOperator(
        task_id="log_all_counts",
        bash_command="""
            echo "START LOG"
            echo "ROW COUNTS"
            echo "customer     = {{ (ti.xcom_pull(task_ids='count_rows_prl_group.count_rows_customer') or [[0]])[0][0] }}"            
            echo "END LOG"
            """,
    )

    with TaskGroup(group_id="export_top3_customers",
                   dag=dag) as export_top3_group:

        export_top3_min = PythonOperator(
            task_id="export_top3_min",
            python_callable=export_top3_min_to_file,
            provide_context=True,
        )

        export_top3_max = PythonOperator(
            task_id="export_top3_max",
            python_callable=export_top3_max_to_file,
            provide_context=True,
        )

    finish_task = BashOperator(
        task_id='message_task',
        bash_command='echo "Success finished"',
    )


    (
    check_db_connection_task
    >> drop_product_table
    >> after_drop_product
    >> prepare_tables
    >> load_files_group
    >> count_rows_group
    >> log_all_counts
    >> export_top3_group
    >> finish_task
    )



