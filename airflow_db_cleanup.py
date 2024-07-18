"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the DagRun (and TaskInstance, Log, XCom, Job DB and SlaMiss) entries to avoid
having too much data in your Airflow MetaStore based on a specified dag_id.

end_date and start_date decide between what dates. end_date is the date closest to you in time and start_date is the one furthest away.

dag_id specifices dag we want to delete, here multiple dag_ids can be added or if all dags are wished to be deleted input "*".

state_of_dag you can choose to only delete "success", "failed" or "*". So if you want to specify to only delete dag_ids that failed you add "failed"


Here is an example: 
airflow dags trigger --conf '{
    "end_date": "2024-04-01",
    "start_date": "2024-03-31",
    "dag_id": [
        "hello_world"
    ],
    "state_of_dag" : "*"
}}' airflow_db_cleanup

airflow dags 
--conf options:
    end_date:<INT> 
    start_date:<INT> 
    dag_id:[<STRING>]
    state_of_dag:<STRING> 

"""
import logging
import os

import airflow
from airflow import DAG
from airflow.models import  DagRun
from airflow.operators.python_operator import PythonOperator


# To see actual queries and debug sql alchemy
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


DAG_ID = os.path.basename(__file__).split('.')[0]

START_DATE = airflow.utils.dates.days_ago(1)




def get_default_date():
    from datetime import datetime, timedelta
    try:
        from airflow.utils import timezone
        now = timezone.utcnow
    except ImportError:
        now = datetime.utcnow

    n = datetime.now()
    if (n.hour >= 21):
        default_date = n.date().strftime('%Y-%m-%d')
    else:
        default_date = (n.date() + timedelta(days=-1)).strftime('%Y-%m-%d')
    return default_date

def log_dag(**kwargs):
    import cybertron_common
    cybertron_common.log_task(**kwargs)

# Printing and setting up parameters, then delete the specified entries
def print_and_cleanup_task(**context):
    
    import sys
    from airflow import settings, exceptions
    from sqlalchemy import func, and_
    from sqlalchemy.orm import load_only
    from sqlalchemy.exc import ProgrammingError
    import pendulum

    # How close the closest delete can be in days
    MAX_DAYS_AGO = 90

    PRINT_DELETES = False
    ENABLE_DELETE = True

    #Schedule params
    SCHEDULES = [
        {
            "DAG_ID" : ["Cybertron_SQS_controller"],
            "DAYS_AGO" : 90,
            "INTERVALL_DAYS_START_END" : 7,
            "STATE_OF_DAG" : "*"
        },
        {
            "DAG_ID" : ["hello_world"],
            "DAYS_AGO" : 180,
            "INTERVALL_DAYS_START_END" : 10,
            "STATE_OF_DAG" : "success"
        }
    ]


    session = settings.Session()

    local_tz = pendulum.timezone("Europe/Stockholm")


    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf/Inputed values: " + str(dag_run_conf))

    cur_date = pendulum.now(tz="UTC")

    list_dagIDs_to_clean = []

    end_date = None
    start_date = None
    dag_id = []
    state_of_dag = None

    if dag_run_conf:
        end_date = dag_run_conf.get(
            "end_date", None
        )
        start_date = dag_run_conf.get(
            "start_date", None
        )
        dag_id = dag_run_conf.get(
            "dag_id", []
        )
        state_of_dag = dag_run_conf.get(
            "state_of_dag", None
        )

        dag_id_config = {
            "end_date" : end_date,
            "start_date" : start_date,
            "dag_id" : dag_id,
            "state_of_dag" : state_of_dag
        }

        list_dagIDs_to_clean.append(dag_id_config)
        
    else:
        for dag_ids in SCHEDULES:

            SCHEDULE_DAYS_AGO = dag_ids["DAYS_AGO"]
            SCHEDULED_INTERVALL_DAYS_START_END = dag_ids["INTERVALL_DAYS_START_END"]

            start_date = str(cur_date.subtract(days=(SCHEDULE_DAYS_AGO + SCHEDULED_INTERVALL_DAYS_START_END)).strftime('%Y-%m-%d'))
            end_date = str(cur_date.subtract(days=(SCHEDULE_DAYS_AGO)).strftime('%Y-%m-%d'))
            dag_id = dag_ids["DAG_ID"]
            state_of_dag = dag_ids["STATE_OF_DAG"]

            dag_id_config = {
                "end_date" : end_date,
                "start_date" : start_date,
                "dag_id" : dag_id,
                "state_of_dag" : state_of_dag
            }

            list_dagIDs_to_clean.append(dag_id_config)

        logging.info("No config found, using scheduled values.")


    for dag_id_config in list_dagIDs_to_clean:

        start_date = dag_id_config["start_date"]
        end_date = dag_id_config["end_date"]
        dag_id = dag_id_config["dag_id"]
        state_of_dag = dag_id_config["state_of_dag"]

        try:
            if( end_date is None or start_date is None or state_of_dag is None or not dag_id):
                logging.error("All parameters are not inputed")
                raise exceptions.AirflowFailException("Terminating the DAG due to date validation error")

            end_date = pendulum.parse(end_date, tz= "UTC").end_of('day')
            start_date = pendulum.parse(start_date, tz= "UTC").start_of('day')

            max_date = cur_date.end_of('day').subtract(days=MAX_DAYS_AGO)

            end_date = end_date.in_tz(local_tz)
            start_date = start_date.in_tz(local_tz)
            max_date = max_date.in_tz(local_tz)

        except Exception as e:
            logging.error("Error in inputed values")
            logging.error("Terminating")
            sys.exit()



        try:
            if (end_date > max_date):
                logging.error("End_date is not far away enough from now, end_date needs to be less than max_date" )
                logging.error("max_date: " + str(max_date.strftime('%Y-%m-%d')))
                logging.error("end_date: " + str(end_date.strftime('%Y-%m-%d')))
                raise exceptions.AirflowFailException("Terminating the DAG due to date validation error")
            if (end_date < start_date ):
                    logging.error(
                        "Dates are inputed the wrong way, start dates needs to be less than or equal to end_date "
                    )
                    logging.error("start_date: " + str(start_date.strftime('%Y-%m-%d')))
                    logging.error("end_date:   " + str(end_date.strftime('%Y-%m-%d')))
                    raise exceptions.AirflowFailException("Terminating the DAG due to date validation error")
        except Exception as e:
            logging.error("Terminating")
            sys.exit()

        logging.info("Finished Loading Configurations for dag_ids:" + str(dag_id))
        logging.info("")

        airflow_db_model = DagRun
        age_check_column = DagRun.execution_date

        log_end_date = end_date.in_tz(tz="UTC").strftime('%Y-%m-%d')
        log_start_date = start_date.in_tz(tz="UTC").strftime('%Y-%m-%d')
        
        logging.info("Configurations:")
        logging.info("start_date:               " + str(log_start_date))
        logging.info("end_date:                 " + str(log_end_date))
        logging.info("dag_id:                   " + str(dag_id))
        logging.info("state_of_dag:             " + str(state_of_dag))
        logging.info("enable_delete:            " + str(ENABLE_DELETE))
        logging.info("session:                  " + str(session))
        logging.info("airflow_db_model:         " + str(airflow_db_model))
        logging.info("age_check_column:         " + str(age_check_column))
        logging.info("")

        logging.info("Running Cleanup Process...")

        try:
            query = session.query(airflow_db_model).options(
                load_only(age_check_column,'state', 'dag_id')
            )

            logging.info("INITIAL QUERY : " + str(query))

            query = query.filter(
                and_(func.timezone(local_tz.name, age_check_column) <= end_date),
                and_(func.timezone(local_tz.name, age_check_column) >= start_date),
            )

            if '*' not in dag_id:
                query = query.filter(
                    and_(airflow_db_model.dag_id.in_(dag_id))
                )
            
            if '*' != state_of_dag:
                query = query.filter(
                    airflow_db_model.state == state_of_dag
                )

            if PRINT_DELETES:
                entries_to_delete = query.all() 
                logging.info("Query: " + str(query))
                logging.info(
                    "Process will be Deleting the following " +
                    str(airflow_db_model.__name__) + "(s):"
                )
                for entry in entries_to_delete:
                    logging.info(
                        "\tEntry: " + str(entry) + ", Date: " +
                        str(entry.__dict__[str(age_check_column).split(".")[1]])
                    )

                logging.info(
                    "Process will be Deleting " + str(len(entries_to_delete)) + " " +
                    str(airflow_db_model.__name__) + "(s)"
                )
            else:
                logging.warn(
                    "You've opted to skip printing the db entries to be deleted. Set PRINT_DELETES to True to show entries!!!")

            count_query = query.with_entities(func.date_trunc('day', func.timezone(local_tz.name, age_check_column)).label('date'), func.count().label('count')).group_by('date')
            counts = count_query.all()
            if not counts:
                logging.info("Count of entries to delete was 0 for period " + str(log_start_date) + " to " + str(log_end_date))
            else:
                for count in counts:
                    formated_date = count.date.strftime('%Y-%m-%d')
                    logging.info(f"Count of entries to delete on {formated_date}: {count.count}")
            
            if ENABLE_DELETE:
                logging.info("Performing Delete...")

                query.delete(synchronize_session=False)
                session.commit() 

                logging.info("Finished Performing Delete")
            else:
                logging.warn(
                    "You've opted to skip deleting the db entries. Set ENABLE_DELETE to True to delete entries!!!")

            logging.info("Finished Running Cleanup Process")

        except ProgrammingError as e:
            logging.error(e)
            logging.error(str(airflow_db_model) +
                        " is not present in the metadata. Skipping...")
            
        except Exception as e:
            logging.error("An error occured: %s", str(e))
            if session.is_active:
                session.rollback()
                logging.error("Transaction has been rolled back")
        
        finally:
            session.close()



default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'params': {
        "start_date" : get_default_date(),
        "end_date" : get_default_date(), 
        "dag_id":["ingest_EEX", "Cybertron_SQS_controller"],
        "state_of_dag" : "*"
        }
}

# Dag
dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval= '* * * * *',
    start_date=START_DATE, 
    tags=['dbcleanup', 'airflow-maintenance-dags']
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False

# Tasks
task_log_dag = PythonOperator(
    task_id = 'log_dag',
    python_callable = log_dag,
    dag = dag)


# DagRun also deletes task_fail, task_reschedule, rendered_task_instance_fields, 
# task_instance, task_map and xcom tables on cascade. 
print_and_cleanup_op = PythonOperator(
    task_id='print_and_cleanup_task_' + str(DagRun.__name__),
    python_callable=print_and_cleanup_task,
    params= {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date
    },
    provide_context=True,
    dag=dag
)

#Flow
task_log_dag >> print_and_cleanup_op
