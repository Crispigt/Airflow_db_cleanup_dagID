# Airflow_db_cleanup_dagID
(Airflow V2.3.3) A dag for cleaning up the Airflow db specificly for the DagRun table and related tables to Dag_id. that cascades deletes.  Parameters that the dag is based on are dag_id, between specific dates, the state of the dag(failed/success).
