from dag_utils.postgres.PostgresManager import PostgresManager


def run_dq_check(check_name, sql_query):
    """
    DQ check logic will be saved in a string and executed by PostgresManager 
    """
    postgres_manager = PostgresManager()
    postgres_manager.run_dq_check(check_name, sql_query)


