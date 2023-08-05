"""
Script main de ejecución que contiene el orquestador de los distintos ficheros run del proyecto.
"""
import sys
from init_config import launcher

sys.path.insert(0, "src.zip")
sys.path.insert(1, "bigdatatoolbox.zip")

JOBS = dict(
    JOB_TABLON_USERS_CRM=dict(function=run_create_tablon_users_crm, init_spark=True)
)

if __name__ == "__main__":

    # Obtenemos el nombre de la función como parámetro de entrada
    try:
        job_function = sys.argv[1]
        function = JOBS.get(job_function)
    except IndexError:
        raise Exception("Es necesario ")

    # Lanzamos la ejecución de la función.
    launcher(function["function"], init_spark=function["init_spark"], init_azure_sql=init_azure_sql,
             password_azure=password_azure, production_execution=True)
