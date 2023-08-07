"""
Script main de ejecución que contiene el orquestador de los distintos ficheros run del proyecto.
"""
import sys
from init_config import launcher
from src.run_tablon import run_tablon

sys.path.insert(0, "src.zip")
sys.path.insert(1, "src.zip")

JOBS = dict(
    JOB_RUN_TABLON=dict(function=run_tablon, init_spark=True, use_databricks_spark=True)
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
