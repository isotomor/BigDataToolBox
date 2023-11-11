import os
import time
import pickle
import logging
import pandas as pd
from prophet import Prophet
from datetime import datetime
from src.init_config import launcher
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import plot_cross_validation_metric

logging.getLogger('cmdstanpy').disabled = True


def create_features_time(df, label=None):
    """
    Función que agrega variables relacionadas con la fecha para ser incorporadas en el modelo prophet
    Parámetro:
    - df: dataframe con datos para el modelo. Debe tener una columna llamada "date" con formato '%Y-%m-%d'
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['hour'] = df['date'].dt.hour
    df['dayofweek'] = df['date'].dt.dayofweek
    df['weekday'] = df['date'].dt.day_name()
    df['weekday'] = df['weekday'].astype('category')
    df['quarter'] = df['date'].dt.quarter
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['dayofyear'] = df['date'].dt.dayofyear
    df['dayofmonth'] = df['date'].dt.day
    df['weekofyear'] = df['date'].dt.isocalendar().week
    df['date_offset'] = (df['date'].dt.month * 100 + df['date'].dt.day - 320) % 1300

    df['season'] = pd.cut(df['date_offset'], [0, 300, 602, 900, 1300],
                          labels=['Spring', 'Summer', 'Fall', 'Winter'])

    X = df[['month', 'year', 'season', 'hour', 'dayofweek', 'weekday']]
    if label:
        y = df[label]
        return X, y
    return X


def prophet_model(project_data, df, action, historico, horizonte, train_output_path, estimate_output_path, date, null,
                  cross_val, unique_id=None):
    """
    Función que ejecuta el entrenamiento del modelo o realiza el forecast para los próximos días (7) con el modelo
    prophet

    Parámetros:
    - df: dataframe que contiene datos diarios de ventas (sales), con un id (id_) que es unico para cada producto/tienda
     y la fecha (snapshotdate)
    - action: puede ser "train", si se quiere entrenar el modelo, o "estimate" si se quiere predecir
    - historico: histórico que se tiene para realizar el entrenamiento, en años. En nuestro caso hay sólo 3, pero se
    asumirá que hay 5 años.
    - horizonte: Horizonte de predicción. En nuestro caso será 7 días
    - train_output_path: path donde se guardan los resultados del entrenamiento, en su fecha correpsondiente.
    - estimate_output_path: path donde se guardan los resultados del forecast, en su fecha correpsondiente.
    - null: opción para reemplazar valores nulos  por la mediana. Por defecto, no se hace nada en la función Prophet
    """

    # Convertimos la columna 'date' a formato datetime
    df['date'] = pd.to_datetime(df['date'])

    # Si se proporciona un unique_id, úsalo. De lo contrario, usa todos los unique_ids en el dataframe.
    if unique_id is None:
        unique_ids = df['id'].unique()
    else:
        unique_ids = [unique_id]
    project_data.logger.info(unique_ids)
    results = []

    # Obtenemos el rango de fechas disponible en el dataframe.
    min_date = df['date'].min()
    max_date = df['date'].max()
    available_years = (max_date - min_date).days // 365  # Aproximación al número de años disponibles

    # Establecemos el 'historico' basado en el rango de fechas disponible. Como máximo se tomarán 5 años.
    historico = min(historico, available_years)

    for uid in unique_ids:
        df_uid = df[df['id'] == uid]

        # Preparamos el dataframe para Prophet
        df_prophet = df_uid[['date', 'sales']].copy()
        df_prophet.columns = ['ds', 'y']

        # Si queremos reemplazar valores nulos con la mediana
        if null:
            df_prophet['y'].fillna(df_prophet['y'].median(), inplace=True)

        # Definimos un máximo que puede asumir la variable objetivo para acotar las predicciones-
        # Es necesario el lim superior para poder fijar el límite inferior en cero (cap)
        some_upper_limit = df_prophet['y'].max()

        # Acción "train"
        if action == "train":
            # Filtramos los datos de los últimos años (historico =5)
            last_date = df_prophet['ds'].max()
            start_date = last_date - pd.DateOffset(years=historico)
            df_train = df_prophet[df_prophet['ds'] > start_date].copy()
            df_train['cap'] = some_upper_limit
            df_train['floor'] = 0

            # Entrenar el modelo Prophet
            model = Prophet()
            project_data.logger.info("Lanzo en entrenamiento")
            model.fit(df_train)
            project_data.logger.info("TErmino el entrenamiento")

            # Validación cruzada
            if cross_val:
                df_cv = cross_validation(model, initial=f'{historico - 1}y', period='7 days', horizon='7 days')
                df_perf = performance_metrics(df_cv)
                # project_data.logger.info(f"Performance metrics for {uid}:\n", df_perf.head(7))

                # Creamos el subdirectorio cross_val si no existe
                cross_val_directory_path = os.path.join(train_output_path, date, "cross_val")
                if not os.path.exists(cross_val_directory_path):
                    os.makedirs(cross_val_directory_path)

                # Definimos la ruta del archivo para guardar los resultados de la validación cruzada
                cross_val_filename = os.path.join(cross_val_directory_path, f"cross_val_results_{uid}.csv")

                # Guardamos el DataFrame df_perf en el archivo CSV
                df_perf.to_csv(cross_val_filename, index=False)

            # Crear el directorio con el nombre de la fecha si no existe
            train_directory_path = os.path.join(train_output_path, date)
            if not os.path.exists(train_directory_path):
                os.makedirs(train_directory_path)

            model_name = f"{train_directory_path}/prophet_model_{uid}.pkl"
            with open(model_name, "wb") as file:
                pickle.dump(model, file)

            results.append(f"Modelo para {uid} entrenado y guardado exitosamente.")

        # Acción "estimate"
        elif action == "estimate":
            # Cargar el modelo entrenado desde el archivo pickle
            train_directory_path = os.path.join(train_output_path, date)
            model_name = f"{train_directory_path}/prophet_model_{uid}.pkl"
            with open(model_name, "rb") as file:
                model = pickle.load(file)

            # Hacemos la predicción para los próximos 7 días

            future = model.make_future_dataframe(periods=horizonte)

            # Como las ventas son positivas, fijamos valores máximos y mínimos
            future['cap'] = some_upper_limit
            future['floor'] = 0.0
            forecast = model.predict(future)

            # Filtramos solo las predicciones de los próximos 7 días
            forecast = forecast.tail(7)

            # Guardamos los resultados en un archivo parquet con la fecha del día en que se hace la estimación
            current_date = datetime.now().strftime('%Y-%m-%d')
            est_directory_path = os.path.join(estimate_output_path, current_date)
            if not os.path.exists(est_directory_path):
                os.makedirs(est_directory_path)
            filename = f"{est_directory_path}/forecast_{uid}.parquet"

            timestamp_columns = forecast.select_dtypes(include=['datetime64[ns]']).columns
            forecast[timestamp_columns] = forecast[timestamp_columns].astype('object')
            forecast.to_parquet(filename)

            results.append(f"Predicción para {uid} realizada. Resultados guardados en {filename}.")

        else:
            results.append(f"Acción no reconocida para {uid}.")

    return results


def view_forecast(id_predicctions, date, estimate_output_path):
    """
    Función para visualizar las predicciones guardadas en un archivo parquet para un id_ específico.

    Parámetros:
    - id_ (str): El id_ para el que deseas ver las predicciones.
    - date (str): La fecha en que se realizó la predicción (formato 'YYYY-MM-DD').
    - estimate_output_path: path donde se guardan los resultados del forecast.

    Retorna:
    - DataFrame con las predicciones para el id_ en la fecha especificada.
    """

    # Cargamos el archivo parquet
    filepath = os.path.join(estimate_output_path, date, f"forecast_{id_predicctions}.parquet")
    df_forecast = pd.read_parquet(filepath)

    return df_forecast


def main_baseline_prophet_cval(project_data, input_path, train_output_path, estimate_output_path, df_name, action,
                               historico, horizonte, date=None, null=None, cross_val=None, unique_id=None):
    """
    Función que ejecuta el entrenamiento del modelo o realiza el forecast para los próximos días (7) con el modelo
    prophet. la salida forcast_df corresponde a los
    datos de forecast para el id (unique_id) del input, si es que se ha dado la action "estimate" y un unique_id

    Parámetros:
    - input_path: path donde se encuentran los datos de entrada.
    - train_output_path: path donde se guardan los resultados del entrenamiento, en su fecha correpsondiente.
    - estimate_output_path: path donde se guardan los resultados del forecast, en su fecha correpsondiente.
    - df_name: nombre del dataframe donde leer los datos. El df contiene datos diarios de ventas (sales), con un id
    (id_) que es unico para cada producto/tienda y la fecha (snapshot_date).
    - action: puede ser "train", si se quiere entrenar el modelo, o "estimate" si se quiere predecir.
    - historico: histórico que se tiene para realizar el entrenamiento en años.
    - horizonte: Horizonte de predicción en días.
    - unique_id: (opcional) id único para el cual se desea hacer el entrenamiento y la predicción. If None, tanto el
    entrenamiento, como la predicción se hacen para todos
    - date: (opcional) fecha de hoy. Si no se proporciona, se toma la fecha actual.
    - null: opción para reemplazar valores nulos de y por la mediana. Por defecto, no se hace nada en la función Prophet
    - cross_val = True,# si es True se realiza el training con cross validation y se guardan resultados para cada id
    unico
    - unique_id= "A_1_015_1", # Ejemplo ID UNICO. son los resultados que se mostrarán al final. If None, se realiza
    entrenamiento y forecast para todos los id unicos
    """
    project_data.logger.info("Empiezo")
    # Registrar el tiempo de inicio
    start_time = time.time()

    # Creamos el directorio 'forecast' si no existe
    if not os.path.exists('forecast'):
        os.makedirs('forecast')

    # Creamos el directorio 'training' si no existe
    if not os.path.exists('training'):
        os.makedirs('training')

    # Si no se proporciona una fecha, toma la fecha actual
    if date is None:
        date = datetime.today().strftime('%Y-%m-%d')

    # Se leen los datos:
    print(f"El input_path es: {input_path}")
    print(f"El df_name es: {df_name}")
    # df = pd.read_parquet(f"{input_path}{df_name}.parquet")
    storaga_account = "cloudmlarquitecture"
    df = project_data.spark.read.csv(f"abfss://raw@{storaga_account}.dfs.core.windows.net/products.csv").toPandas()

    data = df[['snapshot_date', 'price', 'id_', 'sales']].copy()
    data = data.rename(columns={'snapshot_date': 'date', 'id_': 'id'}, inplace=False)

    # Si queremos agregar variables relacionadas con el tiempo para incluir en prophet
    X, y = create_features_time(data, label=['date', 'price', 'id', 'sales'])
    df_new = pd.concat([X, y], axis=1)

    project_data.logger.info("llamo a prophet_model")
    outputs = prophet_model(project_data, df_new, action, historico, horizonte, train_output_path, estimate_output_path, date, null,
                            cross_val, unique_id)

    # Si la acción es "estimate" y se ha proporcionado un unique_id, entonces se visualiza la predicción para ese id
    if action == "estimate" and unique_id:
        forecast_df = view_forecast(unique_id, date, estimate_output_path)
    else:
        forecast_df = None
    # Registrar el tiempo final
    end_time = time.time()

    # Calcular la duración
    duration = end_time - start_time

    project_data.logger.info(f"La action '{action}' se ejecutó en {duration:.2f} segundos.")
    return forecast_df


def run_main_baseline_prophet_cval(project_data, **_):
    project_data.logger.info("Entro a la función")

    training = main_baseline_prophet_cval(project_data=project_data,
                                          input_path="./",  # directorio donde se encuentran los datos de entrada
                                          train_output_path="./training/",  # directorio de salida del entrenamiento
                                          estimate_output_path="./forecast/",
                                          # directorio de salida de las estimaciones
                                          df_name='dataframe',  # fichero con datos en formato parquet
                                          action="train",
                                          # puede ser "estimate" para forecats o "train" para entrenamiento
                                          historico=3,
                                          # histórico de años que tenemos en para realizar el training # cambio a 3, ya
                                          # que al no haber datos para 5 años, la CV da error
                                          horizonte=7,  # número de días que se quieren predecir
                                          date=None,  # if none es la fecha de hoy
                                          null=True,
                                          # opción para reemplazar valores nulos de y por la mediana. Por defecto,
                                          # no se hace nada
                                          cross_val=None,
                                          # si es True se realiza el training con cross validation y se guardan
                                          # resultados para cada id unico
                                          unique_id='A_1_015_1',
                                          # Ejemplo ID UNICO. son los resultados que se mostrarán al final. If None,
                                          # se realiza entrenamiento y forecast para todos los id unicos
                                          )

    # =======================================================================================================
    #                                                ESTIMATE
    # =======================================================================================================
    # Con la action "estimate" se puede indicad un unique_id para ver los resultados de estimación para ese id único.
    # Ejemplo = "A_1_015_1"

    forecast_unique_id = main_baseline_prophet_cval(project_data=project_data,
                                                    input_path="./",
                                                    # directorio donde se encuentran los datos de entrada
                                                    train_output_path="./training/",
                                                    # directorio de salida del entrenamiento
                                                    estimate_output_path="./forecast/",
                                                    # directorio de salida de las estimaciones
                                                    df_name='dataframe',  # fichero con datos en formato parquet
                                                    action="estimate",
                                                    # puede ser "estimate" para forecats o "train" para entrenamiento
                                                    historico=3,
                                                    # histórico de años que tenemos en para realizar el training
                                                    horizonte=7,  # número de días que se quieren predecir
                                                    date=None,  # if none es la fecha de hoy
                                                    null=True,
                                                    # opción para reemplazar valores nulos de y por la mediana.
                                                    # Por defecto, no se hace nada
                                                    cross_val=None,
                                                    # si es True se realiza el training con cross validation y se
                                                    # guardan resultados para cada id unico
                                                    unique_id='A_1_015_1',
                                                    # Ejemplo ID UNICO. son los resultados que se mostrarán al final.
                                                    # If None, se calculan datos para todos los id unicos
                                                    )

    forecast_unique_id.head()


if __name__ == "__main__":
    launcher(run_main_baseline_prophet_cval, init_spark=False, use_databricks_spark=False)
