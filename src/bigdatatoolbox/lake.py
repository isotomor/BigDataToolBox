class Lake(object):
    """
    Clase encargada de gestionar las llamadas al datalake, escritura y lectura de datos con las diferentes interfaces.
    """

    def __init__(self, config, logger, spark=None):
        self.config = config
        self.logger = logger
        self.spark = spark

