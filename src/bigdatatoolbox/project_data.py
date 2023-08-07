from .lake import Lake


class ProjectData(Lake):
    """
    Clase que contiene todos los datos necesarios para arrancar un projecto.
    """
    def __init__(self, config, logger, spark=None):
        self.config = config
        self.logger = logger
        self.spark = spark
