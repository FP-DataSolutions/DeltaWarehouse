class Logger:
    DEFAULT_LOG_LEVEL = 'INFO'

    # noinspection PyProtectedMember
    def __init__(self, class_name):
        #spark_context = SparkSpawner.get_sc()
        #spark_context.setLogLevel(self.DEFAULT_LOG_LEVEL)
        #self.__logger = spark_context._jvm.org.apache.log4j.LogManager.getLogger(class_name)
        return

    def error(self, msg):
        print(msg)
        #self.__logger.error(msg)

    def warn(self, msg):
        print(msg)
        #self.__logger.warn(msg)

    def info(self, msg):
        #self.__logger.info(msg)
        print(msg)

    def debug(self, msg):
        print(msg)
        #self.__logger.debug(msg)

    def trace(self, msg):
        print(msg)
        #self.__logger.trace(msg)
