import logging
LOG_FORMAT="%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename='/var/log/producer.log',level=logging.DEBUG,format=LOG_FORMAT)

genlog=logging.getLogger()
