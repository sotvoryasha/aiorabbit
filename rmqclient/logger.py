import logging
import sys

logger = logging.getLogger('RMQClient')
logger.setLevel('DEBUG')
formatter = logging.Formatter(fmt='[%(asctime)s] [%(name)s] [%(levelname).1s] %(message)s',
                              datefmt='%Y.%m.%d %H:%M:%S')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

logger.addHandler(handler)