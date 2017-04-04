from agent.agent import Server
from model.model import Model, Accepter
import logging
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()

if __name__ == '__main__':
    sock = "/tmp/arima.sock"
    if os.path.exists(sock):
        os.remove(sock)
    server = Server(sock, Accepter())
    logger.info("Started server")
    logger.info("%d", os.getpid())
    server.serve()
    server.stop()
    logger.info("Finished server")
