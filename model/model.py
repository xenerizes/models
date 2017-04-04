from agent.agent import Agent, Handler
import agent.udf_pb2 as udf_pb2
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


class Model(object):
    pass


class MessageHandler(Handler):
    def __init__(self, agent, model):
        self._agent = agent
        self._model = model
        self._predict = 0
        self._period = 0

    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.BATCH
        response.info.options['predict'].valueTypes.append(udf_pb2.INT)

        return response

    def init(self, init_req):
        response = udf_pb2.Response()
        succ = True
        msg = ''

        for opt in init_req.options:
            if opt.name == 'predict':
                self._predict = opt.values[0].intValue

        if self._predict < 1:
            succ = False
            msg += ' must supply number of values to be predicted > 0'

        response.init.success = succ
        response.init.error = msg

        return response

    def begin_batch(self, begin_req):
        pass

    def point(self, point):
        pass

    def end_batch(self, end_req):
        pass

    def snapshot(self):
        response = udf_pb2.Response()
        response.snapshot.snapshot = ''
        return response

    def restore(self, restore_req):
        response = udf_pb2.Response()
        response.restore.success = False
        response.restore.error = 'not implemented'
        return response


class Accepter(object):
    def __init__(self):
        self._count = 0

    def accept(self, conn, addr):
        self._count += 1
        a = Agent(conn, conn)
        h = MessageHandler(a)
        a.handler = h
        logger.info("Starting Agent for connection %d", self._count)
        a.start()
        a.wait()
        logger.info("Agent finished connection %d", self._count)

