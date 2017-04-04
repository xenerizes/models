from agent.agent import Agent, Handler
import agent.udf_pb2 as udf_pb2


class Model(object):
    pass


class MessageHandler(Handler):
    def __init__(self, agent):
        self._agent = agent

    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.BATCH

        return response

    def init(self, init_req):
        response = udf_pb2.Response()
        response.init.success = True

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

