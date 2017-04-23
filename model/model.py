from agent.agent import Agent, Handler
import agent.udf_pb2 as udf_pb2
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()

FIELD_TYPES = ['int', 'double']


class Model(object):
    def __init__(self):
        self._dates = []
        self._values = []
        self._predict = 0
        self._period = None
        self._model = None

    def append(self, date, value):
        self._dates.append(date)
        self._values.append(value)

    def drop(self):
        self._dates = []
        self._values = []

    def get_series(self):
        return pd.Series(data=self._values, index=pd.to_datetime(self._dates))

    def auto(self):
        pass

    def predict(self):
        pass

    def min(self):
        return min(self._values)

    def max(self):
        return max(self._values)


class MessageHandler(Handler):
    def __init__(self, agent, model):
        self._agent = agent
        self._model = model
        self._predict = 0
        self._field = ''
        self._field_type = None
        self._begin_response = None
        self._point = None

    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.BATCH
        response.info.options['predict'].valueTypes.append(udf_pb2.INT)
        response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        response.info.options['type'].valueTypes.append(udf_pb2.STRING)

        return response

    def init(self, init_req):
        response = udf_pb2.Response()
        succ = True
        msg = ''

        for opt in init_req.options:
            if opt.name == 'predict':
                self._predict = opt.values[0].intValue
                self._model._predict = self._predict
            if opt.name == 'field':
                self._field = opt.values[0].stringValue
            if opt.name == 'type':
                self._field_type = opt.values[0].stringValue

        if self._predict < 1:
            succ = False
            msg += ' must supply number of values to be predicted > 0'
        if self._field == '':
            succ = False
            msg += ' must specify the field to use'
        if self._field_type not in FIELD_TYPES:
            succ = False
            msg += ' field type must be one of {}'.format(FIELD_TYPES)

        response.init.success = succ
        response.init.error = msg[1:]

        return response

    def begin_batch(self, begin_req):
        self._model.drop()

        response = udf_pb2.Response()
        response.begin.CopyFrom(begin_req)
        self._begin_response = response

    def point(self, point):
        if self._field_type is 'int':
            value = point.fieldsInt[self._field]
        else:
            value = point.fieldsDouble[self._field]
        self._model.append(pd.to_datetime(point.time), value)
        self._point = point

    def end_batch(self, end_req):
        self._model.auto()
        forecast = self._model.predict()

        self._begin_response.begin.size = self._predict
        self._agent.write_response(self._begin_response)

        response = udf_pb2.Response()
        response.point.CopyFrom(self._point)
        for i in range(0, self._predict):
            response.point.time = forecast.index[i].value
            if self._field_type is 'int':
                response.point.fieldsInt[self._field] = forecast[i]
            else:
                response.point.fieldsDouble[self._field] = forecast[i]
            self._agent.write_response(response)

        response.end.CopyFrom(end_req)
        self._agent.write_response(response)

    def snapshot(self):
        response = udf_pb2.Response()
        response.snapshot.snapshot = ''
        return response

    def restore(self, restore_req):
        response = udf_pb2.Response()
        response.restore.success = False
        response.restore.error = 'not implemented'
        return response


class Acceptor(object):
    def __init__(self):
        self._count = 0

    def accept(self, conn, address):
        self._count += 1
        a = Agent(conn, conn)
        h = MessageHandler(a, ARIMAModel())
        a.handler = h
        logger.info("Starting Agent for connection %d", self._count)
        a.start()
        a.wait()
        logger.info("Agent finished connection %d", self._count)
