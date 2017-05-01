from agent.agent import Server, Agent
from model.model import Model, MessageHandler
import logging
import os
from statsmodels.tsa.stattools import arma_order_select_ic
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima_model import ARIMA
from pandas.tseries.offsets import Second

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


def is_stationary(ts):
    results = adfuller(ts, regression='ct')
    return results[0] < results[4]['5%']


class ARIMAModel(Model):
    def __init__(self):
        Model.__init__(self)
        self._order = None

    def select_order_brute_force(self):
        def objfunc(order, endog, exog):
            from statsmodels.tsa.arima_model import ARIMA
            fit = ARIMA(endog, order, exog).fit(full_output=False)
            return fit.aic

        ts = self.get_series()
        bic = arma_order_select_ic(ts).bic_min_order
        grid = (slice(bic[0], bic[0] + 1, 1), slice(1, 2, 1), slice(bic[1], bic[1] + 1, 1))
        from scipy.optimize import brute
        return brute(objfunc, grid, args=(ts, None), finish=None)

    def select_order(self):
        ts = self.get_series()
        if is_stationary(ts):
            bic = arma_order_select_ic(ts).bic_min_order
            return bic[0], 0, bic[1]

        ts1diff = ts.diff(periods=1).dropna()
        if is_stationary(ts1diff):
            bic = arma_order_select_ic(ts1diff).bic_min_order
            return bic[0], 1, bic[1]

        ts2diff = ts.diff(periods=2).dropna()
        bic = arma_order_select_ic(ts2diff).bic_min_order

        return bic[0], 2, bic[1]

    def get_fitted_values(self):
        return self._model.fittedvalues

    def auto(self):
        ts = self.get_series()
        self._period = ts.index[1] - ts.index[0]
        freq = Second(self._period.total_seconds())
        self._order = self.select_order()
        self._model = ARIMA(self.get_series(), order=self._order, freq=freq).fit()

    def predict(self):
        start_date = self._model.fittedvalues.index[-1]
        end_date = start_date + self._predict * self._period
        forecast = self._model.predict(start_date.isoformat(), end_date.isoformat())

        if self._order[1] > 0:
            shift = self.max() - self.min()
            forecast += shift

        return forecast


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


if __name__ == '__main__':
    sock = "/tmp/arima.sock"
    if os.path.exists(sock):
        os.remove(sock)
    server = Server(sock, Acceptor())
    logger.info("Started server")
    logger.info("%d", os.getpid())
    server.serve()
    server.stop()
    logger.info("Finished server")
