from agent.agent import Server
from model.model import Model, Accepter
import logging
import os
import sys
import statsmodels.tsa.stattools as stattools
from statsmodels.tsa.arima_model import ARIMA as ARIMA

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


class ARIMAModel(Model):
    @redirect
    def select_order(self):
        def objfunc(order, endog, exog):
            from statsmodels.tsa.arima_model import ARIMA
            old_target = sys.stdout
            sys.stdout = open('/dev/null')
            fit = ARIMA(endog, order, exog).fit()
            sys.stdout = old_target
            return fit.aic

        ts = self.get_series()
        bic = stattools.arma_order_select_ic(ts).bic_min_order
        grid = (slice(bic[0], bic[0]+1, 1), slice(1, 2, 1), slice(bic[1], bic[1]+1, 1))
        from scipy.optimize import brute
        return brute(objfunc, grid, args=(ts, None), finish=None)

    def is_stationary(self):
        pass

    @redirect
    def auto(self):
        order = self.select_order()
        self._model = ARIMA(self.get_series(), order).fit()

    def predict(self):
        ts = self.get_series()
        period = ts.index[-1] - ts.index[-2]
        start_date = ts.index[-1]
        end_date = start_date + self._predict * period
        return self._model.predict(start_date, end_date)


def redirect(f):
    def wrapped():
        old_target = sys.stdout
        sys.stdout = open('/dev/null')
        f()
        sys.stdout = old_target
    return wrapped()


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
