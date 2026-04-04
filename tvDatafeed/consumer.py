from __future__ import annotations

import logging
import queue
import threading
from typing import TYPE_CHECKING, Callable, Optional

import pandas as pd

from tvDatafeed.config import CONSUMER_QUEUE_MAXSIZE, CONSUMER_QUEUE_PUT_TIMEOUT

if TYPE_CHECKING:
    from tvDatafeed.seis import Seis

logger = logging.getLogger(__name__)


class Consumer(threading.Thread):
    '''
    Seis data consumer and processor

    This object contains reference to Seis and callback function
    which will be called when new data bar becomes available for
    that Seis. Data reception and calling callback function is
    done in a separate thread which the user must start by calling
    start() method.

    Parameters
    ----------
    seis : Seis
        Consumer receives data bar from this Seis
    callback : func
        reference to a function to be called when new data available,
        function protoype must be func_name(seis, data)

    Methods
    -------
    put(data)
        Put new data into buffer to be processed
    del_consumer()
        Shutdown the callback thread and remove from Seis
    start()
        start data processing and callback thread
    stop()
        Stop the data processing and callback thread
    '''
    def __init__(self, seis: Seis, callback: Callable[[Seis, pd.DataFrame], None]) -> None:
        super().__init__()

        self._buffer: queue.Queue[Optional[pd.DataFrame]] = queue.Queue(maxsize=CONSUMER_QUEUE_MAXSIZE)
        self.seis: Optional[Seis] = seis
        self.callback: Optional[Callable] = callback
        self.name = self.callback.__name__ + "_" + self.seis.symbol + "_" + seis.exchange + "_" + seis.interval.value

    def __repr__(self) -> str:
        return f'Consumer({repr(self.seis)},{self.callback.__name__})'

    def __str__(self) -> str:
        return f'{repr(self.seis)},callback={self.callback.__name__}'

    def run(self) -> None:
        # callback thread tasks
        while True:
            data = self._buffer.get()
            if data is None:
                break

            try:  # in case user provided function throws an exception
                self.callback(self.seis, data)
            except Exception as e:  # remove the consumer from Seis and close down gracefully
                logger.error("callback %s raised an exception: %s", self.callback.__name__, e, exc_info=True)
                self.del_consumer()
                self.seis = None  # delete references
                self.callback = None
                self._buffer = None
                raise

        self.seis = None  # delete references
        self.callback = None
        self._buffer = None

    def put(self, data: Optional[pd.DataFrame]) -> None:
        '''
        Put new data into buffer to be processed

        Parameters
        ----------
        data : pandas.DataFrame
            contains single bar data retrieved from TradingView
        '''
        try:
            self._buffer.put(data, timeout=CONSUMER_QUEUE_PUT_TIMEOUT)
        except queue.Full:
            logger.warning("Consumer queue full for %s, dropping data", self.name)

    def del_consumer(self, timeout: float = -1) -> bool:
        '''
        Stop the callback thread and remove from Seis

        Parameters
        ----------
        timeout : int, optional
            maximum time to wait in seconds for return, default
            is -1 (blocking)

        Returns
        -------
        boolean
            True if successful, False if timed out.
        '''
        return self.seis.del_consumer(self, timeout)

    def stop(self, join_timeout: float = 5.0) -> None:
        '''
        Stop the data processing and callback thread

        Parameters
        ----------
        join_timeout : float, optional
            maximum time to wait for thread to finish, default is 5.0 seconds
        '''
        try:
            self._buffer.put(None, timeout=CONSUMER_QUEUE_PUT_TIMEOUT)
        except queue.Full:
            logger.warning("Could not send stop signal to %s, queue full", self.name)
        if self.is_alive():
            self.join(timeout=join_timeout)
