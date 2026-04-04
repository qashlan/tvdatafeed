from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

import pandas as pd

if TYPE_CHECKING:
    import tvDatafeed
    from tvDatafeed.consumer import Consumer


class Seis:
    """
    Symbol, exchange and interval data set

    Holds a unique set of symbol, exchange and interval
    values in addition to keeping a set of consumers
    instances for this set.

    Parameters
    ----------
    symbol : str
        ticker string for symbol
    exchange : str
        exchange where symbol is listed
    interval : tvDatafeed.Interval
        chart interval

    Methods
    -------
    new_consumer(callback)
        Create a new consumer and add to Seis
    del_consumer(consumer)
        Remove consumer from Seis
    get_hist(n_bars)
        Get historic data for this Seis
    del_seis()
        Remove Seis from tvDatafeedLive where it is
        listed
    get_consumers()
        Return a list of consumers for this Seis
    """

    def __init__(self, symbol: str, exchange: str, interval: tvDatafeed.Interval) -> None:
        self._symbol: str = symbol
        self._exchange: str = exchange
        self._interval: tvDatafeed.Interval = interval

        self._tvdatafeed: Optional[tvDatafeed.TvDatafeedLive] = None
        self._consumers: list[Consumer] = []
        self._updated: Optional[object] = None  # datetime of the data bar that was last retrieved from TradingView

    def __eq__(self, other: object) -> bool:
        # Compare two seis instances to decide if they are equal
        #
        # Instances are equal if symbol, exchange and interval attributes
        # are of same value.
        if isinstance(other, self.__class__):  # make sure that they are the same class
            if self.symbol == other.symbol and self.exchange == other.exchange and self.interval == other.interval:  # these attributes need to be identical
                return True
        # TODO : add an option to compare Seis with list and tuple containing 3 string elements (symb, exch, inter)

        return False

    def __repr__(self) -> str:
        return f'Seis("{self._symbol}","{self._exchange}",{self._interval})'

    def __str__(self) -> str:
        return "symbol='" + self._symbol + "',exchange='" + self._exchange + "',interval='" + self._interval.name + "'"

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def exchange(self) -> str:
        return self._exchange

    @property
    def interval(self) -> tvDatafeed.Interval:
        return self._interval

    @property
    def tvdatafeed(self) -> Optional[tvDatafeed.TvDatafeedLive]:
        return self._tvdatafeed

    @tvdatafeed.setter
    def tvdatafeed(self, value: tvDatafeed.TvDatafeedLive) -> None:
        if (self._tvdatafeed) is not None:
            raise AttributeError("Cannot overwrite attribute, need to delete it first")
        elif not isinstance(value, __import__('tvDatafeed').TvDatafeedLive):
            raise ValueError("Argument must be instance of TvDatafeed")
        else:
            self._tvdatafeed = value

    @tvdatafeed.deleter
    def tvdatafeed(self) -> None:
        self._tvdatafeed = None

    def new_consumer(self, callback: Callable, timeout: float = -1) -> Consumer | bool:
        '''
        Create a new consumer and add to Seis

        Parameters
        ----------
        callback : func
            function to call when new data produced
        timeout : int, optional
            maximum time to wait in seconds for return, default
            is -1 (blocking)

        Returns
        -------
        tvdatafeed.Consumer
            If timeout was specified and expired then False will be
            returned instead of Consumer

        Raises
        ------
        NameError
            if no TvDatafeedLive reference is added for this Seis
        '''
        if self._tvdatafeed is None:
            raise NameError("TvDatafeed not provided")

        return self._tvdatafeed.new_consumer(self, callback, timeout)  # methods go through tvdatafeed to acquire lock and make it thread safe

    def del_consumer(self, consumer: Consumer, timeout: float = -1) -> bool:
        '''
        Remove consumer from Seis

        Parameters
        ----------
        consumer : tvdatafeed.Consumer
            consumer instance
        timeout : int, optional
            maximum time to wait in seconds for return, default
            is -1 (blocking)

        Returns
        -------
        boolean
            True if successful, False if timed out.

        Raises
        ------
        NameError
            if no TvDatafeedLive reference is added for this Seis
        '''
        if self._tvdatafeed is None:
            raise NameError("TvDatafeed not provided")

        return self._tvdatafeed.del_consumer(consumer, timeout)

    def add_consumer(self, consumer: Consumer) -> None:
        # Add consumer into Seis, not for direct use
        self._consumers.append(consumer)

    def pop_consumer(self, consumer: Consumer) -> None:
        # Remove consumer from Seis, not for direct use
        if consumer not in self._consumers:
            raise NameError("Consumer does not exist in the list")
        self._consumers.remove(consumer)

    def is_new_data(self, data: pd.DataFrame) -> bool:
        ''''
        Check if datas datetime is newer than previous datas datetime

        Parameters
        ----------
        data : pandas.DataFrame
            contains retrieved data and datetime

        Returns
        -------
        boolean
            True is new, False otherwise
        '''
        latest = data.index[0].to_pydatetime()
        if self._updated != latest:
            self._updated = latest
            return True

        return False

    def get_hist(self, n_bars: int = 10, timeout: float = -1) -> pd.DataFrame | bool:
        '''
        Get historic data for this Seis

        Parameters
        ----------
        n_bars : int, optional
            number of historic bars to retrieve, defaults to 10
        timeout : int, optional
            maximum time to wait in seconds for return, default
            is -1 (blocking)

        Returns
        -------
        pandas.DataFrame
            DataFrame containing data bars or if timeout was specified
            and timed out then False will be returned

        Raises
        ------
        NameError
            if no TvDatafeedLive reference is added for this Seis
        '''
        if self._tvdatafeed is None:
            raise NameError("TvDatafeed not provided")

        return self._tvdatafeed.get_hist(symbol=self._symbol, exchange=self._exchange, interval=self._interval, n_bars=n_bars, timeout=timeout)

    def del_seis(self, timeout: float = -1) -> bool:
        '''
        Remove Seis from tvDatafeedLive where it is
        listed

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
        if self._tvdatafeed is None:
            raise NameError("TvDatafeed not provided")

        return self._tvdatafeed.del_seis(self, timeout)

    def get_consumers(self) -> list[Consumer]:
        '''
        Return a list of consumers for this Seis

        Returns
        -------
        list
            contains all consumer instances registered
            for this Seis
        '''
        return self._consumers
