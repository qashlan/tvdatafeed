import time
import pytest
import pandas as pd
from unittest.mock import MagicMock

from tvDatafeed import Interval
from tvDatafeed.seis import Seis
from tvDatafeed.consumer import Consumer


def _make_seis():
    return Seis("AAPL", "NASDAQ", Interval.in_daily)


def _make_df():
    idx = pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
    return pd.DataFrame(
        {"open": [100], "high": [105], "low": [99], "close": [103], "volume": [1000]},
        index=idx,
    )


class TestConsumerInit:
    def test_creates_with_seis_and_callback(self):
        seis = _make_seis()
        cb = MagicMock(__name__="my_callback")
        c = Consumer(seis, cb)
        assert c.seis is seis
        assert c.callback is cb

    def test_thread_name(self):
        seis = _make_seis()
        cb = MagicMock(__name__="on_data")
        c = Consumer(seis, cb)
        assert "on_data" in c.name
        assert "AAPL" in c.name


class TestConsumerRepr:
    def test_repr(self):
        seis = _make_seis()
        cb = MagicMock(__name__="on_data")
        c = Consumer(seis, cb)
        r = repr(c)
        assert "Consumer" in r
        assert "on_data" in r

    def test_str(self):
        seis = _make_seis()
        cb = MagicMock(__name__="on_data")
        c = Consumer(seis, cb)
        s = str(c)
        assert "on_data" in s


class TestConsumerLifecycle:
    def test_put_and_callback(self):
        seis = _make_seis()
        received = []

        def on_data(s, data):
            received.append(data)

        c = Consumer(seis, on_data)
        c.start()

        df = _make_df()
        c.put(df)
        c.stop()  # sends None sentinel
        c.join(timeout=5)

        assert len(received) == 1
        assert received[0] is df

    def test_stop_terminates_thread(self):
        seis = _make_seis()
        cb = MagicMock(__name__="cb")
        c = Consumer(seis, cb)
        c.start()
        c.stop()
        c.join(timeout=5)
        assert not c.is_alive()

    def test_multiple_puts(self):
        seis = _make_seis()
        received = []

        def on_data(s, data):
            received.append(data)

        c = Consumer(seis, on_data)
        c.start()

        for _ in range(5):
            c.put(_make_df())

        c.stop()
        c.join(timeout=5)

        assert len(received) == 5

    def test_callback_exception_stops_consumer(self):
        seis = _make_seis()
        # Mock tvdatafeed on seis so del_consumer doesn't raise
        mock_tvdl = MagicMock()
        mock_tvdl.del_consumer.return_value = True
        seis._tvdatafeed = mock_tvdl

        def bad_callback(s, data):
            raise RuntimeError("callback error")

        c = Consumer(seis, bad_callback)
        c.start()
        c.put(_make_df())
        c.join(timeout=5)

        assert not c.is_alive()
