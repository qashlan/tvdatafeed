import pytest
import pandas as pd
import datetime
from unittest.mock import MagicMock

from tvDatafeed import Interval
from tvDatafeed.seis import Seis


class TestSeisEquality:
    def test_equal_seis(self):
        s1 = Seis("AAPL", "NASDAQ", Interval.in_daily)
        s2 = Seis("AAPL", "NASDAQ", Interval.in_daily)
        assert s1 == s2

    def test_different_symbol(self):
        s1 = Seis("AAPL", "NASDAQ", Interval.in_daily)
        s2 = Seis("GOOG", "NASDAQ", Interval.in_daily)
        assert s1 != s2

    def test_different_exchange(self):
        s1 = Seis("AAPL", "NASDAQ", Interval.in_daily)
        s2 = Seis("AAPL", "NYSE", Interval.in_daily)
        assert s1 != s2

    def test_different_interval(self):
        s1 = Seis("AAPL", "NASDAQ", Interval.in_daily)
        s2 = Seis("AAPL", "NASDAQ", Interval.in_1_hour)
        assert s1 != s2

    def test_not_equal_to_other_type(self):
        s1 = Seis("AAPL", "NASDAQ", Interval.in_daily)
        assert s1 != "not a seis"
        assert s1 != 42
        assert s1 != None


class TestSeisProperties:
    def test_read_only_properties(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        assert s.symbol == "AAPL"
        assert s.exchange == "NASDAQ"
        assert s.interval == Interval.in_daily

    def test_tvdatafeed_initially_none(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        assert s.tvdatafeed is None

    def test_consumers_initially_empty(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        assert s.get_consumers() == []


class TestSeisRepr:
    def test_repr(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        r = repr(s)
        assert "AAPL" in r
        assert "NASDAQ" in r

    def test_str(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        text = str(s)
        assert "AAPL" in text
        assert "NASDAQ" in text


class TestSeisIsNewData:
    def _make_df(self, timestamp):
        idx = pd.DatetimeIndex([timestamp])
        return pd.DataFrame(
            {"open": [100], "high": [105], "low": [99], "close": [103], "volume": [1000]},
            index=idx,
        )

    def test_first_data_is_new(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        df = self._make_df(datetime.datetime(2024, 1, 1))
        assert s.is_new_data(df) is True

    def test_same_data_is_not_new(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        df = self._make_df(datetime.datetime(2024, 1, 1))
        s.is_new_data(df)  # first call sets _updated
        assert s.is_new_data(df) is False

    def test_different_data_is_new(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        df1 = self._make_df(datetime.datetime(2024, 1, 1))
        df2 = self._make_df(datetime.datetime(2024, 1, 2))
        s.is_new_data(df1)
        assert s.is_new_data(df2) is True


class TestSeisConsumerManagement:
    def test_add_and_get_consumer(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        mock_consumer = MagicMock()
        s.add_consumer(mock_consumer)
        assert mock_consumer in s.get_consumers()

    def test_pop_consumer(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        mock_consumer = MagicMock()
        s.add_consumer(mock_consumer)
        s.pop_consumer(mock_consumer)
        assert mock_consumer not in s.get_consumers()

    def test_pop_nonexistent_consumer_raises(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        with pytest.raises(NameError, match="Consumer does not exist"):
            s.pop_consumer(MagicMock())


class TestSeisTvdatafeedProperty:
    def test_set_non_tvdatafeedlive_raises(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        with pytest.raises(ValueError, match="Argument must be instance"):
            s.tvdatafeed = "not a tvdatafeed"

    def test_delete_tvdatafeed(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        # Can't easily set without a real TvDatafeedLive, but can test delete on None
        del s.tvdatafeed
        assert s.tvdatafeed is None

    def test_new_consumer_without_tvdatafeed_raises(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        with pytest.raises(NameError, match="TvDatafeed not provided"):
            s.new_consumer(lambda seis, data: None)

    def test_del_consumer_without_tvdatafeed_raises(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        with pytest.raises(NameError, match="TvDatafeed not provided"):
            s.del_consumer(MagicMock())

    def test_get_hist_without_tvdatafeed_raises(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        with pytest.raises(NameError, match="TvDatafeed not provided"):
            s.get_hist()

    def test_del_seis_without_tvdatafeed_raises(self):
        s = Seis("AAPL", "NASDAQ", Interval.in_daily)
        with pytest.raises(NameError, match="TvDatafeed not provided"):
            s.del_seis()
