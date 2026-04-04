import pytest
from unittest.mock import patch, MagicMock

from tvDatafeed import Interval
from tvDatafeed.datafeed import TvDatafeedLive


class TestSeisesAndTrigger:
    """Test the internal _SeisesAndTrigger data structure."""

    def _make_sat(self):
        return TvDatafeedLive._SeisesAndTrigger()

    def _make_seis(self, symbol="AAPL", exchange="NASDAQ", interval=Interval.in_daily):
        from tvDatafeed.seis import Seis
        return Seis(symbol, exchange, interval)

    def test_empty_sat(self):
        sat = self._make_sat()
        assert len(sat) == 0
        assert list(sat) == []

    def test_contains_false_when_empty(self):
        sat = self._make_sat()
        seis = self._make_seis()
        assert seis not in sat

    def test_append_with_update_dt(self):
        import datetime
        sat = self._make_sat()
        seis = self._make_seis()
        sat.append(seis, update_dt=datetime.datetime(2024, 1, 1))
        assert seis in sat

    def test_append_without_update_dt_raises(self):
        sat = self._make_sat()
        seis = self._make_seis()
        with pytest.raises(ValueError, match="Missing update datetime"):
            sat.append(seis)

    def test_append_second_seis_same_interval(self):
        import datetime
        sat = self._make_sat()
        s1 = self._make_seis("AAPL", "NASDAQ")
        s2 = self._make_seis("GOOG", "NASDAQ")
        sat.append(s1, update_dt=datetime.datetime(2024, 1, 1))
        sat.append(s2)  # same interval group, no update_dt needed
        assert s1 in sat
        assert s2 in sat

    def test_discard(self):
        import datetime
        sat = self._make_sat()
        seis = self._make_seis()
        sat.append(seis, update_dt=datetime.datetime(2024, 1, 1))
        sat.discard(seis)
        assert seis not in sat

    def test_discard_nonexistent_raises(self):
        sat = self._make_sat()
        seis = self._make_seis()
        with pytest.raises(KeyError, match="No such Seis"):
            sat.discard(seis)

    def test_get_seis_found(self):
        import datetime
        sat = self._make_sat()
        seis = self._make_seis()
        sat.append(seis, update_dt=datetime.datetime(2024, 1, 1))
        found = sat.get_seis("AAPL", "NASDAQ", Interval.in_daily)
        assert found is seis

    def test_get_seis_not_found(self):
        sat = self._make_sat()
        assert sat.get_seis("AAPL", "NASDAQ", Interval.in_daily) is None

    def test_iter_all_seises(self):
        import datetime
        sat = self._make_sat()
        s1 = self._make_seis("AAPL", "NASDAQ", Interval.in_daily)
        s2 = self._make_seis("GOOG", "NASDAQ", Interval.in_1_hour)
        sat.append(s1, update_dt=datetime.datetime(2024, 1, 1))
        sat.append(s2, update_dt=datetime.datetime(2024, 1, 1))
        all_seises = list(sat)
        assert s1 in all_seises
        assert s2 in all_seises

    def test_quit_sets_flag(self):
        sat = self._make_sat()
        sat.quit()
        assert sat._trigger_quit is True

    def test_clear_not_implemented(self):
        sat = self._make_sat()
        with pytest.raises(NotImplementedError):
            sat.clear()
