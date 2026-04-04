import datetime
import json
import pytest
import pandas as pd
import requests
from unittest.mock import patch, MagicMock

from tvDatafeed.main import TvDatafeed, Interval


class TestGenerateId:
    def test_qs_prefix(self):
        session = TvDatafeed._TvDatafeed__generate_id("qs_")
        assert session.startswith("qs_")

    def test_cs_prefix(self):
        session = TvDatafeed._TvDatafeed__generate_id("cs_")
        assert session.startswith("cs_")

    def test_length(self):
        session = TvDatafeed._TvDatafeed__generate_id("qs_")
        assert len(session) == 15  # "qs_" + 12 chars

    def test_uniqueness(self):
        s1 = TvDatafeed._TvDatafeed__generate_id("qs_")
        s2 = TvDatafeed._TvDatafeed__generate_id("qs_")
        assert s1 != s2

    def test_lowercase_chars(self):
        session = TvDatafeed._TvDatafeed__generate_id("qs_")
        assert session[3:].isalpha()
        assert session[3:].islower()


class TestPrependHeader:
    def test_basic(self):
        result = TvDatafeed._TvDatafeed__prepend_header("hello")
        assert result == "~m~5~m~hello"

    def test_empty(self):
        result = TvDatafeed._TvDatafeed__prepend_header("")
        assert result == "~m~0~m~"

    def test_json_message(self):
        msg = '{"m":"test","p":[]}'
        result = TvDatafeed._TvDatafeed__prepend_header(msg)
        assert result == f"~m~{len(msg)}~m~{msg}"


class TestConstructMessage:
    def test_basic(self):
        result = TvDatafeed._TvDatafeed__construct_message("set_auth_token", ["token123"])
        parsed = json.loads(result)
        assert parsed == {"m": "set_auth_token", "p": ["token123"]}

    def test_no_spaces(self):
        result = TvDatafeed._TvDatafeed__construct_message("test", [1, 2])
        assert " " not in result  # separators=(",", ":")

    def test_complex_params(self):
        result = TvDatafeed._TvDatafeed__construct_message("func", ["a", {"flags": ["force"]}])
        parsed = json.loads(result)
        assert parsed["p"][1]["flags"] == ["force"]


class TestFormatSymbol:
    def test_colon_in_symbol(self):
        result = TvDatafeed._TvDatafeed__format_symbol("NSE:NIFTY", "NSE")
        assert result == "NSE:NIFTY"

    def test_cash_no_contract(self):
        result = TvDatafeed._TvDatafeed__format_symbol("NIFTY", "NSE")
        assert result == "NSE:NIFTY"

    def test_futures_contract(self):
        result = TvDatafeed._TvDatafeed__format_symbol("CRUDEOIL", "MCX", contract=1)
        assert result == "MCX:CRUDEOIL1!"

    def test_invalid_contract(self):
        with pytest.raises(ValueError, match="not a valid contract"):
            TvDatafeed._TvDatafeed__format_symbol("SYM", "EX", contract="bad")


class TestFilterRawMessage:
    def test_valid_message(self):
        # The p regex expects the payload to end with "}"])}
        text = '{"m":"timescale_update","p":["cs_abc",{"key":"val"}"]}'
        result = TvDatafeed._TvDatafeed__filter_raw_message(text)
        assert result is not None
        found, found2 = result
        assert found == "timescale_update"

    def test_no_match(self):
        result = TvDatafeed._TvDatafeed__filter_raw_message("garbage data")
        assert result is None


class TestCreateDf:
    def test_valid_data(self):
        # Real TradingView format: {"i":N,"v":[timestamp,open,high,low,close,volume]}
        raw = '{"s":[{"i":0,"v":[1609459200,100.0,105.0,99.0,103.0,1000000.0]},{"i":1,"v":[1609545600,103.0,108.0,102.0,107.0,1200000.0]}]}'
        df = TvDatafeed._TvDatafeed__create_df(raw, "TEST:SYM")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["symbol", "open", "high", "low", "close", "volume"]
        assert df["symbol"].iloc[0] == "TEST:SYM"
        assert df["open"].iloc[0] == 100.0
        assert df["close"].iloc[0] == 103.0
        assert df["volume"].iloc[0] == 1000000.0

    def test_no_volume_data(self):
        # When volume field is missing, parser should fill with 0.0
        raw = '{"s":[{"i":0,"v":[1609459200,100.0,105.0,99.0,103.0]}]}'
        df = TvDatafeed._TvDatafeed__create_df(raw, "SYM")
        assert isinstance(df, pd.DataFrame)
        assert df["volume"].iloc[0] == 0.0

    def test_malformed_data(self):
        result = TvDatafeed._TvDatafeed__create_df("no data here", "SYM")
        assert result is None


class TestInterval:
    def test_all_values(self):
        assert Interval.in_1_minute.value == "1"
        assert Interval.in_daily.value == "1D"
        assert Interval.in_weekly.value == "1W"
        assert Interval.in_monthly.value == "1M"

    def test_enum_count(self):
        assert len(Interval) == 13


class TestGetHistIntegration:
    @patch("tvDatafeed.main.create_connection")
    @patch.object(TvDatafeed, "_TvDatafeed__auth", return_value="test_token")
    def test_get_hist_returns_dataframe(self, mock_auth, mock_ws_create):
        mock_ws = MagicMock()
        mock_ws_create.return_value = mock_ws

        # Simulate recv() returning data then series_completed
        mock_ws.recv.side_effect = [
            '{"m":"timescale_update","p":["cs_abc",{"s":[{"i":0,"v":[1609459200,100.0,105.0,99.0,103.0,1000000.0]}]}]}',
            '{"m":"series_completed","p":["cs_abc","s1"]}',
        ]

        tv = TvDatafeed()
        df = tv.get_hist("AAPL", "NASDAQ", n_bars=1)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df["symbol"].iloc[0] == "NASDAQ:AAPL"
        assert mock_ws.send.call_count > 0
        mock_ws.close.assert_called_once()  # verify ws is closed

    @patch("tvDatafeed.main.create_connection")
    @patch.object(TvDatafeed, "_TvDatafeed__auth", return_value="test_token")
    def test_get_hist_with_futures(self, mock_auth, mock_ws_create):
        mock_ws = MagicMock()
        mock_ws_create.return_value = mock_ws

        mock_ws.recv.side_effect = [
            '{"m":"timescale_update","p":["cs_abc",{"s":[{"i":0,"v":[1609459200,50.0,55.0,49.0,53.0,500.0]}]}]}',
            '{"m":"series_completed","p":["cs_abc","s1"]}',
        ]

        tv = TvDatafeed()
        df = tv.get_hist("CRUDEOIL", "MCX", fut_contract=1, n_bars=1)

        assert isinstance(df, pd.DataFrame)
        assert df["symbol"].iloc[0] == "MCX:CRUDEOIL1!"
        mock_ws.close.assert_called_once()

    @patch("tvDatafeed.main.create_connection")
    @patch.object(TvDatafeed, "_TvDatafeed__auth", return_value="test_token")
    def test_get_hist_ws_error_returns_none(self, mock_auth, mock_ws_create):
        mock_ws = MagicMock()
        mock_ws_create.return_value = mock_ws
        mock_ws.recv.side_effect = OSError("connection lost")

        tv = TvDatafeed()
        result = tv.get_hist("AAPL", "NASDAQ", n_bars=1)

        assert result is None
        mock_ws.close.assert_called_once()  # verify ws is closed even on error

    @patch("tvDatafeed.main.create_connection")
    @patch.object(TvDatafeed, "_TvDatafeed__auth", return_value="test_token")
    def test_get_hist_generates_fresh_sessions(self, mock_auth, mock_ws_create):
        """Each get_hist call should use fresh session IDs (thread safety)."""
        mock_ws = MagicMock()
        mock_ws_create.return_value = mock_ws
        mock_ws.recv.side_effect = [
            '{"m":"series_completed","p":["cs_abc","s1"]}',
        ]

        tv = TvDatafeed()
        tv.get_hist("AAPL", "NASDAQ", n_bars=1)

        # Capture session IDs from the first call
        first_call_sends = [str(call) for call in mock_ws.send.call_args_list]

        mock_ws.reset_mock()
        mock_ws.recv.side_effect = [
            '{"m":"series_completed","p":["cs_abc","s1"]}',
        ]
        tv.get_hist("AAPL", "NASDAQ", n_bars=1)

        second_call_sends = [str(call) for call in mock_ws.send.call_args_list]

        # Session strings should differ between calls
        assert first_call_sends != second_call_sends


class TestSearchSymbol:
    @patch("tvDatafeed.main.requests.get")
    @patch.object(TvDatafeed, "_TvDatafeed__auth", return_value="test_token")
    def test_search_returns_list(self, mock_auth, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = '[{"symbol":"AAPL","exchange":"NASDAQ","type":"stock"}]'
        mock_get.return_value = mock_resp

        tv = TvDatafeed()
        result = tv.search_symbol("AAPL", "NASDAQ")

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"

    @patch("tvDatafeed.main.requests.get")
    @patch.object(TvDatafeed, "_TvDatafeed__auth", return_value="test_token")
    def test_search_error_returns_empty(self, mock_auth, mock_get):
        mock_get.side_effect = requests.RequestException("network error")

        tv = TvDatafeed()
        result = tv.search_symbol("AAPL")

        assert result == []
