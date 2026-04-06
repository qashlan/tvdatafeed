from __future__ import annotations

import datetime
import enum
import json
import logging
import random
import re
import socket
import string
import threading
import time
from typing import Optional

import pandas as pd
import requests
from websocket import WebSocketConnectionClosedException, create_connection, WebSocket

from tvDatafeed.config import SIGN_IN_URL, SEARCH_URL, WS_URL, WS_TIMEOUT
from tvDatafeed import config as _cfg

logger = logging.getLogger(__name__)


class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"


class TvDatafeed:
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {
        'Referer': 'https://www.tradingview.com',
        'Origin': 'https://www.tradingview.com',
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
    }
    __re_series_data = re.compile(r'"s":\[(.+?)\}\]')
    __re_bar_split = re.compile(r"\[|:|,|\]")

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.token: str = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning(
                "you are using nologin method, data you access may be limited"
            )

        self._persistent_ws: Optional[WebSocket] = None
        self._ws_lock = threading.Lock()

    # ── Persistent connection management ───────────────────────────────────

    @property
    def is_connected(self) -> bool:
        """True if a persistent websocket is open."""
        return self._persistent_ws is not None

    def open_connection(self) -> None:
        """Open a persistent websocket for reuse across multiple get_hist() calls."""
        self.close_connection()
        self._persistent_ws = self.__create_connection()
        self.__send_message(self._persistent_ws, "set_auth_token", [self.token])

    def close_connection(self) -> None:
        """Close the persistent websocket if open."""
        if self._persistent_ws is not None:
            try:
                self._persistent_ws.close()
            except Exception:
                pass
            self._persistent_ws = None

    def __enter__(self):
        self.open_connection()
        return self

    def __exit__(self, *exc):
        self.close_connection()
        return False

    def __auth(self, username: Optional[str], password: Optional[str]) -> Optional[str]:

        if (username is None or password is None):
            return None

        data = {"username": username,
                "password": password,
                "remember": "on"}
        try:
            session = requests.Session()
            session.headers.update(self.__signin_headers)
            # Initial GET to pass CloudFront bot detection
            session.get("https://www.tradingview.com/", timeout=10)
            response = session.post(url=SIGN_IN_URL, data=data, timeout=10)
            return response.json()['user']['auth_token']
        except (requests.RequestException, KeyError, ValueError) as e:
            logger.error('error while signin: %s', e)
            return None

    @staticmethod
    def __create_connection() -> WebSocket:
        logger.debug("creating websocket connection")
        ws = create_connection(
            WS_URL,
            headers=TvDatafeed.__ws_headers,
            timeout=WS_TIMEOUT,
            sockopt=[(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)],
        )
        # Drain the server's initial greeting (session_id message) before
        # sending any commands — without this, the first request races with
        # the greeting and often fails to parse the response correctly.
        try:
            greeting = ws.recv()
            logger.debug("server greeting: %s", greeting[:120] if greeting else "")
        except Exception as e:
            logger.debug("no greeting received: %s", e)
        return ws

    @staticmethod
    def __filter_raw_message(text: str) -> Optional[tuple[str, str]]:
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")
            return None

    @staticmethod
    def __generate_id(prefix: str) -> str:
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters) for _ in range(12))
        return prefix + random_string

    @staticmethod
    def __prepend_header(st: str) -> str:
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func: str, param_list: list) -> str:
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    @staticmethod
    def __create_message(func: str, paramList: list) -> str:
        return TvDatafeed.__prepend_header(TvDatafeed.__construct_message(func, paramList))

    def __send_message(self, ws: WebSocket, func: str, args: list) -> None:
        m = TvDatafeed.__create_message(func, args)
        logger.debug(m)
        with self._ws_lock:
            ws.send(m)

    @staticmethod
    def __create_df(raw_data: str, symbol: str) -> Optional[pd.DataFrame]:
        try:
            out = TvDatafeed.__re_series_data.search(raw_data).group(1)
            x = out.split(',{"')
            data = list()
            volume_data = True

            for xi in x:
                xi = TvDatafeed.__re_bar_split.split(xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4]))

                row = [ts]

                for i in range(5, 10):

                    # skip converting volume data if does not exists
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))

                    except (ValueError, IndexError):
                        volume_data = False
                        row.append(0.0)
                        logger.debug('no volume data')

                data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            data.insert(0, "symbol", value=symbol)
            return data
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")
            return None

    @staticmethod
    def __format_symbol(symbol: str, exchange: str, contract: Optional[int] = None) -> str:

        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def get_hist(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: Optional[int] = None,
        extended_session: bool = False,
    ) -> Optional[pd.DataFrame]:
        """get historical data

        Args:
            symbol (str): symbol name
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True, Defaults to False.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        interval = interval.value

        # Per-call local state for thread safety
        session = self.__generate_id("qs_")
        chart_session = self.__generate_id("cs_")

        # Reuse persistent websocket if available, otherwise create per-call
        own_ws = self._persistent_ws is None
        ws = self.__create_connection() if own_ws else self._persistent_ws

        try:
            if own_ws:
                self.__send_message(ws, "set_auth_token", [self.token])
            self.__send_message(ws, "chart_create_session", [chart_session, ""])
            self.__send_message(ws, "quote_create_session", [session])
            self.__send_message(
                ws,
                "quote_set_fields",
                [
                    session,
                    "ch",
                    "chp",
                    "current_session",
                    "description",
                    "local_description",
                    "language",
                    "exchange",
                    "fractional",
                    "is_tradable",
                    "lp",
                    "lp_time",
                    "minmov",
                    "minmove2",
                    "original_name",
                    "pricescale",
                    "pro_name",
                    "short_name",
                    "type",
                    "update_mode",
                    "volume",
                    "currency_code",
                    "rchp",
                    "rtc",
                ],
            )

            self.__send_message(
                ws, "quote_add_symbols", [session, symbol,
                                      {"flags": ["force_permission"]}]
            )
            self.__send_message(ws, "quote_fast_symbols", [session, symbol])

            self.__send_message(
                ws,
                "resolve_symbol",
                [
                    chart_session,
                    "symbol_1",
                    '={"symbol":"'
                    + symbol
                    + '","adjustment":"splits","session":'
                    + ('"regular"' if not extended_session else '"extended"')
                    + "}",
                ],
            )
            self.__send_message(
                ws,
                "create_series",
                [chart_session, "s1", "s1", "symbol_1", interval, n_bars],
            )
            self.__send_message(ws, "switch_timezone", [
                                chart_session, "exchange"])

            raw_data_parts: list[str] = []

            logger.debug(f"getting data for {symbol}...")
            deadline = time.monotonic() + _cfg.RECV_TIMEOUT
            while time.monotonic() < deadline:
                try:
                    result = ws.recv()
                except (OSError, TimeoutError) as e:
                    logger.error(e)
                    break

                # Echo heartbeat pings back to keep the connection alive
                if result.startswith("~h~"):
                    with self._ws_lock:
                        try:
                            ws.send(result)
                        except Exception:
                            pass
                    continue

                raw_data_parts.append(result)

                if "series_completed" in result:
                    break
            else:
                logger.error(f"recv timeout exceeded for {symbol}")

            raw_data = "\n".join(raw_data_parts)
            return self.__create_df(raw_data, symbol)
        except WebSocketConnectionClosedException:
            logger.error("connection lost for %s", symbol)
            if not own_ws:
                # Persistent connection died — clear it so caller can reconnect
                self._persistent_ws = None
            return None
        finally:
            if own_ws:
                ws.close()

    def search_symbol(self, text: str, exchange: str = '') -> list[dict]:
        url = SEARCH_URL.format(text, exchange)

        symbols_list: list[dict] = []
        try:
            resp = requests.get(url)

            symbols_list = json.loads(resp.text.replace(
                '</em>', '').replace('<em>', ''))
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.error(e)

        return symbols_list


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    tv = TvDatafeed()
    print(tv.get_hist("CRUDEOIL", "MCX", fut_contract=1))
    print(tv.get_hist("NIFTY", "NSE", fut_contract=1))
    print(
        tv.get_hist(
            "EICHERMOT",
            "NSE",
            interval=Interval.in_1_hour,
            n_bars=500,
            extended_session=False,
        )
    )
