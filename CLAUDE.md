# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

tvdatafeed is a Python library for downloading historical and live OHLCV data from TradingView via WebSocket. It's a fork of StreamAlpha's original with added live data streaming capabilities. Requires Python >= 3.9.

## Setup & Development

```bash
# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Or install separately
pip install -e .
pip install -r requirements-dev.txt

# Run all tests
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_main.py -v

# Run a single test
python -m pytest tests/test_main.py::TestCreateDf::test_valid_data -v
```

## Architecture

Five modules in `tvDatafeed/`:

- **config.py** — All configurable constants: URLs, timeouts, retry limits, queue sizes.

- **main.py** — `TvDatafeed`: base class for historical data. Each `get_hist()` call creates its own WebSocket connection, session, and chart session as local variables (thread-safe). Connections are closed in a `finally` block. Parses TradingView's custom `~m~LEN~m~JSON` wire protocol into pandas DataFrames.

- **datafeed.py** — `TvDatafeedLive(TvDatafeed)`: extends base with live streaming. Runs a background `_main_loop` thread that polls at interval boundaries, checks for new bars, and pushes data to consumers. Uses `_SeisesAndTrigger`, a nested dict structure organizing monitored symbols by interval with expiry timestamps. Per-symbol retry failures are isolated (don't kill the entire feed). Uses exponential backoff on retries.

- **seis.py** — `Seis`: immutable container representing a Symbol-Exchange-Interval set. Tracks its registered consumers and caches last-seen data for deduplication via `is_new_data()`.

- **consumer.py** — `Consumer(threading.Thread)`: runs a dedicated thread per callback. Receives data via a bounded queue (maxsize=100) with backpressure. `stop()` sends sentinel and joins the thread.

**Data flow (live):** `TvDatafeedLive._main_loop` → sleeps until next interval expires → calls `get_hist(n_bars=2)` for each expired Seis → checks `is_new_data()` → pushes to each Seis's Consumer threads → Consumer calls user callback.

**Data flow (historical):** `TvDatafeed.get_hist()` → opens ephemeral WebSocket → sends chart session + symbol resolution messages → receives raw data with 30s timeout → `__create_df()` parses into DataFrame → closes WebSocket.

## Key Design Notes

- All concurrency uses `threading` (no asyncio). The live feed relies on `threading.Event` for interval synchronization.
- `get_hist()` is thread-safe: all mutable state (ws, session, chart_session) is local to each call.
- All lock acquire/release pairs use `try/finally` to prevent lock leaks. Consumer threads are joined on cleanup.
- `__init__.py` re-exports `TvDatafeed`, `TvDatafeedLive`, `Interval`, `Seis`, and `Consumer`.
- All modules use `from __future__ import annotations` and have full type hints.
- `__create_df` uses regex+split parsing (not JSON) on raw WebSocket data. The TradingView payload format is `{"i":N,"v":[timestamp,open,high,low,close,volume]}` — after `re.split(r"\[|:|,|\]", ...)`, the timestamp lands at index 4 and OHLCV at indices 5-9. Test data must include the `"i"` field to match real wire format.
- The `_SeisesAndTrigger` internal dict stores `{interval_key: [[seis_list], expiry_datetime]}`. The custom `__getitem__` returns only the seis list (index 0), while `super().__getitem__` returns the full tuple.
