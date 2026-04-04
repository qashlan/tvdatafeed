import pytest
import json


# Canned WebSocket response data for mocking get_hist
SAMPLE_RAW_DATA = (
    '~m~1234~m~{"m":"timescale_update","p":["cs_abc123",{"s":[{"v":[1609459200,100.0,105.0,99.0,103.0,1000000.0]},{"v":[1609545600,103.0,108.0,102.0,107.0,1200000.0]}]}]}\n'
    '~m~50~m~{"m":"series_completed","p":["cs_abc123","s1"]}\n'
)

SAMPLE_RAW_DATA_NO_VOLUME = (
    '~m~1234~m~{"m":"timescale_update","p":["cs_abc123",{"s":[{"v":[1609459200,100.0,105.0,99.0,103.0]}]}]}\n'
    '~m~50~m~{"m":"series_completed","p":["cs_abc123","s1"]}\n'
)
