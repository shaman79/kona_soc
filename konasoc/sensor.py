import asyncio
import socket
import time
import re
import logging
import random
from datetime import datetime, timezone, timedelta

import voluptuous as vol
from homeassistant.components.sensor import SensorEntity
from homeassistant.const import (
    CONF_HOST, CONF_PORT, CONF_NAME, CONF_SCAN_INTERVAL, PERCENTAGE
)
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.restore_state import RestoreEntity

_LOGGER = logging.getLogger(__name__)

DOMAIN = "konasoc"

# Config keys
CONF_HEADER       = "header"
CONF_RXID         = "response_id"
CONF_PID          = "pid"
CONF_OFFSET       = "offset"
CONF_LENGTH       = "length"     # bytes: 1 or 2 (LE)
CONF_SCALE        = "scale"
CONF_INIT         = "init"
CONF_HOLD_LAST    = "hold_last_value"
CONF_RETRIES      = "retries"           # per update
CONF_CONN_TIMEOUT = "connect_timeout"   # seconds
CONF_IO_TIMEOUT   = "io_timeout"        # seconds for reads
CONF_ATZ_AFTER    = "atz_after_failures"

# Defaults (Display SoC)
DEFAULT_NAME    = "Kona SoC (Display)"
DEFAULT_PORT    = 35000
DEFAULT_HEADER  = "7E4"        # BMS request
DEFAULT_RXID    = "7EC"        # BMS response
DEFAULT_PID     = "220105"     # Display SoC block
DEFAULT_OFFSET  = 24           # byte after '62 01 05'
DEFAULT_LENGTH  = 1            # one byte
DEFAULT_SCALE   = 0.5          # u8 / 2 → percent
DEFAULT_INIT    = ["ATE0","ATL0","ATS0","ATH1","ATSP6","ATAL","ATST64"]
DEFAULT_HOLD    = True
DEFAULT_SCAN_TD = timedelta(seconds=5)

# Resilience
DEFAULT_RETRIES       = 3
DEFAULT_CONNECT_TO    = 8.0
DEFAULT_IO_TO         = 2.0
DEFAULT_ATZ_AFTER     = 8      # issue ATZ on the next init after N consecutive failures

PLATFORM_SCHEMA = cv.PLATFORM_SCHEMA.extend({
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.port,
    vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
    vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_TD): cv.time_period,
    vol.Optional(CONF_HEADER, default=DEFAULT_HEADER): cv.string,
    vol.Optional(CONF_RXID, default=DEFAULT_RXID): cv.string,
    vol.Optional(CONF_PID, default=DEFAULT_PID): cv.string,
    vol.Optional(CONF_OFFSET, default=DEFAULT_OFFSET): cv.positive_int,
    vol.Optional(CONF_LENGTH, default=DEFAULT_LENGTH): vol.In([1, 2]),
    vol.Optional(CONF_SCALE,  default=DEFAULT_SCALE): vol.Coerce(float),
    vol.Optional(CONF_INIT,   default=DEFAULT_INIT): [cv.string],
    vol.Optional(CONF_HOLD_LAST, default=DEFAULT_HOLD): cv.boolean,
    # Resilience knobs
    vol.Optional(CONF_RETRIES, default=DEFAULT_RETRIES): cv.positive_int,
    vol.Optional(CONF_CONN_TIMEOUT, default=DEFAULT_CONNECT_TO): vol.Coerce(float),
    vol.Optional(CONF_IO_TIMEOUT, default=DEFAULT_IO_TO): vol.Coerce(float),
    vol.Optional(CONF_ATZ_AFTER, default=DEFAULT_ATZ_AFTER): cv.positive_int,
})

def _hexpairs(s): return re.findall(r"[0-9A-Fa-f]{2}", s)

class KonaSocSensor(SensorEntity, RestoreEntity):
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_should_poll = True

    def __init__(self, host, port, name, header, rxid, pid, offset, length, scale,
                 init_cmds, scan_td, hold_last, retries, conn_to, io_to, atz_after):
        # Config
        self._host = host
        self._port = port
        self._attr_name = name
        self._header = header.upper()
        self._rxid = rxid.upper()
        self._pid = pid.replace(" ", "").upper()
        self._offset = int(offset)
        self._length = int(length)
        self._scale = float(scale)
        self._init_cmds = init_cmds
        self._interval_seconds = max(1, int(scan_td.total_seconds()))
        self._hold_last = bool(hold_last)
        self._retries = max(1, int(retries))
        self._connect_timeout = float(conn_to)
        self._io_timeout = float(io_to)
        self._atz_after_failures = int(atz_after)

        # State
        self._attr_native_value = None
        self._last_success_utc = None
        self._last_error = None
        self._next_earliest_poll = 0.0
        self._consecutive_failures = 0

    # ----- HA plumbing -----
    @property
    def unique_id(self):
        return f"konasoc_{self._host}_{self._port}_{self._pid}".replace(".", "_")

    @property
    def extra_state_attributes(self):
        attrs = {
            "interval_seconds": self._interval_seconds,
            "pid": self._pid,
            "offset": self._offset,
            "length": self._length,
            "scale": self._scale,
            "retries": self._retries,
            "connect_timeout": self._connect_timeout,
            "io_timeout": self._io_timeout,
            "consecutive_failures": self._consecutive_failures,
        }
        if self._last_success_utc:
            attrs["last_success"] = self._last_success_utc.isoformat()
        if self._last_error:
            attrs["last_error"] = self._last_error
        return attrs

    async def async_added_to_hass(self):
        last_state = await self.async_get_last_state()
        if last_state and last_state.state not in (None, "unknown", "unavailable"):
            try:
                self._attr_native_value = float(last_state.state)
            except (TypeError, ValueError):
                pass

    async def async_update(self):
        now = time.time()
        if now < self._next_earliest_poll:
            return
        self._next_earliest_poll = now + self._interval_seconds

        # Run blocking logic in executor; never raise
        try:
            soc = await asyncio.get_running_loop().run_in_executor(None, self._read_with_retries)
            if soc is not None:
                self._attr_native_value = round(soc, 2)
                self._last_success_utc = datetime.now(timezone.utc)
                self._last_error = None
                self._consecutive_failures = 0
            else:
                if not self._hold_last:
                    self._attr_native_value = None
        except Exception as e:
            # Should not happen, but keep sensor alive
            self._last_error = f"unexpected: {e}"
            _LOGGER.debug("Unexpected SoC update error: %s", e)
            if not self._hold_last:
                self._attr_native_value = None

    # ----- Low-level I/O (sync; runs in executor) -----

    def _send(self, sock, cmd):
        sock.sendall((cmd + "\r").encode("ascii"))

    def _read_until_prompt(self, sock, timeout=None):
        if timeout is None:
            timeout = self._io_timeout
        end = time.time() + timeout
        data = b""
        sock.settimeout(self._io_timeout)
        while time.time() < end:
            try:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                data += chunk
                if b">" in chunk:
                    break
            except socket.timeout:
                break
        return data.decode(errors="ignore")

    def _isotp_payload(self, resp_text):
        # Collect frames with RX CAN ID (e.g. '7EC...')
        lines = [ln for ln in resp_text.split("\r") if ln.strip()]
        frames = [ln.strip() for ln in lines if ln.strip().upper().startswith(self._rxid)]
        if not frames:
            return []
        per_frame = [_hexpairs(ln[len(self._rxid):]) for ln in frames]
        per_frame = [t for t in per_frame if t]
        if not per_frame:
            return []

        first = per_frame[0]
        payload = []
        if len(first) >= 2 and first[0].upper().startswith("10"):
            total_len = int(first[1], 16)
            payload.extend(first[2:])
            for toks in per_frame[1:]:
                if not toks:
                    continue
                payload.extend(toks[1:])  # skip PCI
            if len(payload) > total_len:
                payload = payload[:total_len]
        else:
            for toks in per_frame:
                payload.extend(toks)
        return payload

    def _echo_from_pid(self):
        # PID '22AABB' -> echo '62 AA BB'
        pid = self._pid
        if len(pid) == 6 and pid.startswith("22"):
            return ["62", pid[2:4], pid[4:6]]
        return ["62", "01", "01"]

    def _parse_value_from_payload(self, payload):
        up = [b.upper() for b in payload]
        echo = self._echo_from_pid()
        idx_after = -1
        for i in range(len(up)-2):
            if up[i:i+3] == echo:
                idx_after = i + 3
                break
        if idx_after < 0:
            return None

        i = idx_after + self._offset
        if self._length == 1:
            if i >= len(up): return None
            raw = int(up[i], 16)
        else:
            if i + 1 >= len(up): return None
            raw = (int(up[i+1], 16) << 8) | int(up[i], 16)

        val = raw * self._scale
        return val if 0.0 <= val <= 100.0 else None

    def _init_adapter(self, sock, use_atz=False):
        # Optional ATZ to recover flaky adapters after many failures
        cmds = []
        if use_atz:
            cmds.append("ATZ")
        cmds += self._init_cmds + [f"ATSH{self._header}"]
        for cmd in cmds:
            try:
                self._send(sock, cmd)
                self._read_until_prompt(sock, timeout=self._io_timeout + 0.5)
            except Exception:
                # swallow and continue to keep resilient
                pass

    def _try_once(self, force_atz=False):
        s = socket.create_connection((self._host, self._port), timeout=self._connect_timeout)
        try:
            s.settimeout(self._io_timeout)
            try:
                _ = s.recv(1024)  # flush banner if any
            except Exception:
                pass

            self._init_adapter(s, use_atz=force_atz)

            # Request block
            self._send(s, self._pid)
            resp = self._read_until_prompt(s, timeout=self._io_timeout + 0.5)

            payload = self._isotp_payload(resp)
            if not payload:
                raise RuntimeError("no payload")

            val = self._parse_value_from_payload(payload)
            if val is None:
                raise RuntimeError("parse failed")

            return val

        finally:
            try: s.close()
            except Exception: pass

    def _read_with_retries(self):
        last_err = None
        # Use ATZ if we have hit a high failure streak
        force_atz = (self._consecutive_failures >= self._atz_after_failures)
        for attempt in range(1, self._retries + 1):
            try:
                val = self._try_once(force_atz=force_atz)
                return val
            except Exception as e:
                last_err = str(e)
                # small jitter before retry
                time.sleep(0.2 + random.uniform(0.0, 0.3))
                # Only use ATZ on the first attempt of a "recovery cycle"
                force_atz = False

        # After all retries failed: record and keep last value
        self._consecutive_failures += 1
        self._last_error = f"{self._retries} attempt(s) failed: {last_err}"
        _LOGGER.debug("konasoc read failed (%s)", self._last_error)
        return None

# --- HA setup ---

async def async_setup_platform(hass, config, add_entities, discovery_info=None):
    scan_td = config[CONF_SCAN_INTERVAL]
    sensor = KonaSocSensor(
        host=config[CONF_HOST],
        port=config[CONF_PORT],
        name=config[CONF_NAME],
        header=config[CONF_HEADER],
        rxid=config[CONF_RXID],
        pid=config[CONF_PID],
        offset=config[CONF_OFFSET],
        length=config[CONF_LENGTH],
        scale=config[CONF_SCALE],
        init_cmds=config[CONF_INIT],
        scan_td=scan_td,
        hold_last=config[CONF_HOLD_LAST],
        retries=config[CONF_RETRIES],
        conn_to=config[CONF_CONN_TIMEOUT],
        io_to=config[CONF_IO_TIMEOUT],
        atz_after=config[CONF_ATZ_AFTER],
    )
    add_entities([sensor], True)
