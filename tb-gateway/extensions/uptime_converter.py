from thingsboard_gateway.connectors.converter import Converter
import logging
import time
from datetime import timedelta

log = logging.getLogger("converter")

class UptimeConverter(Converter):
    def __init__(self, config):
        self.__config = config
        self.__prev_values = {}
        log.info("UptimeConverter initialized with timedelta handling")

    def convert(self, config, data):
        try:
            # Handle kemungkinan data timedelta langsung
            if isinstance(data, timedelta):
                log.warning("Received raw timedelta, converting to seconds")
                return self._handle_timedelta(data)
            
            # Handle format data standar [timestamp, value]
            ts = data[0] if isinstance(data, list) and len(data) > 0 else int(time.time()*1000)
            raw_value = data[1] if isinstance(data, list) and len(data) > 1 else data
            
            # Konversi berbagai tipe data ke float
            if isinstance(raw_value, timedelta):
                return self._handle_timedelta(raw_value, ts)
            elif isinstance(raw_value, (int, float)):
                return self._handle_numeric(raw_value, config, ts)
            else:
                try:
                    numeric_value = float(raw_value)
                    return self._handle_numeric(numeric_value, config, ts)
                except:
                    log.error(f"Unsupported data type: {type(raw_value)}")
                    return None
                    
        except Exception as e:
            log.exception(f"Conversion error: {str(e)}")
            return None

    def _handle_timedelta(self, delta, ts=None):
        """Tangani langsung objek timedelta"""
        if ts is None:
            ts = int(time.time()*1000)
            
        uptime_seconds = delta.total_seconds()
        return {
            "ts": ts,
            "values": {
                "uptime_sec": uptime_seconds,
                "uptime_str": self._format_uptime(uptime_seconds)
            }
        }

    def _handle_numeric(self, value, config, ts):
        """Tangani nilai numerik (ticks)"""
        device_name = config.get("device_name", "default")
        prev_value = self.__prev_values.get(device_name, 0)
        
        # Handle counter reset
        if value < prev_value:
            delta = value  # Device rebooted
        else:
            delta = value - prev_value
            
        self.__prev_values[device_name] = value
        
        # Convert ticks to seconds (1 tick = 0.01 second)
        uptime_seconds = delta / 100.0
        
        return {
            "ts": ts,
            "values": {
                "uptime_sec": uptime_seconds,
                "uptime_str": self._format_uptime(uptime_seconds)
            }
        }

    def _format_uptime(self, seconds):
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, _ = divmod(remainder, 60)
        return f"{int(days)}d {int(hours)}h {int(minutes)}m"
