from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from puresnmp.types import TimeTicks 
import datetime

class SNMPUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        device_name = self.__config['deviceName']
        device_type = self.__config['deviceType']

        converted_data = ConvertedData(device_name=device_name, device_type=device_type)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", self.__config['deviceName'], e)

        try:
            for datatype in ('attributes', 'telemetry'):
                if datatype in config:  # Periksa apakah datatype ada di config
                    for datatype_config in config[datatype]:
                        try:
                            data_key = datatype_config["key"]
                            item_data = data.get(data_key)
                            value = None

                            self._log.debug(f"Processing key '{data_key}'. Raw item_data: {item_data} (Type: {type(item_data)})")

                            if data_key == "sysUpTime":
                                total_seconds = None
                                if isinstance(item_data, TimeTicks):
                                    total_seconds = item_data.value / 100.0
                                elif isinstance(item_data, datetime.timedelta):
                                    total_seconds = item_data.total_seconds()

                                if total_seconds is not None:
                                    days = int(total_seconds // 86400)
                                    hours = int((total_seconds % 86400) // 3600)
                                    minutes = int((total_seconds % 3600) // 60)
                                    value = f"{days} days, {hours} hours, {minutes} minutes"
                                    self._log.info(f"Converted sysUpTime to human-readable: {value}")
                                else:
                                    value = str(item_data)
                                    self._log.warning(f"sysUpTime item_data type not handled for calc: {type(item_data)}, converted to string")

                            elif isinstance(item_data, dict):
                                    value = {str(k): str(v) for k, v in item_data.items()}
                            elif isinstance(item_data, list):
                                if not item_data:
                                    value = []
                                else:
                                    if all(isinstance(item, dict) for item in item_data):
                                        res = {}
                                        for item in item_data:
                                            res.update(item)
                                        value = {str(k): str(v) for k, v in res.items()}
                                    else:
                                        try:
                                            value = ','.join(map(str, item_data))
                                        except Exception:
                                            value = str(item_data)
                                            self._log.warning(f"Unhandled list type for key {data_key}: {item_data}, converted to string")
                            elif isinstance(item_data, str):
                                value = item_data
                            elif isinstance(item_data, bytes):
                                try:
                                    value = item_data.decode("UTF-8")
                                except UnicodeDecodeError:
                                    value = str(item_data)
                                    self._log.warning(f"Bytes decoding error for key {data_key}, using string representation")
                            elif isinstance(item_data, (int, float)):
                                value = item_data
                            else:
                                value = str(item_data)
                                self._log.warning(f"Unhandled data type for key {data_key}: {item_data} (Type: {type(item_data)}), converted to string")

                            if value is not None:
                                datapoint_key = TBUtility.convert_key_to_datapoint_key(data_key, device_report_strategy, datatype_config, self._log)
                                if datatype == 'attributes':
                                    converted_data.add_to_attributes(datapoint_key, value)
                                else:
                                    telemetry_entry = TelemetryEntry({datapoint_key: value})
                                    converted_data.add_to_telemetry(telemetry_entry)
                        
                        except Exception as e:
                            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                            self._log.exception(f"Error processing {datatype_config['key']}: {e}")
        
        except Exception as e:
            self._log.exception(f"Unexpected error in conversion loop: {e}")

        self._log.debug(f"Final converted_data object before return: {converted_data}")
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced', 
                                                count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced', 
                                                count=converted_data.telemetry_datapoints_count)
        
        return converted_data
