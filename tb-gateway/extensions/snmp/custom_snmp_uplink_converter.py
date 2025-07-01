import time
import os
import sys

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from datetime import timedelta
from puresnmp.types import TimeTicks
from collections import defaultdict
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from parse_interface_data import parse_interface_data
from parse_storage_data import parse_storage_data
from parse_processor_data import parse_processor_data

class CustomSNMPUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        self.SCALE_MAP = {"cpuTemperature": 0.1}  

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        device_name = self.__config['deviceName']
        device_type = self.__config['deviceType']

        self._log.info("Custom SNMP Uplink Converter Dipakai untuk device: %s", device_name)
        converted_data = ConvertedData(device_name=device_name, device_type=device_type)
        device_report_strategy = None
        try:
             device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
             self._log.trace("Report strategy config is not specified for device %s: %s", self.__config['deviceName'], e)

        # Handle named metrics first
        if 'interfaceMetrics' in data:
            try:
                self._log.info("Parsing interface data for device: %s", device_name)
                interface_data = data['interfaceMetrics']
                interfaces = parse_interface_data(interface_data, device_name)
                self._log.info(f"Found {len(interfaces)} interfaces for device: {device_name}")
                
                if interfaces:
                    telemetry_entry = TelemetryEntry({"interfaces": interfaces})
                    converted_data.add_to_telemetry(telemetry_entry)
                        
            except Exception as e:
                self._log.exception("Error parsing interface data for device %s: %s", device_name, str(e))

        if 'storageMetrics' in data:
            try:
                self._log.info("Parsing storage data for device: %s", device_name)
                storage_data = data['storageMetrics']
                storages = parse_storage_data(storage_data, device_name)
                self._log.info(f"Found {len(storages)} storage devices for device: %s", device_name)
                
                if storages:
                    telemetry_entry = TelemetryEntry({"storages": storages})
                    converted_data.add_to_telemetry(telemetry_entry)
                        
            except Exception as e:
                self._log.exception("Error parsing storage data for device %s: %s", device_name, str(e))

        if 'hrProcessorLoad' in data:
            try:
                self._log.info("Parsing processor data for device: %s", device_name)
                processor_data = data['hrProcessorLoad']
                processors = parse_processor_data(processor_data, device_name)
                self._log.info(f"Found {len(processors)} processors for device: %s", device_name)
                
                if processors:
                    telemetry_entry = TelemetryEntry({"processors": processors})
                    converted_data.add_to_telemetry(telemetry_entry)
                        
            except Exception as e:
                self._log.exception("Error parsing processor data for device %s: %s", device_name, str(e))
        
        # Handle direct OIDs (ubah elif menjadi if terpisah)
        interface_oids_present = any(key.startswith('1.3.6.1.2.1.2.2.1.') or key.startswith('1.3.6.1.2.1.31.1.1.1.') for key in data.keys())
        if interface_oids_present and 'interfaceMetrics' not in data:
            try:
                self._log.info("Parsing interface data from direct OIDs for device: %s", device_name)
                interfaces = parse_interface_data(data, device_name)
                self._log.info(f"Found {len(interfaces)} interfaces for device: %s", device_name)
                
                if interfaces:
                    telemetry_entry = TelemetryEntry({"interfaces": interfaces})
                    converted_data.add_to_telemetry(telemetry_entry)
                        
            except Exception as e:
                self._log.exception("Error parsing direct interface OIDs for device %s: %s", device_name, str(e))

        storage_oids_present = any(key.startswith('1.3.6.1.2.1.25.2.3.1.') for key in data.keys())
        if storage_oids_present and 'storageMetrics' not in data:
            try:
                self._log.info("Parsing storage data from direct OIDs for device: %s", device_name)
                storages = parse_storage_data(data, device_name)
                self._log.info(f"Found {len(storages)} storage devices for device: %s", device_name)
                
                if storages:
                    telemetry_entry = TelemetryEntry({"storages": storages})
                    converted_data.add_to_telemetry(telemetry_entry)
                        
            except Exception as e:
                self._log.exception("Error parsing direct storage OIDs for device %s: %s", device_name, str(e))

        processor_oids_present = any(key.startswith('1.3.6.1.2.1.25.3.3.1.2.') for key in data.keys())
        if processor_oids_present and 'hrProcessorLoad' not in data:
            try:
                self._log.info("Parsing processor data from direct OIDs for device: %s", device_name)
                processors = parse_processor_data(data, device_name)
                self._log.info(f"Found {len(processors)} processors for device: %s", device_name)
                
                if processors:
                    telemetry_entry = TelemetryEntry({"processors": processors})
                    converted_data.add_to_telemetry(telemetry_entry)
                        
            except Exception as e:
                self._log.exception("Error parsing direct processor OIDs for device %s: %s", device_name, str(e))

        # Process other telemetry/attributes
        try:
            for datatype in ('attributes', 'telemetry'):
                if datatype not in config:
                    continue
                for datatype_config in config[datatype]:
                    data_key = datatype_config["key"]
                    
                    if data_key in ['interfaceMetrics', 'storageMetrics', 'hrProcessorLoad']:
                        continue
                        
                    item_data = data.get(data_key)
                    if item_data is None:
                        continue
                        
                    scale = self.SCALE_MAP.get(data_key)
                    value = None
                    
                    if isinstance(item_data, TimeTicks):
                        seconds = item_data.value / 100
                        value = str(timedelta(seconds=seconds))
                    elif isinstance(item_data, timedelta):
                        value = str(item_data)
                    elif isinstance(item_data, (int, float)):
                        if scale:
                            value = item_data * scale
                        else:
                            value = item_data
                    elif isinstance(item_data, dict):
                        value = {str(k): str(v) for k, v in item_data.items()}
                    elif isinstance(item_data, list):
                        if item_data and isinstance(item_data[0], str):
                            value = ','.join(item_data)
                        elif item_data and isinstance(item_data[0], dict):
                            res = {}
                            for item in item_data:
                                res.update(**item)
                            value = {str(k): str(v) for k, v in res.items()}
                    elif isinstance(item_data, str):
                        value = item_data
                    elif isinstance(item_data, bytes):
                        try:
                            value = item_data.decode("UTF-8")
                        except UnicodeDecodeError:
                            value = item_data.hex()
                    else:
                        value = str(item_data)

                    if value is not None:
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(
                            data_key, 
                            device_report_strategy,
                            datatype_config, 
                            self._log
                        )
                        
                        if datatype == 'attributes':
                            converted_data.add_to_attributes(datapoint_key, value)
                        else:
                            telemetry_entry = TelemetryEntry({datapoint_key: value})
                            converted_data.add_to_telemetry(telemetry_entry)
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception("Error processing non-interface/storage/processor data for device %s: %s", device_name, str(e))

        self._log.debug(converted_data)
        StatisticsService.count_connector_message(
            self._log.name, 
            'convertersAttrProduced',
            count=converted_data.attributes_datapoints_count
        )
        StatisticsService.count_connector_message(
            self._log.name, 
            'convertersTsProduced',
            count=converted_data.telemetry_datapoints_count
        )

        return converted_data
