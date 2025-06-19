import asyncio
import logging
import re
import time
from datetime import timedelta

from async_snmp import Snmp, SnmpV1, SnmpV2c, SnmpV3
from async_snmp.exceptions import SnmpError
from async_snmp.security import User
from google.protobuf.json_format import ParseDict
from pysnmp.smi import builder, view
from pysnmp.proto import rfc1902

from thingsboard_gateway.connectors.connector import Connector, GatewaySyncRpcRequest, ServiceCallRpcRequest
from thingsboard_gateway.grpc_tools.generated.attribute_service_pb2 import AttributeService, GwAttributeServiceStub
from thingsboard_gateway.grpc_tools.generated.gw_rpc_svc_pb2 import GwRpcService, GwRpcServiceStub
from thingsboard_gateway.grpc_tools.generated.telemetry_service_pb2 import TelemetryService, GwTelemetryServiceStub
from thingsboard_gateway.grpc_tools.generated.ts_kv_service_pb2 import TsKvService, GwTsKvServiceStub

log = logging.getLogger("snmp_connector")
log.setLevel(logging.DEBUG) # Pastikan level logging DEBUG

class SnmpConnector(Connector):
    def __init__(self, gateway, config):
        super().__init__()
        self.__gateway = gateway
        self.__config = config
        self.__loop = self.__gateway.get_event_loop()
        self.__stopped = False
        self.__clients = {}
        self.__devices = {}
        self.__attribute_updates = [] # Pastikan ini diinisialisasi jika tidak ada di parent
        self.__rpc_requests = []    # Pastikan ini diinisialisasi jika tidak ada di parent
        self.__load_connector_config()
        self.__tb_snmp_user = self.__config.get('tbSnmpUser')

        self.interface_index_to_name = {} # Map untuk menyimpan { "deviceName": { "index": "interfaceName", ... } }
        self.last_interface_metadata_ts = {} # Untuk melacak update per perangkat


    def __load_connector_config(self):
        try:
            for device_config in self.__config.get("devices"):
                try:
                    device_name = device_config["deviceName"]
                    self.__devices[device_name] = {**device_config}
                    self.__devices[device_name]["pollPeriod"] = self.__devices[device_name].get("pollPeriod", self.__config.get("pollPeriod", 60000))
                    
                    for attribute_config in self.__devices[device_name].get("attributes", []):
                        attribute_config["pollPeriod"] = attribute_config.get("pollPeriod", self.__devices[device_name]["pollPeriod"])
                        attribute_config["lastPollTime"] = 0 
                    for telemetry_config in self.__devices[device_name].get("telemetry", []):
                        telemetry_config["pollPeriod"] = telemetry_config.get("pollPeriod", self.__devices[device_name]["pollPeriod"])
                        telemetry_config["lastPollTime"] = 0 

                    self.__attribute_updates.extend(self.__devices[device_name].get("attributeUpdateRequests", []))
                    self.__rpc_requests.extend(self.__devices[device_name].get("serverSideRpcRequests", []))
                except Exception as e:
                    log.exception(f"Error loading device config for {device_config.get('deviceName', 'unknown')}: {e}")
            log.debug(f"Loaded devices: {self.__devices}")
        except Exception as e:
            log.exception(f"Error in __load_connector_config: {e}")

    async def __connect_to_device(self, device):
        device_ip = device["ip"]
        device_port = device["port"]
        community = device.get("community")
        sec_name = device.get("securityName")
        auth_protocol = device.get("authenticationProtocol")
        auth_passphrase = device.get("authenticationPassphrase")
        priv_protocol = device.get("privacyProtocol")
        priv_passphrase = device.get("privacyPassphrase")
        snmp_version = device.get("snmpVersion", "v2c")

        try:
            if snmp_version == "v1":
                self.__clients[device_ip] = SnmpV1(host=device_ip, port=device_port, community=community, loop=self.__loop)
            elif snmp_version == "v2c":
                self.__clients[device_ip] = SnmpV2c(host=device_ip, port=device_port, community=community, loop=self.__loop)
            elif snmp_version == "v3":
                user = User(sec_name=sec_name,
                            auth_protocol=auth_protocol,
                            auth_passphrase=auth_passphrase,
                            priv_protocol=priv_protocol,
                            priv_passphrase=priv_passphrase)
                self.__clients[device_ip] = SnmpV3(host=device_ip, port=device_port, user=user, loop=self.__loop)
            else:
                log.error(f"Unsupported SNMP version: {snmp_version}")
                return False
            log.info(f"Successfully initialized SNMP client for {device_ip}:{device_port}")
            return True
        except Exception as e:
            log.error(f"Failed to initialize SNMP client for {device_ip}:{device_port}. Error: {e}")
            return False

    async def on_server_side_rpc_request(self, content):
        log.debug(f"Received RPC request for SNMP Connector: {content}")
        request = content # Assuming content is already a dict
        self.__loop.call_soon_threadsafe(self.server_side_rpc_handler, request)

    def server_side_rpc_handler(self, content):
        try:
            device = self.__find_device_by_name(content["device"])
            if device is None:
                log.error(f"Device \"{content['device']}\" not found")
                return

            rpc_method_name = content["data"]["method"]

            if self.__check_and_process_reserved_rpc(device, rpc_method_name, content):
                return

            rpc_config = tuple(filter(lambda rpc_config: re.search(
                rpc_method_name, rpc_config['requestFilter']), device["serverSideRpcRequests"]))
            if len(rpc_config):
                self.__process_rpc_request(device, rpc_config[0], content)
            else:
                log.error(f"RPC method \"{rpc_method_name}\" not found")
        except Exception as e:
            log.exception(f"Error in server_side_rpc_handler: {e}")
            self.__gateway.send_rpc_reply(device=content["device"],
                                            req_id=content["data"]["id"],
                                            content={'error': str(e), "success": False})

    def __check_and_process_reserved_rpc(self, device, rpc_method_name, content):
        if rpc_method_name in ('get', 'set'):
            log.debug(f'Processing reserved RPC method: {rpc_method_name}')

            params = {}
            if isinstance(content['data']['params'], str):
                for param in content['data']['params'].split(';'):
                    try:
                        (key, value) = param.split('=')
                        if key and value:
                            params[key] = value
                    except ValueError:
                        log.warning(f"Invalid RPC parameter format: {param}")
                        continue
            else:
                params = content['data']['params']

            if rpc_method_name == 'set':
                content['data']['params'] = params.get('value', content['data']['params'])

            self.__process_rpc_request(device, params, content)
            return True
        return False

    def __process_rpc_request(self, device, rpc_config, content):
        common_parameters = self.__get_common_parameters(device)
        try:
            result = asyncio.run_coroutine_threadsafe(self.__process_methods(rpc_config["method"],
                                                                            common_parameters,
                                                                            {**rpc_config,
                                                                             "value": content["data"]["params"]}),
                                                  loop=self.__loop).result(timeout=int(rpc_config.get("timeout", 5)))
            
            final_result = str(result)
            log.trace(f'RPC result: {final_result}')
            self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                           content={"result": final_result})
        except Exception as e:
            log.exception(f"Error in __process_rpc_request: {e}")
            self.__gateway.send_rpc_reply(device=content["device"],
                                           req_id=content["data"]["id"],
                                           content={'error': str(e), "success": False})

    def on_attributes_update(self, content):
        try:
            device = self.__find_device_by_name(content["device"])
            if device is None:
                log.error(f"Device \"{content['device']}\" not found")
                return

            for attribute_request_config in device["attributeUpdateRequests"]:
                for attribute, value in content["data"].items(): 
                    if re.search(attribute, attribute_request_config["attributeFilter"]):
                        common_parameters = self.__get_common_parameters(device)
                        result = asyncio.run_coroutine_threadsafe(self.__process_methods(attribute_request_config["method"], common_parameters,
                                                                     {**attribute_request_config, "value": value}),
                                                                  loop=self.__loop).result(timeout=int(attribute_request_config.get("timeout", 5)))
                        log.debug(
                            f"Received attribute update request for device \"{content['device']}\" "
                            f"with attribute \"{attribute}\" and value \"{value}\"")
                        log.debug(f"RPC/Attribute update result: {result}")
                        log.debug(f"Full content: {content}")
        except Exception as e:
            log.exception(f"Error in on_attributes_update: {e}")

    def __find_device_by_name(self, device_name):
        device_filter = tuple(filter(lambda device: device["deviceName"] == device_name, self.__devices.values()))
        if len(device_filter):
            return device_filter[0]
        return None

    async def __process_methods(self, method, common_parameters, datatype_config):
        client = common_parameters["client"]
        device_name = common_parameters["device_name"]
        key = datatype_config.get("key", "default_key")

        response = {}

        try:
            if method == "get":
                oid = datatype_config.get("oid")
                if oid:
                    val = await client.get(oid=oid)
                    # Konversi timedelta untuk sysUpTime ke format yang diinginkan
                    if key == "sysUpTime" and isinstance(val, timedelta):
                        total_seconds = int(val.total_seconds())
                        days = total_seconds // (24 * 3600)
                        hours = (total_seconds % (24 * 3600)) // 3600
                        minutes = (total_seconds % 3600) // 60
                        seconds = total_seconds % 60
                        response = {key: f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"}
                    else:
                        response = {key: str(val)}
                else:
                    log.warning(f"OID is missing for GET method with key: {key}")

            elif method == "multiget":
                oids = datatype_config.get("oids")
                if oids and isinstance(oids, list):
                    temp_response = await client.multiget(oids=oids)
                    response = {str(k): str(v) for k, v in temp_response.items()}
                else:
                    log.warning(f"OIDs are missing or not a list for MULTIGET method with key: {key}")

            elif method == "getnext":
                oid = datatype_config.get("oid")
                if oid:
                    master_response = await client.getnext(oid=oid)
                    response = {str(master_response.oid): str(master_response.value)}
                else:
                    log.warning(f"OID is missing for GETNEXT method with key: {key}")
            
            # --- BLOK PEMETAAN INTERFACE NAME (WALK) ---
            elif method == "walk":
                oid_to_walk = datatype_config["oid"]
                
                # Memastikan ini adalah walk untuk interfaceName
                if key == "interfaceName" and oid_to_walk == ".1.3.6.1.2.1.2.2.1.2": 
                    raw_if_descrs = {}
                    async for binded_var in client.walk(oid=oid_to_walk):
                        clean_value = str(binded_var.value).replace("b'", "").strip("'")
                        raw_if_descrs[str(binded_var.oid)] = clean_value
                    
                    log.debug(f"Raw interfaceName from walk for {device_name}: {raw_if_descrs}")

                    new_interface_map = {}
                    for full_oid, clean_name in raw_if_descrs.items():
                        match = re.search(r'\.(\d+)$', full_oid)
                        if match:
                            index = match.group(1)
                            new_interface_map[index] = clean_name
                    
                    self.interface_index_to_name[device_name] = new_interface_map # Simpan per perangkat
                    self.last_interface_metadata_ts[device_name] = int(time.time() * 1000)

                    log.debug(f"Processed interface_index_to_name map for {device_name}: {self.interface_index_to_name[device_name]}")
                    
                    # Mengirim map ini sebagai atribut.
                    # Perhatikan bahwa widget tidak langsung menggunakan atribut ini,
                    # tapi ini penting untuk mapping di bulkwalk telemetry.
                    response = {key: self.interface_index_to_name[device_name]} 
                else: # Jika ini adalah 'walk' biasa untuk OID lain (misalnya hrProcessorLoad)
                    temp_response = {}
                    async for binded_var in client.walk(oid=oid_to_walk):
                        temp_response[str(binded_var.oid)] = str(binded_var.value)
                    response = temp_response
            
            elif method == "multiwalk":
                oids = datatype_config.get("oids")
                if oids and isinstance(oids, list):
                    temp_response = {}
                    async for binded_var in client.multiwalk(oids=oids):
                        temp_response[str(binded_var.oid)] = str(binded_var.value)
                    response = temp_response
                else:
                    log.warning(f"OIDs are missing or not a list for MULTIWALK method with key: {key}")

            elif method == "set":
                oid = datatype_config["oid"]
                value = datatype_config["value"]
                response = await client.set(oid=oid, value=value)
            elif method == "multiset":
                mappings = datatype_config["mappings"]
                response = await client.multiset(mappings=mappings)
            elif method == "bulkget":
                scalar_oids = datatype_config.get("scalarOid", [])
                scalar_oids = scalar_oids if isinstance(scalar_oids, list) else list(scalar_oids)
                repeating_oids = datatype_config.get("repeatingOid", [])
                repeating_oids = repeating_oids if isinstance(repeating_oids, list) else list(repeating_oids)
                max_list_size = datatype_config.get("maxListSize", 1)
                temp_response = await client.bulkget(scalar_oids=scalar_oids, repeating_oids=repeating_oids,
                                                 max_list_size=max_list_size)
                response = temp_response.scalars
            
            # --- BLOK PEMETAAN INTERFACE METRICS (BULKWALK) ---
            elif method == "bulkwalk":
                oids_to_walk = datatype_config["oid"] # Daftar OID root (mis. [".1.3.6.1.2.1.31.1.1.1.6", ...])
                
                if key == "interfaceMetrics": # Logika khusus untuk interfaceMetrics
                    interfaces_data = {} 
                    
                    # Mapping OID root ke nama kunci yang bisa dibaca oleh widget Anda
                    metric_key_map = {
                        ".1.3.6.1.2.1.31.1.1.1.6": "traffic_out_octets", # Byte keluar
                        ".1.3.6.1.2.1.31.1.1.1.10": "traffic_in_octets",  # Byte masuk
                        ".1.3.6.1.2.1.2.2.1.8": "status_code",
                        ".1.3.6.1.2.1.2.2.1.13": "in_discards",
                        ".1.3.6.1.2.1.2.2.1.19": "out_discards",
                        ".1.3.6.1.2.1.31.1.1.1.15": "speed_mbps", # OID untuk ifHighSpeed
                    }
                    
                    log.debug(f"Starting bulkwalk for interfaceMetrics for {device_name} with OIDs: {oids_to_walk}")

                    for oid_root in oids_to_walk:
                        temp_bulkwalk_results = {}
                        async for binded_var in client.bulkwalk(bulk_size=datatype_config.get("bulkSize", 10), oids=[oid_root]):
                            temp_bulkwalk_results[str(binded_var.oid)] = binded_var.value
                        
                        if not temp_bulkwalk_results:
                            log.warning(f"bulkwalk for {oid_root} yielded no results for {device_name}. Skipping this OID root.")
                            continue 
                        
                        for full_oid, value in temp_bulkwalk_results.items():
                            match = re.search(r'\.(\d+)$', full_oid)
                            if match:
                                index = match.group(1)
                                friendly_key = metric_key_map.get(oid_root)

                                if index not in interfaces_data:
                                    interfaces_data[index] = {"interfaceIndex": int(index)}

                                if friendly_key:
                                    try:
                                        interfaces_data[index][friendly_key] = int(value) 
                                    except (ValueError, TypeError):
                                        interfaces_data[index][friendly_key] = str(value)
                            else:
                                log.warning(f"Could not extract index from OID: {full_oid}")
                    
                    log.debug(f"Interfaces raw data after bulkwalk for {device_name}: {interfaces_data}")
                    
                    current_interface_names = self.interface_index_to_name.get(device_name, {})
                    log.debug(f"Current interface_index_to_name map for {device_name}: {current_interface_names}")

                    final_telemetry_array = []
                    if not current_interface_names:
                        log.warning(f"interface_index_to_name map is empty for {device_name}. Interface metrics might have generic names and display issues.")

                    for index, data in interfaces_data.items():
                        # Kunci di objek harus "interface" agar sesuai dengan widget
                        interface_name = current_interface_names.get(index, f"if-{index}")
                        data["interface"] = interface_name 
                        final_telemetry_array.append(data)
                    
                    # Ini adalah *payload* telemetri akhir. Kuncinya adalah "interfaceMetrics"
                    # dan nilainya adalah array objek yang sudah dipetakan.
                    response = {key: final_telemetry_array} 
                    log.debug(f"Final {key} prepared for {device_name}: {response}")

                else: # Jika ini bulkwalk untuk OID lain yang tidak spesifik
                    temp_response = {}
                    for oid_root in oids_to_walk:
                        async for binded_var in client.bulkwalk(bulk_size=datatype_config.get("bulkSize", 10), oids=[oid_root]):
                            temp_response[str(binded_var.oid)] = str(binded_var.value)
                    response = temp_response

            elif method == "table":
                oid = datatype_config["oid"]
                num_base_nodes = datatype_config.get("numBaseNodes", 0)
                response = await client.table(oid=oid)
            elif method == "bulktable":
                oid = datatype_config["oid"]
                num_base_nodes = datatype_config.get("numBaseNodes", 0)
                bulk_size = datatype_config.get("bulkSize", 10)
                response = await client.bulktable(oid=oid, bulk_size=bulk_size)
            else:
                log.error(f"Method \"{method}\" - Not found for key {key}")
                response = {}

        except SnmpError as snmp_e:
            log.error(f"SNMP Error for key {key}, method {method} on {device_name}: {snmp_e}")
            response = {}
        except Exception as e:
            log.exception(f"Error in __process_methods for key {key}, method {method} on {device_name}: {e}")
            response = {}
        
        return response

    def __get_common_parameters(self, device_config):
        return {"client": self.__clients[device_config["ip"]], "device_name": device_config["deviceName"]}

    async def run(self):
        try:
            for device_name, device_config in self.__devices.items():
                if await self.__connect_to_device(device_config):
                    log.info(f"Successfully connected to device: {device_name}")
                    self.__gateway.send_device_disconnect_event(device_name)
                    self.__gateway.send_device_connect_event(device_name, {"deviceType": device_config["deviceType"]})
                else:
                    log.error(f"Failed to connect to device: {device_name}")
                    continue 

            while not self.__stopped:
                for device_name, device_config in self.__devices.items():
                    client = self.__clients.get(device_config["ip"])
                    if client is None or not client.is_connected: # Menambahkan cek is_connected
                        log.warning(f"Client not connected or lost connection for device: {device_name}. Attempting to reconnect...")
                        if not await self.__connect_to_device(device_config):
                            log.error(f"Failed to reconnect to device: {device_name}. Skipping polling cycle.")
                            continue

                    common_parameters = self.__get_common_parameters(device_config)
                    attributes_to_send = {}
                    telemetry_to_send = {}

                    # Collect attributes
                    for attribute_config in device_config.get("attributes", []):
                        current_time = time.time() * 1000
                        if (current_time - attribute_config.get("lastPollTime", 0)) >= attribute_config["pollPeriod"]:
                            attribute_config["lastPollTime"] = current_time
                            result = await self.__process_methods(attribute_config["method"], common_parameters, attribute_config)
                            if result:
                                if isinstance(result, dict):
                                    attributes_to_send.update(result)
                                else:
                                    attributes_to_send[attribute_config["key"]] = result

                    # Collect telemetry
                    for telemetry_config in device_config.get("telemetry", []):
                        current_time = time.time() * 1000
                        if (current_time - telemetry_config.get("lastPollTime", 0)) >= telemetry_config["pollPeriod"]:
                            telemetry_config["lastPollTime"] = current_time
                            result = await self.__process_methods(telemetry_config["method"], common_parameters, telemetry_config)
                            if result:
                                if isinstance(result, dict):
                                    telemetry_to_send.update(result)
                                else:
                                    telemetry_to_send[telemetry_config["key"]] = result

                    if attributes_to_send:
                        log.debug(f"Sending attributes for device {device_name}: {attributes_to_send}")
                        self.__gateway.send_attributes(device_name, attributes_to_send)

                    if telemetry_to_send:
                        log.debug(f"Sending telemetry for device {device_name}: {telemetry_to_send}")
                        self.__gateway.send_telemetry(device_name, telemetry_to_send)

                # Waktu tidur di antara siklus polling penuh untuk semua perangkat
                await asyncio.sleep(self.__config.get("pollPeriod", 60000) / 1000.0) 
        except Exception as e:
            log.exception(f"Error in SNMP connector run loop: {e}")

    def stop(self):
        self.__stopped = True
        for client in self.__clients.values():
            if client:
                client.close()
        log.info("SNMP Connector stopped.")
