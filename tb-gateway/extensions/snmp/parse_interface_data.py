import json
import re
import time
from datetime import timedelta

_interface_history = {}

def parse_interface_data(raw_data, device_name=None):
    global _interface_history
    current_time = time.time()
    
    if device_name is None:
        device_name = "default"
    
    if device_name not in _interface_history:
        _interface_history[device_name] = {}
    
    device_history = _interface_history[device_name]
    
    if isinstance(raw_data, dict):
        data = raw_data
    elif isinstance(raw_data, str):
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            try:
                data = eval(raw_data)
            except:
                raise ValueError("Cannot parse raw_data as JSON or dict")
    else:
        raise ValueError("raw_data must be dict or string")
    
    base_oids = {
        '1.3.6.1.2.1.2.2.1.2': 'ifDescr',
        '1.3.6.1.2.1.2.2.1.3': 'ifType',
        '1.3.6.1.2.1.2.2.1.4': 'ifMtu',
        '1.3.6.1.2.1.2.2.1.8': 'ifOperStatus',
        '1.3.6.1.2.1.2.2.1.9': 'ifLastChange',
        '1.3.6.1.2.1.2.2.1.13': 'ifInDiscards',
        '1.3.6.1.2.1.2.2.1.14': 'ifInErrors',
        '1.3.6.1.2.1.2.2.1.20': 'ifOutErrors',
        '1.3.6.1.2.1.31.1.1.1.6': 'ifHCInOctets',
        '1.3.6.1.2.1.31.1.1.1.10': 'ifHCOutOctets',
        '1.3.6.1.2.1.31.1.1.1.15': 'ifHighSpeed'
    }
    
    interfaces = {}

    for oid, value in data.items():
        matched_base_oid = None
        for base_oid, attr_name in base_oids.items():
            if oid.startswith(base_oid + '.'):
                matched_base_oid = base_oid
                break
        
        if matched_base_oid:
            if_index = oid.split('.')[-1]
            
            if if_index not in interfaces:
                interfaces[if_index] = {'ifIndex': int(if_index)}
            
            processed_value = value
            if isinstance(value, str):
                if value.startswith("b'") and value.endswith("'"):
                    processed_value = value[2:-1]
                elif value.startswith('b"') and value.endswith('"'):
                    processed_value = value[2:-1]
                elif value.isdigit():
                    processed_value = int(value)
            elif isinstance(value, bytes):
                try:
                    processed_value = value.decode('utf-8')
                except UnicodeDecodeError:
                    processed_value = str(value)
            elif isinstance(value, timedelta):
                processed_value = str(value)
            elif isinstance(value, (int, float)):
                processed_value = value
            else:
                try:
                    json.dumps(value)
                    processed_value = value
                except (TypeError, ValueError):
                    processed_value = str(value)
            
            attr_name = base_oids[matched_base_oid]
            interfaces[if_index][attr_name] = processed_value

    for if_index, interface in interfaces.items():
        if_idx = int(if_index)
        
        in_octets = interface.get('ifHCInOctets', 0)
        out_octets = interface.get('ifHCOutOctets', 0)
        
        interface['ifInThroughputBps'] = 0
        interface['ifOutThroughputBps'] = 0
        
        if if_idx in device_history:
            prev_data = device_history[if_idx]
            time_diff = current_time - prev_data['timestamp']
            
            if time_diff > 5:
                def calculate_delta(current, previous, is_64bit=True):
                    if current >= previous:
                        return current - previous
                    else:
                        if is_64bit:
                            return (18446744073709551615 - previous) + current + 1
                        else:
                            return (4294967295 - previous) + current + 1
                
                in_delta = calculate_delta(in_octets, prev_data['in_octets'])
                out_delta = calculate_delta(out_octets, prev_data['out_octets'])
                
                in_bps = int((in_delta * 8) / time_diff)
                out_bps = int((out_delta * 8) / time_diff)
                
                interface['ifInThroughputBps'] = in_bps
                interface['ifOutThroughputBps'] = out_bps
        
        device_history[if_idx] = {
            'timestamp': current_time,
            'in_octets': in_octets,
            'out_octets': out_octets
        }
    
    cutoff_time = current_time - (24 * 60 * 60)
    to_remove = []
    for if_idx, data in device_history.items():
        if data['timestamp'] < cutoff_time:
            to_remove.append(if_idx)
    
    for if_idx in to_remove:
        del device_history[if_idx]
    
    return sorted(interfaces.values(), key=lambda x: x['ifIndex'])

def get_interface_history():
    return _interface_history

def clear_interface_history():
    global _interface_history
    _interface_history = {}
