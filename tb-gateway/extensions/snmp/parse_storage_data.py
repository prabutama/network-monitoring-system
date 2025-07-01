import json
import re
from datetime import timedelta

def bytes_to_human(bytes_size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def parse_storage_data(raw_data, device_name=None):  # Tambah parameter device_name
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
        '1.3.6.1.2.1.25.2.3.1.1': 'index',
        '1.3.6.1.2.1.25.2.3.1.2': 'type_oid',
        '1.3.6.1.2.1.25.2.3.1.3': 'name',
        '1.3.6.1.2.1.25.2.3.1.4': 'unit_size',
        '1.3.6.1.2.1.25.2.3.1.5': 'size_units',
        '1.3.6.1.2.1.25.2.3.1.6': 'used_units'
    }
    
    storage_types = {
        '1.3.6.1.2.1.25.2.1.1': 'other',
        '1.3.6.1.2.1.25.2.1.2': 'ram',
        '1.3.6.1.2.1.25.2.1.3': 'virtual_memory',
        '1.3.6.1.2.1.25.2.1.4': 'fixed_disk',
        '1.3.6.1.2.1.25.2.1.5': 'removable_disk',
        '1.3.6.1.2.1.25.2.1.6': 'floppy_disk',
        '1.3.6.1.2.1.25.2.1.7': 'compact_disk',
        '1.3.6.1.2.1.25.2.1.8': 'ram_disk',
        '1.3.6.1.2.1.25.2.1.9': 'flash_memory',
        '1.3.6.1.2.1.25.2.1.10': 'network_disk'
    }
    
    storages = {}

    for oid, value in data.items():
        matched_base_oid = None
        for base_oid, attr_name in base_oids.items():
            if oid.startswith(base_oid + '.'):
                matched_base_oid = base_oid
                break
        
        if matched_base_oid:
            storage_index = oid.split('.')[-1]
            
            if storage_index not in storages:
                storages[storage_index] = {'index': int(storage_index)}
            
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
            storages[storage_index][attr_name] = processed_value

    for storage_index, storage in storages.items():
        if 'type_oid' in storage:
            type_oid = str(storage['type_oid'])
            storage['type'] = storage_types.get(type_oid, 'unknown')
        
        unit_size = storage.get('unit_size', 1)
        size_units = storage.get('size_units', 0)
        used_units = storage.get('used_units', 0)
        
        if isinstance(unit_size, (int, float)) and isinstance(size_units, (int, float)) and isinstance(used_units, (int, float)):
            total_bytes = unit_size * size_units
            used_bytes = unit_size * used_units
            free_bytes = total_bytes - used_bytes
            
            storage['total_bytes'] = int(total_bytes)
            storage['used_bytes'] = int(used_bytes)
            storage['free_bytes'] = int(free_bytes)
            storage['total_human'] = bytes_to_human(total_bytes)
            storage['used_human'] = bytes_to_human(used_bytes)
            storage['free_human'] = bytes_to_human(free_bytes)
            
            if total_bytes > 0:
                usage_percent = (used_bytes / total_bytes) * 100
                storage['usage_percent'] = round(usage_percent, 2)
            else:
                storage['usage_percent'] = 0.0
        else:
            storage['total_bytes'] = 0
            storage['used_bytes'] = 0
            storage['free_bytes'] = 0
            storage['total_human'] = "0 B"
            storage['used_human'] = "0 B"
            storage['free_human'] = "0 B"
            storage['usage_percent'] = 0.0

    return sorted(storages.values(), key=lambda x: x['index'])
