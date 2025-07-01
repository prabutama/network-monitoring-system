import json
import re
from datetime import timedelta

def parse_processor_data(raw_data, device_name=None):
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
    
    base_oid = '1.3.6.1.2.1.25.3.3.1.2'
    
    processors = {}

    for oid, value in data.items():
        if oid.startswith(base_oid + '.'):
            processor_index = oid.split('.')[-1]
            
            if processor_index not in processors:
                processors[processor_index] = {'index': int(processor_index)}
            
            processed_value = value
            if isinstance(value, str):
                if value.isdigit():
                    processed_value = int(value)
            elif isinstance(value, (int, float)):
                processed_value = int(value)
            else:
                try:
                    processed_value = int(str(value))
                except (ValueError, TypeError):
                    processed_value = 0
            
            processors[processor_index]['load'] = processed_value
            processors[processor_index]['load_percent'] = processed_value
            processors[processor_index]['status'] = get_load_status(processed_value)
            processors[processor_index]['level'] = get_load_level(processed_value)
    
    processor_list = list(processors.values())
    if len(processor_list) > 1:
        total_load = sum(p.get('load', 0) for p in processor_list)
        avg_load = total_load / len(processor_list)
        
        system_processor = {
            'index': 0,
            'load': round(avg_load, 2),
            'load_percent': round(avg_load, 2),
            'status': get_load_status(avg_load),
            'level': get_load_level(avg_load),
            'type': 'system_average'
        }
        processor_list.insert(0, system_processor)
    
    return sorted(processors.values(), key=lambda x: x['index'])

def get_load_status(load_percent):
    if load_percent < 50:
        return "normal"
    elif load_percent < 75:
        return "moderate"
    elif load_percent < 90:
        return "high"
    else:
        return "critical"

def get_load_level(load_percent):
    if load_percent < 25:
        return "low"
    elif load_percent < 50:
        return "medium"
    elif load_percent < 75:
        return "high"
    else:
        return "very_high"
