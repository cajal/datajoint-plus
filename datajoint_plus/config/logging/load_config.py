import envyaml
from pathlib import Path
from . import templates

def replace_special_values(config_dict, replacement_mapping):
    """
    Recursively searches a dictionary and replaces all "special values" in matching strings (syntax designated) according to a mapping.

    Special values are must start with '+' and end with '&':
        e.g. if the special value to be replaced was "filename" then the string must contain "+filename&" for all instances of "filename" to be replaced.      
    :param config_dict: (dict) the Python dictionary to search
    :replacement_mapping: (dict) mapping of special values and their replacements
    :returns: (dict) config_dict with replaced values
    
    Example:
        config_dict = {
            'file': {
                'filename': 'base_dir/+filename&'
            },
            'loggers': {
                'name' : '+name&'
            }
                
        }

        replacement_mapping = {
            'filename': 'main.log',
            'name': 'main'
        }

        replace_special_values(config_dict, replacement_mapping)
            returns: 
                {
                    'file': {
                        'filename': 'base_dir/main.log'
                    },
                    'loggers': {
                        'name' : 'main'
                    }

                }
    """
    for name, value in replacement_mapping.items():
        to_replace = '+' + name + '&'
        for k, v in config_dict.items():
            if isinstance(v, str):
                if to_replace in v:
                    config_dict[k] = v.replace(to_replace, str(value))
                    if config_dict[k].isnumeric():
                        config_dict[k] = int(config_dict[k])
            elif isinstance(v, dict):
                replace_special_values(v, replacement_mapping)
    return config_dict


def import_config_yaml_as_dict(path_to_file=None, search_templates=False, replacement_mapping={}):
    """
    Imports logconfig yaml as a Python dict. Evaluates environment variables designated with ${}.
    
    :param path_to_file: (str or Path) path to yaml file
    :param search_templates: (bool) whether to search djp templates for yaml file 
    :param replacement_mapping: (dict) replacement mapping dict to pass to replace_special_values 
    :returns: Python dict
    """
    if search_templates:
        src = templates.__path__
        if isinstance(src, list) or isinstance(src, tuple):
            src = src[0]
        base_path = Path(src)
        path = base_path.joinpath(path_to_file)
    else:
        path = Path(path_to_file)

    d = envyaml.EnvYAML(path, flatten=False).export().get('logconfig')

    return d if not replacement_mapping else replace_special_values(d, replacement_mapping)