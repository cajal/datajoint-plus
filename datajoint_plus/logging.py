import logging
import logging.config
import os
from pathlib import Path
from datajoint_plus.config.logging.load_config import import_config_yaml_as_dict
from .config import config


def basicConfig(filename=None, level=None, format='%(asctime)s - %(name)s:%(levelname)s:%(message)s', datefmt="%m-%d-%Y %I:%M:%S %p %Z", force=True, **kwargs):
    if filename is not None:
        filename = Path(filename)
    
    level = level if level is not None else config['loglevel']

    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % level)

    logging.basicConfig(filename=filename, level=numeric_level, format=format, datefmt=datefmt, force=force, **kwargs)


def getLogger(name, config_file='console.yml', search_templates=True, level=None, update_root_level=True, **kwargs):
    """
    :param name: (str) name to assign to logging.getLogger object
    :param config_file: (str or Path) path to logconfig file  
    :param search_templates: (bool) whether to search djp templates for config files
    :param level: (str) logger level to overwrite config file
    :param update_root_level: (bool) if True, will update root logger level. 
        Only applicable if level is provided.
    :param kwargs: kwargs to pass as replacement_dict (see datajoint_plus.config.logging.load_config.import_config_yaml_as_dict)
    """
    config = import_config_yaml_as_dict(path_to_file=config_file, search_templates=search_templates, replacement_mapping=kwargs)
    
    # name logger
    config['loggers'][name] = config['loggers'].pop('unnamed')

    # instantiate logger
    logging.config.dictConfig(config)

    # get logger
    logger = logging.getLogger(name)

    # overwrite level
    if level is not None:
        logger.setLevel(level)
        if update_root_level:
            logging.getLogger().setLevel(level) # root logger
    
    return logger


class LogFileManager:
    def __init__(self, name, filename, base_dir=None, config_file=None, search_templates=True, level=None, **kwargs):
        """
        :param name: (str) name to assign to logging.getLogger object
        :param filename: (str or Path) path to log file relative to base_dir
        :param base_dir" (str or Path) base directory for log files
            if None - defaults to config['DJ_LOG_BASE_DIR']
        :param config_file: (str or Path) path to logconfig file  
        :param search_templates: (bool) whether to search djp templates for config files
        :param level: (bool) logger level to overwrite config file
        :param kwargs: kwargs to pass to getLogger 
        """
        self.name = name
        self.level = level
        self.base_dir = Path(os.getenv('DJ_LOG_BASE_DIR')) if base_dir is None else base_dir
        self.filename = Path(filename)
        self.filepath = self.base_dir.joinpath(self.filename)
        self.config_file = config_file
        self._search_templates = search_templates
        self.level = level
        self.kwargs = kwargs

        if not self.filepath.parent.exists():
            self.filepath.parent.mkdir(exist_ok=True, parents=True)

    def __repr__(self):
        return f'LogFileManager({self.name})'

    def __call__(self, method, *args, **kwargs):
        getattr(self.logger, method)(*args, **kwargs)

    @property
    def filepaths(self):
        """
        Returns a list of log files accessible by Logger.
        """
        return [p for p in self.filepath.parent.iterdir() if str(self.filepath.name) in p.name]

    @property
    def logger(self):
        self.kwargs['filename'] = self.filename
        return getLogger(
            self.name, 
            config_file=self.config_file, 
            search_templates=self._search_templates,
            level=self.level,
            **self.kwargs
        )

    def lines(self, log_file_ind=None, return_path=False):
        """
        Returns list of lines in specified log.

        :param log_file_ind: (int) optional, the index of the log file to access from self.paths
            default - None, same as 0
        :param return_path: (bool) if True, returns path of log file
        :returns: list of str
            if return_path is True, also returns path to log file
        """
        if log_file_ind is not None:
            try:
                path = self.filepaths[log_file_ind]
            except IndexError:
                print('No log file at specified index.')
                return
        else:
            path = self.filepath
            if not path.exists():
                print('Log file does not exist yet. Log an entry to create it.')
                return

        with open(path, 'r') as f:
            lines = f.readlines()
            if return_path:
                return lines, path
            else:
                return lines

    def _showlines(self, n_entries, log_file_ind):
        result = self.lines(log_file_ind, return_path=True)
        if result is None:
            return
        lines, path = result
        n_lines = len(lines)
        n_lines_shown = min(n_lines, n_entries)
        return lines, n_lines, n_lines_shown, path

    def head(self, n_entries:int=10, log_file_ind=None):
        """
        Prints log entries from the top of the file.

        :param n_entries: (int) the number of entries to return
        :param log_file_ind: (int) the index of the log file to access from self.paths

        :returns: None
        """
        result = self._showlines(n_entries, log_file_ind)
        if result is None:
            return
        lines, n_lines, n_lines_shown, path = result
        print(f'Showing first {n_lines_shown} entries from ".../{path.name}" out of {n_lines} total entries: \n')
        for line in lines[:n_entries]:
            line = line.strip('\n')
            print(line)

    def tail(self, n_entries:int=10, log_file_ind=None):
        """
        Prints log entries from the end of the file.

        :param n_entries: (int) the number of entries to return
        :param log_file_ind: (int) the index of the log file to access from self.paths

        :returns: None
        """
        result = self._showlines(n_entries, log_file_ind)
        if result is None:
            return
        lines, n_lines, n_lines_shown, path = result
        print(f'Showing last {n_lines_shown} entries from ".../{path.name}" out of {n_lines} total entries: \n')
        for line in lines[::-1][:n_entries][::-1]:
            line = line.strip('\n')
            print(line)


logger = getLogger(__name__)