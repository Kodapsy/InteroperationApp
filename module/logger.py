# file: module/logger.py
import os
import sys # For printing errors during critical failures
import logging
import logging.handlers
from queue import Queue
from threading import Lock
import functools 
import traceback 


LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'ERROR': logging.ERROR,
    'FATAL': logging.CRITICAL, 
    'CRITICAL': logging.CRITICAL
}

class Logger:
    _instance = None 
    _singleton_init_lock = Lock() 

    def __new__(cls, *args, **kwargs): 
        if not cls._instance:
            with Lock(): 
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, log_dir='../log', max_bytes=10 * 1024 * 1024, backup_count=5):
        if hasattr(self, '_initialized_once') and self._initialized_once:
            return
        
        with Logger._singleton_init_lock: 
            if hasattr(self, '_initialized_once') and self._initialized_once: 
                return

            self.log_dir = os.path.abspath(log_dir) 
            os.makedirs(self.log_dir, exist_ok=True)

            self.queue = Queue(-1)
            self.listener = None
            
            self.configured_loggers_cache = {}  
            self.active_file_handlers_cache = {} 
            
            self.enabled_modules = set()
            self.instance_operations_lock = Lock() 
            self.max_bytes = max_bytes
            self.backup_count = backup_count
            self._initialized_once = True

    def _ensure_listener_updated(self):
        # Must be called when self.instance_operations_lock is held.
        if self.listener:
            self.listener.stop()
            self.listener = None 

        current_handlers_for_new_listener = list(self.active_file_handlers_cache.values())
        
        if not current_handlers_for_new_listener:
            return

        self.listener = logging.handlers.QueueListener(
            self.queue, *current_handlers_for_new_listener, respect_handler_level=True
        )
        try:
            self.listener.start()
        except Exception as e:
            # Fallback for critical listener start failure
            sys.stderr.write(f"CRITICAL LOGGER ERROR: Failed to start QueueListener: {e}\n")
            traceback.print_exc(file=sys.stderr)


    def enable_module(self, module_name):
        with self.instance_operations_lock:
            if module_name not in self.enabled_modules:
                self.enabled_modules.add(module_name)

    def disable_module(self, module_name): 
        with self.instance_operations_lock:
            self.enabled_modules.discard(module_name)

    def get_logger(self, module_name):
        if module_name not in self.enabled_modules: 
             self.enable_module(module_name)
        return _LoggerProxy(self, module_name)

    def _get_or_create_logger_config(self, module_name, level_name_upper):
        # Must be called when self.instance_operations_lock is held.
        key = (module_name, level_name_upper)
        level = LOG_LEVELS[level_name_upper]
        
        needs_listener_update = False 

        if key not in self.configured_loggers_cache:
            internal_logger_name = f"app.{module_name}.{level_name_upper}" 
            actual_logger = logging.getLogger(internal_logger_name)
            actual_logger.setLevel(level) 
            actual_logger.propagate = False
            
            has_correct_qh = any(
                isinstance(h, logging.handlers.QueueHandler) and h.queue is self.queue 
                for h in actual_logger.handlers
            )
            if not has_correct_qh:
                for h_existing in list(actual_logger.handlers): actual_logger.removeHandler(h_existing)
                qh = logging.handlers.QueueHandler(self.queue)
                actual_logger.addHandler(qh)
            self.configured_loggers_cache[key] = actual_logger
        else:
            actual_logger = self.configured_loggers_cache[key]

        if key not in self.active_file_handlers_cache:
            log_file = os.path.join(self.log_dir, f"{module_name}.{level_name_upper.lower()}.log")
            fh = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=self.max_bytes, backupCount=self.backup_count
            )
            fh.setLevel(level) 
            formatter = logging.Formatter('[%(asctime)s] [%(name)s] %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            self.active_file_handlers_cache[key] = fh
            needs_listener_update = True 
        
        if needs_listener_update or self.listener is None:
            self._ensure_listener_updated() 
        
        return actual_logger

    def log(self, module_name, level_name, message):
        with self.instance_operations_lock: 
            if module_name not in self.enabled_modules:
                return
            
            level_upper = level_name.upper()
            if level_upper not in LOG_LEVELS:
                sys.stderr.write(f"LOGGER ERROR: Unknown log level '{level_name}' for '{module_name}'\n")
                return

            actual_logger = self._get_or_create_logger_config(module_name, level_upper)
            actual_logger.log(LOG_LEVELS[level_upper], message)

    def shutdown(self):
        if self.listener:
            try:
                self.listener.stop() 
            except Exception as e:
                sys.stderr.write(f"LOGGER ERROR: Exception while stopping listener: {e}\n")
            self.listener = None
        
        with self.instance_operations_lock:
            for key, handler in list(self.active_file_handlers_cache.items()): 
                try:
                    handler.close()
                except Exception as e:
                    sys.stderr.write(f"LOGGER ERROR: Error closing handler {key}: {e}\n")
            self.active_file_handlers_cache.clear()
            self.configured_loggers_cache.clear()
            self.enabled_modules.clear()


class _LoggerProxy:
    def __init__(self, manager: Logger, module_name: str):
        self.manager = manager
        self.module_name = module_name

    def log(self, level_str_upper, msg_format, *args): 
        if args:
            try:
                final_msg = msg_format % args
            except TypeError: 
                final_msg = " ".join(str(x) for x in (msg_format,) + args)
                sys.stderr.write(f"LOGGER PROXY WARNING: failed to format '{msg_format}' with {args}. Used fallback: '{final_msg}'\n")
        else:
            final_msg = msg_format
        self.manager.log(self.module_name, level_str_upper, final_msg)

    def debug(self, msg_format, *args):
        self.log('DEBUG', msg_format, *args)

    def info(self, msg_format, *args):
        self.log('INFO', msg_format, *args)

    def error(self, msg_format, *args): 
        self.log('ERROR', msg_format, *args)

    def fatal(self, msg_format, *args):
        self.log('CRITICAL', msg_format, *args)
    
    def critical(self, msg_format, *args):
        self.log('CRITICAL', msg_format, *args)

global_logger = Logger(log_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'log')) 


def logger_decorator(module_name: str, level: str = 'INFO', message: str = ''):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger_proxy_instance = global_logger.get_logger(module_name) 

            arg_strs = [repr(a) for a in args] + [f"{k}={v!r}" for k, v in kwargs.items()]
            full_args = ", ".join(arg_strs)
            prefix = f"[{message}] " if message else ""
            entry_msg = f"{prefix}Calling {func.__name__}({full_args})"
            
            log_method = getattr(logger_proxy_instance, level.lower(), logger_proxy_instance.info)

            try:
                log_method(entry_msg)
                result = func(*args, **kwargs)
                log_method(f"{prefix}{func.__name__} returned {result!r}")
                return result
            except Exception as e_deco:
                logger_proxy_instance.error(f"{prefix}{func.__name__} raised: {e_deco!r}")
                logger_proxy_instance.error(f"Traceback for {func.__name__}:\n{traceback.format_exc()}") # Added func name to traceback log
                raise
        return wrapper
    return decorator