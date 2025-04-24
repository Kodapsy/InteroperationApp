import os
import logging
import logging.handlers
from queue import Queue
from threading import Lock

LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'ERROR': logging.ERROR,
    'FATAL': logging.FATAL,
}

class Logger:
    def __init__(self, log_dir='../log', max_bytes=10 * 1024 * 1024, backup_count=5):
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        self.queue = Queue(-1)
        self.listener = None
        self.loggers = {}  # (module, level) -> logger
        self.handlers = []
        self.enabled_modules = set()
        self.lock = Lock()
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        

    def _start_listener(self):
        if self.listener is None:
            self.listener = logging.handlers.QueueListener(self.queue, *self.handlers, respect_handler_level=True)
            self.listener.start()

    def enable_module(self, module_name):
        with self.lock:
            self.enabled_modules.add(module_name)

    def disable_module(self, module_name):
        with self.lock:
            self.enabled_modules.discard(module_name)

    def get_logger(self, module_name):
        return _LoggerProxy(self, module_name)

    def _get_or_create_logger(self, module_name, level_name):
        key = (module_name, level_name)
        if key in self.loggers:
            return self.loggers[key]

        level = LOG_LEVELS[level_name]
        logger = logging.getLogger(f"{module_name}_{level_name}")
        logger.setLevel(level)
        logger.propagate = False

        log_file = os.path.join(self.log_dir, f"{module_name}.{level_name}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=self.max_bytes, backupCount=self.backup_count
        )
        file_handler.setLevel(level)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        queue_handler = logging.handlers.QueueHandler(self.queue)
        queue_handler.setLevel(level)
        queue_handler.setFormatter(formatter)

        logger.addHandler(queue_handler)

        # 防止重复添加
        if file_handler not in self.handlers:
            self.handlers.append(file_handler)
        
        self._start_listener()
        self.loggers[key] = logger
        return logger

    def log(self, module_name, level_name, message):
        if module_name not in self.enabled_modules:
            return
        logger = self._get_or_create_logger(module_name, level_name)
        logger.log(LOG_LEVELS[level_name], message)

    def shutdown(self):
        if self.listener:
            self.listener.stop()


class _LoggerProxy:
    def __init__(self, manager: Logger, module_name: str):
        self.manager = manager
        self.module_name = module_name

    def log(self, level, msg):
        self.manager.log(self.module_name, level.upper(), msg)

    def debug(self, msg):
        self.log('DEBUG', msg)

    def info(self, msg):
        self.log('INFO', msg)

    def error(self, msg):
        self.log('ERROR', msg)

    def fatal(self, msg):
        self.log('FATAL', msg)


import functools
import traceback

# 创建全局 Logger 实例
global_logger = Logger(log_dir='../log')

def logger_decorator(module_name: str, level: str = 'INFO', message: str = ''):
    """
    装饰器：自动记录函数调用日志，包括参数和返回值。
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = global_logger.get_logger(module_name)

            # 构造参数字符串
            arg_strs = []
            if args:
                arg_strs += [repr(a) for a in args]
            if kwargs:
                arg_strs += [f"{k}={v!r}" for k, v in kwargs.items()]
            full_args = ", ".join(arg_strs)

            prefix = f"[{message}] " if message else ""
            entry_msg = f"{prefix}调用 {func.__name__}({full_args})"

            try:
                logger.log(level, entry_msg)

                result = func(*args, **kwargs)

                logger.log(level, f"{prefix}{func.__name__} 返回 {result!r}")
                return result

            except Exception as e:
                logger.error(f"{prefix}{func.__name__} 异常: {e}")
                logger.error(traceback.format_exc())
                raise

        return wrapper
    return decorator
