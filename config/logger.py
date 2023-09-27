import logging

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logging.basicConfig(
    level=logging.INFO,
    format="%(filename)s:%(lineno)d #%(levelname)-8s "
    "[%(asctime)s] - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)
logger.addHandler(console_handler)


def log_func_calls(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Вызвана функция: {func.__name__}")
        return func(*args, **kwargs)

    return wrapper