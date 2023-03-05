import logging

logging.basicConfig(
    filename="logs/bse_logger_bulkdeal.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p",
)
