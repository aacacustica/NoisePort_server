import logging

import logging
import sys


def setup_logging(
    log_file="data_analysis.log",
    level=logging.DEBUG
):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger.propagate = False

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(
        "%(asctime)s - %(filename)s - "
        "%(name)s - %(levelname)s - %(message)s"
    )

    # Log en archivo
    file_handler = logging.FileHandler(
        log_file,
        mode="a"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Log en consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
