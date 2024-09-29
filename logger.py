import logging


def setup_logger(log_file='app.log', log_level=logging.INFO, to_file=False, to_console=True):
    """
    Setup logger to log messages to a file, console, or both.

    Args:
    log_file (str): The name of the log file (used only if to_file=True).
    log_level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
    to_file (bool): If True, log messages to the specified log file.
    to_console (bool): If True, log messages to the console.

    Returns:
    logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger()

    # Jika logger sudah diatur, langsung return
    if logger.hasHandlers():
        return logger

    logger.setLevel(log_level)
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]')

    # Tambahkan file handler jika to_file=True
    if to_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Tambahkan console handler jika to_console=True
    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger