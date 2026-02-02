import logging
import sys


def setup_logging(level: int = logging.INFO):
	root = logging.getLogger()
	if root.handlers:
		return
	handler = logging.StreamHandler(stream=sys.stdout)
	formatter = logging.Formatter(
		"%(asctime)s %(levelname)s %(name)s - %(message)s"
	)
	handler.setFormatter(formatter)
	root.setLevel(level)
	root.addHandler(handler)


def get_logger(name: str):
	setup_logging()
	return logging.getLogger(name)
