import logging
import sys
import json
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any
import os


class JsonFormatter(logging.Formatter):
	"""Structured JSON formatter for log aggregation systems."""
	
	def format(self, record: logging.LogRecord) -> str:
		log_obj = {
			"timestamp": datetime.utcnow().isoformat(),
			"level": record.levelname,
			"logger": record.name,
			"message": record.getMessage(),
			"module": record.module,
			"function": record.funcName,
			"line": record.lineno,
		}
		
		if record.exc_info:
			log_obj["exception"] = self.formatException(record.exc_info)
		
		# Add any extra fields from logging context
		if hasattr(record, 'context'):
			log_obj["context"] = record.context
		
		return json.dumps(log_obj)


class PlainFormatter(logging.Formatter):
	"""Human-readable formatter for console output."""
	
	def format(self, record: logging.LogRecord) -> str:
		timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
		message = f"{timestamp} | {record.levelname:8s} | {record.name} - {record.getMessage()}"
		
		if record.exc_info:
			message += "\n" + self.formatException(record.exc_info)
		
		return message


def setup_logging(
	level: int = logging.INFO,
	log_dir: Optional[str] = None,
	log_file: str = "pipeline.log",
	max_bytes: int = 10 * 1024 * 1024,  # 10MB
	backup_count: int = 5,
	json_format: bool = False
) -> None:
	"""
	Configure logging for production use.
	
	Environment Variables (override arguments):
	    PIPELINE_LOG_DIR: Directory for log files (default: logs/)
	    LOG_FORMAT: 'json' or 'plain' (default: plain)
	    LOG_LEVEL: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)
	
	Args:
	    level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
	    log_dir: Directory for log files. If None, uses logs/ directory
	    log_file: Name of the log file
	    max_bytes: Max size of log file before rotation (default 10MB)
	    backup_count: Number of backup log files to keep
	    json_format: Use JSON format for structured logging (for production)
	
	Example:
	    setup_logging(level=logging.INFO, log_dir="/var/log/mlops")
	    
	Environment-based setup:
	    export PIPELINE_LOG_DIR=/var/log/mlops
	    export LOG_FORMAT=json
	    export LOG_LEVEL=INFO
	    setup_logging()  # Uses env vars
	"""
	# Read from environment variables if not explicitly provided
	if log_dir is None:
		log_dir = os.getenv("PIPELINE_LOG_DIR", "logs")
	
	if not json_format:
		log_format_env = os.getenv("LOG_FORMAT", "plain").lower()
		json_format = log_format_env == "json"
	
	# Parse LOG_LEVEL environment variable
	log_level_env = os.getenv("LOG_LEVEL", "INFO").upper()
	level_map = {
		"DEBUG": logging.DEBUG,
		"INFO": logging.INFO,
		"WARNING": logging.WARNING,
		"ERROR": logging.ERROR,
		"CRITICAL": logging.CRITICAL,
	}
	level = level_map.get(log_level_env, logging.INFO)
	
	root = logging.getLogger()
	
	# Avoid duplicate handlers
	if root.handlers:
		return
	
	root.setLevel(level)
	
	# Formatter selection
	formatter_class = JsonFormatter if json_format else PlainFormatter
	formatter = formatter_class()
	
	# Console Handler (stdout)
	console_handler = logging.StreamHandler(stream=sys.stdout)
	console_handler.setLevel(level)
	console_handler.setFormatter(
		JsonFormatter() if json_format else 
		PlainFormatter()
	)
	root.addHandler(console_handler)
	
	# File Handler with rotation (if log_dir is provided)
	if log_dir:
		log_dir = Path(log_dir)
		log_dir.mkdir(parents=True, exist_ok=True)
		log_path = log_dir / log_file
		
		file_handler = RotatingFileHandler(
			filename=log_path,
			maxBytes=max_bytes,
			backupCount=backup_count
		)
		file_handler.setLevel(level)
		file_handler.setFormatter(formatter)
		root.addHandler(file_handler)


def get_logger(name: str, json_format: bool = False) -> logging.Logger:
	"""
	Get a configured logger instance.
	
	Environment Variables:
	    PIPELINE_LOG_DIR: Directory for log files (default: logs/)
	    LOG_FORMAT: 'json' or 'plain' (default: plain)
	    LOG_LEVEL: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)
	
	Args:
	    name: Logger name (usually __name__)
	    json_format: Use JSON format if root logging not yet configured
	
	Returns:
	    logging.Logger: Configured logger instance
	
	Example:
	    logger = get_logger(__name__)
	    logger.info("Processing started")
	"""
	# Setup if not already configured
	if not logging.getLogger().handlers:
		log_dir = os.getenv("PIPELINE_LOG_DIR", "logs")
		log_format_env = os.getenv("LOG_FORMAT", "plain").lower()
		json_format = json_format or (log_format_env == "json")
		setup_logging(log_dir=log_dir, json_format=json_format)
	
	return logging.getLogger(name)


def log_with_context(logger: logging.Logger, level: int, message: str, 
                     context: Dict[str, Any]) -> None:
	"""
	Log message with structured context (for tracing/debugging).
	
	Args:
	    logger: Logger instance
	    level: Log level
	    message: Log message
	    context: Dictionary of context data (model_key, user_id, request_id, etc.)
	
	Example:
	    log_with_context(
	        logger, 
	        logging.INFO, 
	        "Model training started",
	        {"model_key": "churn", "user": "admin"}
	    )
	"""
	extra = logging.LogRecord(
		name=logger.name,
		level=level,
		pathname="",
		lineno=0,
		msg=message,
		args=(),
		exc_info=None
	)
	extra.context = context
	
	logger.log(level, message, extra={"context": context})


def log_performance(logger: logging.Logger, operation: str, duration_ms: float) -> None:
	"""
	Log performance metrics (useful for monitoring).
	
	Args:
	    logger: Logger instance
	    operation: Operation name (e.g., "model_training", "data_load")
	    duration_ms: Time taken in milliseconds
	
	Example:
	    log_performance(logger, "data_load", 1234.5)
	"""
	logger.info(f"Performance: {operation} completed in {duration_ms:.2f}ms")
