from typing import Any, Dict, Optional


class PipelineError(Exception):
	"""Base exception for pipeline errors."""
	
	def __init__(self, message: str, error_code: str = "PIPELINE_ERROR", 
	             context: Optional[Dict[str, Any]] = None):
		"""
		Initialize pipeline error with structured information.
		
		Args:
		    message: Human-readable error message
		    error_code: Machine-readable error code for logging/monitoring
		    context: Additional context (model_key, stage, data, etc.)
		"""
		self.message = message
		self.error_code = error_code
		self.context = context or {}
		super().__init__(self._format_message())
	
	def _format_message(self) -> str:
		"""Format error message with context."""
		msg = f"[{self.error_code}] {self.message}"
		if self.context:
			msg += f" | Context: {self.context}"
		return msg


class DataLoadError(PipelineError):
	"""Raised when loading data fails."""
	
	def __init__(self, message: str, table: Optional[str] = None, 
	             context: Optional[Dict[str, Any]] = None):
		context = context or {}
		if table:
			context['table'] = table
		super().__init__(message, error_code="DATA_LOAD_ERROR", context=context)


class ModelTrainingError(PipelineError):
	"""Raised when model training fails."""
	
	def __init__(self, message: str, model_key: Optional[str] = None,
	             epoch: Optional[int] = None, context: Optional[Dict[str, Any]] = None):
		context = context or {}
		if model_key:
			context['model_key'] = model_key
		if epoch is not None:
			context['epoch'] = epoch
		super().__init__(message, error_code="MODEL_TRAINING_ERROR", context=context)


class ConfigError(PipelineError):
	"""Raised when configuration is invalid or missing."""
	
	def __init__(self, message: str, config_path: Optional[str] = None,
	             context: Optional[Dict[str, Any]] = None):
		context = context or {}
		if config_path:
			context['config_path'] = config_path
		super().__init__(message, error_code="CONFIG_ERROR", context=context)


class ImportErrorSafe(PipelineError):
	"""Raised when dynamic import/attribute resolution fails in a safe manner."""
	
	def __init__(self, message: str, module: Optional[str] = None,
	             attribute: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
		context = context or {}
		if module:
			context['module'] = module
		if attribute:
			context['attribute'] = attribute
		super().__init__(message, error_code="IMPORT_ERROR", context=context)
