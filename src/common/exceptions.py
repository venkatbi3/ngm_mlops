class PipelineError(Exception):
	"""Base exception for pipeline errors."""


class DataLoadError(PipelineError):
	"""Raised when loading data fails."""


class ModelTrainingError(PipelineError):
	"""Raised when model training fails."""


class ConfigError(PipelineError):
	"""Raised when configuration is invalid or missing."""


class ImportErrorSafe(PipelineError):
	"""Raised when dynamic import/attribute resolution fails in a safe manner."""

