from src.common.logger import get_logger
from src.common.exceptions import PipelineError, DataLoadError


def test_logger_available():
    logger = get_logger("ngm_mlops.test")
    assert logger is not None


def test_exceptions_hierarchy():
    err = DataLoadError("failed")
    assert isinstance(err, PipelineError)
