from abc import ABC, abstractmethod

class BaseInference(ABC):

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def load_features(self):
        pass

    @abstractmethod
    def score(self, model, features):
        pass

    @abstractmethod
    def write_output(self, predictions):
        pass

class BaseTrainer(ABC):

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def load_data(self):
        pass

    @abstractmethod
    def train(self):
        pass

    @abstractmethod
    def evaluate(self, model):
        pass

class BaseValidator(ABC):
    @abstractmethod
    def validate(self, model_uri: str) -> bool:
        pass