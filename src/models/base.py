from abc import ABC, abstractmethod

class BaseInference(ABC):

    def __init__(self, config: dict):
        self.config = config
        self.metadata = {}  # For storing model metadata during inference

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
        self.X = None  # Store training features
        self.y = None  # Store training labels

    @abstractmethod
    def load_data(self):
        """Load and store X, y as instance variables."""
        pass

    @abstractmethod
    def train(self):
        """Train using self.X and self.y."""
        pass

    @abstractmethod
    def evaluate(self, model):
        """Evaluate using test set."""
        pass

class BaseValidator(ABC):
    @abstractmethod
    def validate(self, model_uri: str) -> bool:
        pass