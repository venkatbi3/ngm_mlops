import mlflow.pyfunc
import pandas as pd

class ChurnServingModel(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        self.model = context.artifacts["model"]

    def predict(self, context, model_input):
        if isinstance(model_input, dict):
            model_input = pd.DataFrame([model_input])

        probs = self.model.predict_proba(model_input)[:, 1]
        return pd.DataFrame({"churn_score": probs})
