import numpy as np
import torch

from inference_ids.adapters.pytorch_inference import PyTorchInferenceEngine
from inference_ids.reference_model import IDSModel


def test_predict_returns_one_prediction_per_row(tmp_path):
    model = IDSModel(input_features=4, num_classes=3)
    model.eval()
    state_dict_path = tmp_path / "model.pth"
    torch.save(model.state_dict(), state_dict_path)

    engine = PyTorchInferenceEngine(
        model_module="inference_ids.reference_model",
        model_class="IDSModel",
        state_dict_path=str(state_dict_path),
        init_kwargs={"input_features": 4, "num_classes": 3},
        class_names=["benign", "scan", "dos"],
        device="cpu",
        precision="fp32",
    )

    features = np.random.rand(5, 4).astype(np.float32)
    predictions = engine.predict(features)

    assert len(predictions) == 5
    for prediction in predictions:
        assert prediction.label in {"benign", "scan", "dos"}
        assert 0.0 <= prediction.confidence <= 1.0
        assert len(prediction.logits) == 3


def test_predict_confidence_matches_argmax_probability(tmp_path):
    model = IDSModel(input_features=2, num_classes=2)
    model.eval()
    state_dict_path = tmp_path / "model.pth"
    torch.save(model.state_dict(), state_dict_path)

    engine = PyTorchInferenceEngine(
        model_module="inference_ids.reference_model",
        model_class="IDSModel",
        state_dict_path=str(state_dict_path),
        init_kwargs={"input_features": 2, "num_classes": 2},
        class_names=["a", "b"],
        device="cpu",
        precision="fp32",
    )

    features = np.array([[0.1, 0.2]], dtype=np.float32)
    [prediction] = engine.predict(features)

    probabilities = torch.softmax(torch.tensor(prediction.logits), dim=0)
    expected_confidence = probabilities.max().item()
    assert prediction.confidence == expected_confidence
