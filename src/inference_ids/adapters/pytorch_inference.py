from __future__ import annotations

import importlib

import numpy as np
import torch

from inference_ids.domain.models import Prediction


class PyTorchInferenceEngine:
    def __init__(
        self,
        model_module: str,
        model_class: str,
        state_dict_path: str,
        init_kwargs: dict,
        class_names: list[str],
        device: str = "cpu",
        precision: str = "fp32",
    ) -> None:
        module = importlib.import_module(model_module)
        model_cls = getattr(module, model_class)

        self._device = torch.device(device)
        self._dtype = torch.float16 if precision == "fp16" else torch.float32
        self._class_names = class_names

        model = model_cls(**init_kwargs)
        state_dict = torch.load(state_dict_path, map_location=self._device)
        model.load_state_dict(state_dict)
        model.to(self._device, dtype=self._dtype)
        model.eval()
        self._model = model

    @torch.inference_mode()
    def predict(self, features: np.ndarray) -> list[Prediction]:
        tensor = torch.from_numpy(features).to(self._device, dtype=self._dtype)
        logits = self._model(tensor)
        probabilities = torch.softmax(logits, dim=1)
        confidences, indices = torch.max(probabilities, dim=1)

        predictions = []
        for index, confidence, logit_row in zip(indices.tolist(), confidences.tolist(), logits.tolist()):
            predictions.append(
                Prediction(
                    label=self._class_names[index],
                    confidence=confidence,
                    logits=logit_row,
                    class_index=index,
                )
            )
        return predictions
