from __future__ import annotations

import argparse
import logging

from inference_ids.bootstrap import build_pipeline
from inference_ids.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the live NTC inference pipeline.")
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    config = load_config(args.config)
    pipeline = build_pipeline(config)
    pipeline.run_forever()


if __name__ == "__main__":
    main()
