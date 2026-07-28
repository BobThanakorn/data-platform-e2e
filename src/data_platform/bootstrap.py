from __future__ import annotations

import time

from botocore.exceptions import BotoCoreError, ClientError

from data_platform.config import Settings
from data_platform.storage import ensure_buckets


def main() -> None:
    settings = Settings()
    settings.lake_root.mkdir(parents=True, exist_ok=True)
    for layer in ("bronze", "silver", "gold", "manifests"):
        (settings.lake_root / layer).mkdir(parents=True, exist_ok=True)

    for attempt in range(1, 11):
        try:
            ensure_buckets(settings)
            print("MinIO buckets and local lake directories are ready.")
            return
        except (BotoCoreError, ClientError, OSError) as exc:
            if attempt == 10:
                raise
            print(f"MinIO not ready (attempt {attempt}/10): {exc}")
            time.sleep(3)


if __name__ == "__main__":
    main()
