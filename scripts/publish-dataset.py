# /// script
# dependencies = ["huggingface_hub>=1.0"]
# ///
"""
Publish an export produced by /admin -> "Export to Parquet" as a Hugging Face dataset.

Runs on Hugging Face infrastructure, so nobody needs Python, pip or the AWS CLI locally -
only the `hf` CLI they already installed. The Space's data volume is mounted read-write, so
the Job reads the parquet the app wrote and uploads it to a dataset repo:

    hf jobs uv run scripts/publish-dataset.py \
      -v hf://buckets/<you>/lakehouse:/data \
      -e DATASET_REPO=<you>/uk-price-paid \
      -s HF_TOKEN \
      --flavor cpu-basic

Bucket -> repo server-side copy is not available yet, so this is a genuine upload. Doing it
from a Job rather than a laptop means the bytes never cross the venue wifi.
"""
import os
import sys
from pathlib import Path

from huggingface_hub import HfApi

EXPORT_DIR = Path(os.environ.get("EXPORT_DIR", "/data/export/uk-price-paid"))
REPO = os.environ.get("DATASET_REPO")
PRIVATE = os.environ.get("PRIVATE", "1") not in ("0", "false", "False")

if not REPO:
    sys.exit("Set DATASET_REPO, e.g. -e DATASET_REPO=your-name/uk-price-paid")

if not EXPORT_DIR.is_dir():
    sys.exit(
        f"{EXPORT_DIR} does not exist. Run 'Export to Parquet' on the app's /admin page first, "
        "and check the bucket is mounted at /data."
    )

files = sorted((EXPORT_DIR / "data").glob("*.parquet"))
if not files:
    sys.exit(f"No parquet files in {EXPORT_DIR}/data - the export looks empty.")

size_mb = sum(f.stat().st_size for f in files) / 1024 / 1024
print(f"Publishing {len(files)} files ({size_mb:.1f} MB) from {EXPORT_DIR} to {REPO}")

api = HfApi()
api.create_repo(REPO, repo_type="dataset", private=PRIVATE, exist_ok=True)
api.upload_folder(
    folder_path=str(EXPORT_DIR),
    repo_id=REPO,
    repo_type="dataset",
    commit_message=f"Publish {len(files)} parquet files exported from the Iceberg table",
)

print(f"Done: https://huggingface.co/datasets/{REPO}")
print("The Dataset Viewer and its SQL console will appear once the Hub has indexed the parquet.")
