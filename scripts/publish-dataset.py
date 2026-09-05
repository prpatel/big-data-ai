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

Where the export sits inside the bucket depends on how the app was running when it wrote it,
which is not obvious and is easy to get wrong:

    Spaces  -> app/export/<name>    (docker-entrypoint.sh symlinks /app/data to $DATA_ROOT/app)
    compose -> export/<name>        (the app service mounts DATA_ROOT straight at /app/data)

So this looks in both, and then falls back to searching the mount. Set EXPORT_DIR to skip that.

Bucket -> repo server-side copy is not available yet, so this is a genuine upload. Doing it
from a Job rather than a laptop means the bytes never cross the venue wifi.
"""
import os
import sys
from pathlib import Path

from huggingface_hub import HfApi

MOUNT = Path(os.environ.get("MOUNT", "/data"))
NAME = os.environ.get("EXPORT_NAME", "uk-price-paid")
REPO = os.environ.get("DATASET_REPO")
PRIVATE = os.environ.get("PRIVATE", "1") not in ("0", "false", "False")


def find_export():
    """Locate the export directory: an explicit setting, then the two known layouts, then a search."""
    explicit = os.environ.get("EXPORT_DIR")
    if explicit:
        return Path(explicit)
    for candidate in (MOUNT / "app" / "export" / NAME,   # Spaces
                      MOUNT / "export" / NAME):          # compose
        if (candidate / "data").is_dir():
            return candidate
    # Last resort: the layout changed, or the app ran somewhere unusual.
    for marker in MOUNT.glob(f"**/export/{NAME}/data"):
        return marker.parent
    return None


if not REPO:
    sys.exit("Set DATASET_REPO, e.g. -e DATASET_REPO=your-name/uk-price-paid")

EXPORT_DIR = find_export()
if EXPORT_DIR is None:
    sys.exit(
        f"Could not find an export named '{NAME}' under {MOUNT}.\n"
        f"Looked in {MOUNT}/app/export/{NAME} (Spaces) and {MOUNT}/export/{NAME} (compose).\n"
        "Run 'Export to Parquet' on the app's /admin page first, check the bucket is mounted "
        "at /data, or set EXPORT_DIR explicitly."
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
