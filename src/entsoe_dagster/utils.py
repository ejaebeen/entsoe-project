from pathlib import Path

def generate_file_path_from_asset_key(asset_key: list[str], file_type: str = "parquet") -> Path:
    asset_key[-1] = asset_key[-1] + f".{file_type}"
    return Path(*asset_key)