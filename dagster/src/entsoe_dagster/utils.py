from pathlib import Path

def generate_file_path_from_asset_key(asset_key: list[str], file_type: str = "parquet", partition: bool = False) -> Path:
    asset_key_path = asset_key.copy()

    if partition:
        return Path(*asset_key_path)

    asset_key_path[-1] = asset_key_path[-1] + f".{file_type}"
    return Path(*asset_key_path)