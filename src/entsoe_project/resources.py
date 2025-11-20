from entsoe import EntsoePandasClient
import os

resources = {
    "entsoe_client": EntsoePandasClient(os.getenv("ENTSOE_ACCESS_TOKEN"))
}