import os

from dcp.utils.common import utcnow
from snapflow import Environment, graph, produce


def ensure_api_key() -> str:
    api_key = os.environ.get("FRED_API_KEY")
    if api_key is not None:
        return api_key
    api_key = input("Enter FRED API key: ")
    if not len(api_key) == 32:
        raise Exception(f"Invalid api key {api_key}")
    return api_key


def test_fred():
    api_key = ensure_api_key()

    from snapflow_fred import module as fred

    env = Environment(metadata_storage="sqlite://")

    g = graph()

    # Initial graph
    gdp = g.create_node(
        "fred.import_observations",
        params={"api_key": api_key, "series_id": "gdp"},
    )
    blocks = produce(gdp, env=env, modules=[fred])
    records = blocks[0].as_records()
    assert len(records) >= (utcnow().year - 1946) * 4 - 1
    assert len(records) < (utcnow().year + 1 - 1946) * 4 - 1


if __name__ == "__main__":
    test_fred()
