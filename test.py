from dags import Environment
from dags.core.graph import Graph
from dags.utils.common import utcnow
from dags.testing.utils import get_tmp_sqlite_db_url


def test_fred(api_key: str):
    import dags_fred

    env = Environment(metadata_storage="sqlite://")
    g = Graph(env)
    s = env.add_storage(get_tmp_sqlite_db_url())
    env.add_module(dags_fred)
    # Initial graph
    g.add_node(
        "fred_gdp",
        "fred.extract_observations",
        config={"api_key": api_key, "series_id": "gdp"},
    )
    output = env.produce(g, "fred_gdp", target_storage=s)
    records = output.as_records_list()
    assert len(records) >= (utcnow().year - 1946) * 4
    assert len(records) < (utcnow().year + 1 - 1946) * 4


if __name__ == "__main__":
    api_key = input("Enter FRED API key: ")
    if not len(api_key) == 32:
        raise Exception(f"Invalid api key {api_key}")
    test_fred(api_key)