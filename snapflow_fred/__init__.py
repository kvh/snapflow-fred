from snapflow import SnapflowModule

from .snaps.import_observations import import_fred_observations

module = SnapflowModule(
    "fred",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/fred_observation.yml", "schemas/fred_series.yml"],
    snaps=[import_fred_observations],
)
module.export()
