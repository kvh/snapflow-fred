from typing import TypeVar

from snapflow import SnapflowModule

from .functions.import_observations import import_observations

FredObservation = TypeVar("FredObservation")
FredSeries = TypeVar("FredSeries")

module = SnapflowModule("fred", py_module_path=__file__, py_module_name=__name__,)
module.add_function(import_observations)
