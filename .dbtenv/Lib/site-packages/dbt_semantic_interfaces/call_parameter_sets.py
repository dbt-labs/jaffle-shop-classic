from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity


@dataclass(frozen=True)
class DimensionCallParameterSet:
    """When 'dimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    dimension_reference: DimensionReference


@dataclass(frozen=True)
class TimeDimensionCallParameterSet:
    """When 'time_dimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    time_dimension_reference: TimeDimensionReference
    time_granularity: TimeGranularity


@dataclass(frozen=True)
class EntityCallParameterSet:
    """When 'entity(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    entity_reference: EntityReference


@dataclass(frozen=True)
class FilterCallParameterSets:
    """The calls for metric items made in the Jinja template of the where filter."""

    dimension_call_parameter_sets: Tuple[DimensionCallParameterSet, ...] = ()
    time_dimension_call_parameter_sets: Tuple[TimeDimensionCallParameterSet, ...] = ()
    entity_call_parameter_sets: Tuple[EntityCallParameterSet, ...] = ()


class ParseWhereFilterException(Exception):  # noqa: D
    pass
