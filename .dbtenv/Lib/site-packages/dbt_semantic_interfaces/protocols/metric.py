from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol, Sequence

from dbt_semantic_interfaces.protocols.metadata import Metadata
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity


class MetricInputMeasure(Protocol):
    """Provides a pointer to a measure along with metric-specific processing directives.

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def filter(self) -> Optional[WhereFilter]:  # noqa: D
        pass

    @property
    @abstractmethod
    def alias(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference associated with this metric input measure."""
        ...

    @property
    @abstractmethod
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate."""
        ...


class MetricTimeWindow(Protocol):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc."""

    @property
    @abstractmethod
    def count(self) -> int:  # noqa: D
        pass

    @property
    @abstractmethod
    def granularity(self) -> TimeGranularity:  # noqa: D
        pass


class MetricInput(Protocol):
    """Provides a pointer to a metric along with the additional properties used on that metric."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def filter(self) -> Optional[WhereFilter]:  # noqa: D
        pass

    @property
    @abstractmethod
    def alias(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def offset_window(self) -> Optional[MetricTimeWindow]:  # noqa: D
        pass

    @property
    @abstractmethod
    def offset_to_grain(self) -> Optional[TimeGranularity]:  # noqa: D
        pass

    @property
    @abstractmethod
    def as_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference associated with this metric input."""
        ...

    @property
    @abstractmethod
    def post_aggregation_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference with the aliased name, if appropriate."""
        pass


class MetricTypeParams(Protocol):
    """Type params add additional context to certain metric types (the context depends on the metric type)."""

    @property
    @abstractmethod
    def measure(self) -> Optional[MetricInputMeasure]:  # noqa: D
        pass

    @property
    @abstractmethod
    def input_measures(self) -> Sequence[MetricInputMeasure]:
        """Return measures needed to compute this metric (including measures needed by parent metrics)."""
        pass

    @property
    @abstractmethod
    def numerator(self) -> Optional[MetricInput]:  # noqa: D
        pass

    @property
    @abstractmethod
    def denominator(self) -> Optional[MetricInput]:  # noqa: D
        pass

    @property
    @abstractmethod
    def expr(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def window(self) -> Optional[MetricTimeWindow]:  # noqa: D
        pass

    @property
    @abstractmethod
    def grain_to_date(self) -> Optional[TimeGranularity]:  # noqa: D
        pass

    @property
    @abstractmethod
    def metrics(self) -> Optional[Sequence[MetricInput]]:  # noqa: D
        pass


class Metric(Protocol):
    """Describes a metric."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def description(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def type(self) -> MetricType:  # noqa: D
        pass

    @property
    @abstractmethod
    def type_params(self) -> MetricTypeParams:  # noqa: D
        pass

    @property
    @abstractmethod
    def filter(self) -> Optional[WhereFilter]:  # noqa: D
        pass

    @property
    @abstractmethod
    def input_measures(self: Metric) -> Sequence[MetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        ...

    @property
    @abstractmethod
    def measure_references(self) -> Sequence[MeasureReference]:
        """Return the measure references associated with all input measure configurations for this metric."""
        ...

    @property
    @abstractmethod
    def input_metrics(self) -> Sequence[MetricInput]:
        """Return the associated input metrics for this metric."""
        ...

    @property
    @abstractmethod
    def metadata(self) -> Optional[Metadata]:  # noqa: D
        pass
