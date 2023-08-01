from typing import Set

from typing_extensions import override

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.metric import PydanticMetricInputMeasure
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import MetricType


class AddInputMetricMeasuresRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Add all measures corresponding to the input metrics of the derived metric."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def _get_measures_for_metric(
        semantic_manifest: PydanticSemanticManifest, metric_name: str
    ) -> Set[PydanticMetricInputMeasure]:
        """Returns a unique set of input measures for a given metric."""
        measures: Set = set()
        matched_metric = next(
            iter((metric for metric in semantic_manifest.metrics if metric.name == metric_name)), None
        )
        if matched_metric:
            if matched_metric.type is MetricType.SIMPLE or matched_metric.type is MetricType.CUMULATIVE:
                assert (
                    matched_metric.type_params.measure is not None
                ), f"{matched_metric} should have a measure defined, but it does not."
                measures.add(matched_metric.type_params.measure)
            elif matched_metric.type is MetricType.DERIVED or matched_metric.type is MetricType.RATIO:
                for input_metric in matched_metric.input_metrics:
                    measures.update(
                        AddInputMetricMeasuresRule._get_measures_for_metric(semantic_manifest, input_metric.name)
                    )
            else:
                assert_values_exhausted(matched_metric.type)
        else:
            raise ModelTransformError(f"Metric '{metric_name}' is not configured as a metric in the model.")
        return measures

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for metric in semantic_manifest.metrics:
            measures = AddInputMetricMeasuresRule._get_measures_for_metric(semantic_manifest, metric.name)
            assert len(metric.type_params.input_measures) == 0, f"{metric} should not have measures predefined"
            metric.type_params.input_measures = list(measures)

        return semantic_manifest
