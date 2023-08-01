import logging

from typing_extensions import override

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import AggregationType

logger = logging.getLogger(__name__)


class BooleanMeasureAggregationRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Converts the expression used in boolean measures so that it can be aggregated."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.agg == AggregationType.SUM_BOOLEAN:
                    if measure.expr:
                        measure.expr = f"CASE WHEN {measure.expr} THEN 1 ELSE 0 END"
                    else:
                        measure.expr = f"CASE WHEN {measure.name} THEN 1 ELSE 0 END"

                    measure.agg = AggregationType.SUM

        return semantic_manifest
