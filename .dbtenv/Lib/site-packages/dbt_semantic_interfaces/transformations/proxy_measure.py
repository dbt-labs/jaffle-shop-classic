import logging

from typing_extensions import override

from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class CreateProxyMeasureRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Adds a proxy metric for measures that have the create_metric flag set, if it does not already exist.

    Also checks that a defined metric with the same name as a measure is a proxy metric.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """Creates measure proxy metrics for measures with `create_metric==True`."""
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if not measure.create_metric:
                    continue

                add_metric = True
                for metric in semantic_manifest.metrics:
                    if metric.name == measure.name:
                        if metric.type != MetricType.SIMPLE:
                            raise ModelTransformError(
                                f"Cannot have metric with the same name as a measure ({measure.name}) that is not a "
                                f"proxy for that measure"
                            )
                        logger.warning(
                            f"Metric already exists with name ({measure.name}). *Not* adding measure proxy metric for "
                            f"that measure"
                        )
                        add_metric = False

                if add_metric is True:
                    semantic_manifest.metrics.append(
                        PydanticMetric(
                            name=measure.name,
                            type=MetricType.SIMPLE,
                            type_params=PydanticMetricTypeParams(
                                measure=PydanticMetricInputMeasure(name=measure.name),
                                expr=measure.name,
                            ),
                        )
                    )

        return semantic_manifest
