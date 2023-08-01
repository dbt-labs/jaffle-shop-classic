import logging
from typing import Sequence

from typing_extensions import override

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.add_input_metric_measures import (
    AddInputMetricMeasuresRule,
)
from dbt_semantic_interfaces.transformations.boolean_measure import (
    BooleanMeasureAggregationRule,
)
from dbt_semantic_interfaces.transformations.convert_count import ConvertCountToSumRule
from dbt_semantic_interfaces.transformations.convert_median import (
    ConvertMedianToPercentileRule,
)
from dbt_semantic_interfaces.transformations.names import LowerCaseNamesRule
from dbt_semantic_interfaces.transformations.proxy_measure import CreateProxyMeasureRule
from dbt_semantic_interfaces.transformations.rule_set import (
    SemanticManifestTransformRuleSet,
)
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class PydanticSemanticManifestTransformRuleSet(
    ProtocolHint[SemanticManifestTransformRuleSet[PydanticSemanticManifest]]
):
    """Transform rules that should be used for the Pydantic implementation of SemanticManifest."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRuleSet[PydanticSemanticManifest]:  # noqa: D
        return self

    @property
    def primary_rules(self) -> Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]:  # noqa:
        return (LowerCaseNamesRule(),)

    @property
    def secondary_rules(self) -> Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]:  # noqa: D
        return (
            CreateProxyMeasureRule(),
            BooleanMeasureAggregationRule(),
            ConvertCountToSumRule(),
            ConvertMedianToPercentileRule(),
            AddInputMetricMeasuresRule(),
        )

    @property
    def all_rules(self) -> Sequence[Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]]:  # noqa: D
        return self.primary_rules, self.secondary_rules
