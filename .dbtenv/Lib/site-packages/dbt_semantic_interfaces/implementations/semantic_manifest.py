from typing import List

from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.implementations.metric import PydanticMetric
from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.protocols import ProtocolHint, SemanticManifest


class PydanticSemanticManifest(HashableBaseModel, ProtocolHint[SemanticManifest]):
    """Model holds all the information the SemanticLayer needs to render a query."""

    @override
    def _implements_protocol(self) -> SemanticManifest:
        return self

    semantic_models: List[PydanticSemanticModel]
    metrics: List[PydanticMetric]
    project_configuration: PydanticProjectConfiguration
