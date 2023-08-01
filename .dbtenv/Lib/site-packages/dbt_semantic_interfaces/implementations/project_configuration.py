from __future__ import annotations

from typing import List, Optional

from importlib_metadata import version
from pydantic import validator
from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.implementations.semantic_version import (
    UNKNOWN_VERSION_SENTINEL,
    PydanticSemanticVersion,
)
from dbt_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.project_configuration import ProjectConfiguration


class PydanticProjectConfiguration(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[ProjectConfiguration]):
    """Pydantic implementation of ProjectConfiguration."""

    @override
    def _implements_protocol(self) -> ProjectConfiguration:
        return self

    time_spine_table_configurations: List[PydanticTimeSpineTableConfiguration]
    metadata: Optional[PydanticMetadata] = None
    dsi_package_version: PydanticSemanticVersion = UNKNOWN_VERSION_SENTINEL

    @validator("dsi_package_version", always=True)
    @classmethod
    def __create_default_dsi_package_version(cls, value: Optional[PydanticSemanticVersion]) -> PydanticSemanticVersion:
        """Returns the version of the dbt_semantic_interfaces package that generated this manifest."""
        if value is not None and value != UNKNOWN_VERSION_SENTINEL:
            return value
        return PydanticSemanticVersion.create_from_string(version("dbt_semantic_interfaces"))
