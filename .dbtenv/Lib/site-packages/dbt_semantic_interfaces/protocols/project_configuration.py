from abc import abstractmethod
from typing import Protocol, Sequence

from dbt_semantic_interfaces.protocols.semantic_version import SemanticVersion
from dbt_semantic_interfaces.protocols.time_spine_configuration import (
    TimeSpineTableConfiguration,
)


class ProjectConfiguration(Protocol):
    """Configuration options for the project associated with a semantic manifest."""

    @property
    @abstractmethod
    def dsi_package_version(self) -> SemanticVersion:
        """Version of the dbt-semantic-interfaces package used to define this manifest."""
        pass

    @property
    @abstractmethod
    def time_spine_table_configurations(self) -> Sequence[TimeSpineTableConfiguration]:
        """The time spine table configurations. Multiple allowed for different time grains."""
        pass
