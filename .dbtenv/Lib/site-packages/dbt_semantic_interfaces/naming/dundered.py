from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StructuredDunderedName:
    """Group by items (e.g. dimensions / entities) in a query that are named using a double underscore as a seperator.

    e.g. listing__ds__week ->
    entity_links: ["listing"]
    element_name: "ds"
    granularity: TimeGranularity.WEEK

    The time granularity is part of legacy query syntax and there are plans to migrate away from this format.
    """

    entity_links: Tuple[EntityReference, ...]
    element_name: str
    time_granularity: Optional[TimeGranularity] = None

    @staticmethod
    def parse_name(name: str) -> StructuredDunderedName:
        """Construct from a string like 'listing__ds__month'."""
        name_parts = name.split(DUNDER)

        # No dunder, e.g. "ds"
        if len(name_parts) == 1:
            return StructuredDunderedName((), name_parts[0])

        associated_granularity = None
        granularity: TimeGranularity
        for granularity in TimeGranularity:
            if name_parts[-1] == granularity.value:
                associated_granularity = granularity

        # Has a time granularity
        if associated_granularity:
            #  e.g. "ds__month"
            if len(name_parts) == 2:
                return StructuredDunderedName((), name_parts[0], associated_granularity)
            # e.g. "messages__ds__month"
            return StructuredDunderedName(
                entity_links=tuple(EntityReference(element_name=entity_name) for entity_name in name_parts[:-2]),
                element_name=name_parts[-2],
                time_granularity=associated_granularity,
            )
        # e.g. "messages__ds"
        else:
            return StructuredDunderedName(
                entity_links=tuple(EntityReference(element_name=entity_name) for entity_name in name_parts[:-1]),
                element_name=name_parts[-1],
            )

    @property
    def dundered_name(self) -> str:
        """Return the full name form. e.g. ds or listing__ds__month."""
        items = [entity_reference.element_name for entity_reference in self.entity_links] + [self.element_name]
        if self.time_granularity and self.time_granularity != TimeGranularity.DAY:
            items.append(self.time_granularity.value)
        return DUNDER.join(items)

    @property
    def dundered_name_without_granularity(self) -> str:
        """Return the name without the time granularity. e.g. listing__ds__month -> listing__ds."""
        return DUNDER.join(
            tuple(entity_reference.element_name for entity_reference in self.entity_links) + (self.element_name,)
        )

    @property
    def dundered_name_without_entity(self) -> str:
        """Return the name without the entity. e.g. listing__ds__month -> ds__month."""
        return DUNDER.join((self.element_name,) + ((self.time_granularity.value,) if self.time_granularity else ()))

    @property
    def entity_prefix(self) -> Optional[str]:
        """Return the entity prefix. e.g. listing__ds__month -> listing."""
        if len(self.entity_links) > 0:
            return DUNDER.join(tuple(entity_reference.element_name for entity_reference in self.entity_links))

        return None


class DunderedNameFormatter:
    """Helps to parse names into StructuredDunderedName and vice versa."""

    @staticmethod
    def parse_name(name: str) -> StructuredDunderedName:
        """Construct from a string like 'listing__ds__month'."""
        name_parts = name.split(DUNDER)

        # No dunder, e.g. "ds"
        if len(name_parts) == 1:
            return StructuredDunderedName((), name_parts[0])

        associated_granularity = None
        granularity: TimeGranularity
        for granularity in TimeGranularity:
            if name_parts[-1] == granularity.value:
                associated_granularity = granularity

        # Has a time granularity
        if associated_granularity:
            #  e.g. "ds__month"
            if len(name_parts) == 2:
                return StructuredDunderedName((), name_parts[0], associated_granularity)
            # e.g. "messages__ds__month"
            return StructuredDunderedName(
                entity_links=tuple(EntityReference(element_name=entity_name) for entity_name in name_parts[:-2]),
                element_name=name_parts[-2],
                time_granularity=associated_granularity,
            )
        # e.g. "messages__ds"
        else:
            return StructuredDunderedName(
                entity_links=tuple(EntityReference(element_name=entity_name) for entity_name in name_parts[:-1]),
                element_name=name_parts[-1],
            )

    @staticmethod
    def create_structured_name(  # noqa: D
        element_name: str,
        entity_links: Tuple[EntityReference, ...] = (),
        time_granularity: Optional[TimeGranularity] = None,
    ) -> StructuredDunderedName:
        return StructuredDunderedName(
            entity_links=entity_links,
            element_name=element_name,
            time_granularity=time_granularity,
        )
