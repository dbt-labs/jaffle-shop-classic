from __future__ import annotations

from typing import List, Sequence

from jinja2 import StrictUndefined
from jinja2.exceptions import SecurityError, TemplateSyntaxError, UndefinedError
from jinja2.sandbox import SandboxedEnvironment

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    FilterCallParameterSets,
    ParseWhereFilterException,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.naming.keywords import (
    METRIC_TIME_ELEMENT_NAME,
    is_metric_time_name,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity


class WhereFilterParser:
    """Parses the template in the WhereFilter into FilterCallParameterSets."""

    @staticmethod
    def _exception_message_for_incorrect_format(element_name: str) -> str:
        return (
            f"Name is in an incorrect format: '{element_name}'. It should be of the form: "
            f"<primary entity name>__<dimension_name>"
        )

    @staticmethod
    def parse_call_parameter_sets(where_sql_template: str) -> FilterCallParameterSets:
        """Return the result of extracting the semantic objects referenced in the where SQL template string."""
        # To extract the parameters to the calls, we use a function to record the parameters while rendering the Jinja
        # template. The rendered result is not used, but since Jinja has to render something, using this as a
        # placeholder. An alternative approach would have been to use the Jinja AST API, but this seemed simpler.
        _DUMMY_PLACEHOLDER = "DUMMY_PLACEHOLDER"

        dimension_call_parameter_sets: List[DimensionCallParameterSet] = []
        time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []
        entity_call_parameter_sets: List[EntityCallParameterSet] = []

        def _dimension_call(dimension_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ dimension(...) }}."""
            group_by_item_name = DunderedNameFormatter.parse_name(dimension_name)
            if len(group_by_item_name.entity_links) != 1:
                raise ParseWhereFilterException(
                    WhereFilterParser._exception_message_for_incorrect_format(dimension_name)
                )

            dimension_call_parameter_sets.append(
                DimensionCallParameterSet(
                    dimension_reference=DimensionReference(element_name=group_by_item_name.element_name),
                    entity_path=(
                        tuple(EntityReference(element_name=arg) for arg in entity_path)
                        + group_by_item_name.entity_links
                    ),
                )
            )
            return _DUMMY_PLACEHOLDER

        def _time_dimension_call(
            time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
        ) -> str:
            """Gets called by Jinja when rendering {{ time_dimension(...) }}."""
            group_by_item_name = DunderedNameFormatter.parse_name(time_dimension_name)

            # metric_time is the only time dimension that does not have an associated primary entity, so the
            # GroupByItemName would not have any entity links.
            if is_metric_time_name(group_by_item_name.element_name):
                if len(group_by_item_name.entity_links) != 0 or group_by_item_name.time_granularity is not None:
                    raise ParseWhereFilterException(
                        WhereFilterParser._exception_message_for_incorrect_format(
                            f"Name is in an incorrect format: {time_dimension_name} "
                            f"When referencing {METRIC_TIME_ELEMENT_NAME}, the name should not have any dunders."
                        )
                    )

            else:
                if len(group_by_item_name.entity_links) != 1 or group_by_item_name.time_granularity is not None:
                    raise ParseWhereFilterException(
                        WhereFilterParser._exception_message_for_incorrect_format(time_dimension_name)
                    )

            time_dimension_call_parameter_sets.append(
                TimeDimensionCallParameterSet(
                    time_dimension_reference=TimeDimensionReference(element_name=group_by_item_name.element_name),
                    entity_path=(
                        tuple(EntityReference(element_name=arg) for arg in entity_path)
                        + group_by_item_name.entity_links
                    ),
                    time_granularity=TimeGranularity(time_granularity_name),
                )
            )
            return _DUMMY_PLACEHOLDER

        def _entity_call(entity_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ entity(...) }}."""
            group_by_item_name = DunderedNameFormatter.parse_name(entity_name)
            if len(group_by_item_name.entity_links) > 0 or group_by_item_name.time_granularity is not None:
                WhereFilterParser._exception_message_for_incorrect_format(
                    f"Name is in an incorrect format: {entity_name} "
                    f"When referencing entities, the name should not have any dunders."
                )

            entity_call_parameter_sets.append(
                EntityCallParameterSet(
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                    entity_reference=EntityReference(element_name=entity_name),
                )
            )
            return _DUMMY_PLACEHOLDER

        try:
            SandboxedEnvironment(undefined=StrictUndefined).from_string(where_sql_template).render(
                Dimension=_dimension_call,
                TimeDimension=_time_dimension_call,
                Entity=_entity_call,
            )
        except (UndefinedError, TemplateSyntaxError, SecurityError) as e:
            raise ParseWhereFilterException(f"Error while parsing Jinja template:\n{where_sql_template}") from e

        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_call_parameter_sets),
        )
