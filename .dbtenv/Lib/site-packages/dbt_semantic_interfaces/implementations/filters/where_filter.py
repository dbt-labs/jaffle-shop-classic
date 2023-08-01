from __future__ import annotations

from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets
from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from dbt_semantic_interfaces.parsing.where_filter_parser import WhereFilterParser


class PydanticWhereFilter(PydanticCustomInputParser, HashableBaseModel):
    """A filter applied to the data set containing measures, dimensions, identifiers relevant to the query.

    TODO: Clarify whether the filter applies to aggregated or un-aggregated data sets.

    The data set will contain dimensions as required by the query and the dimensions that a referenced in any of the
    filters that are used in the definition of metrics.
    """

    where_sql_template: str

    @classmethod
    def _from_yaml_value(
        cls,
        input: PydanticParseableValueType,
    ) -> PydanticWhereFilter:
        """Parses a WhereFilter from a string found in a user-provided model specification.

        User-provided constraint strings are SQL snippets conforming to the expectations of SQL WHERE clauses,
        and as such we parse them using our standard parse method below.
        """
        if isinstance(input, str):
            return PydanticWhereFilter(where_sql_template=input)
        else:
            raise ValueError(f"Expected input to be of type string, but got type {type(input)} with value: {input}")

    @property
    def call_parameter_sets(self) -> FilterCallParameterSets:  # noqa: D
        return WhereFilterParser.parse_call_parameter_sets(self.where_sql_template)
