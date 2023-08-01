from typing import Generic, List, Sequence

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import SemanticManifestT
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)


class NonEmptyRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Check if the model contains semantic models and metrics."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has semantic models")
    def _check_model_has_semantic_models(semantic_manifest: PydanticSemanticManifest) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if not semantic_manifest.semantic_models:
            issues.append(
                ValidationError(
                    message="No semantic models present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the model has metrics")
    def _check_model_has_metrics(semantic_manifest: PydanticSemanticManifest) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        # If we are going to generate measure proxy metrics that is sufficient as well
        create_measure_proxy_metrics = False
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.create_metric is True:
                    create_measure_proxy_metrics = True
                    break

        if not semantic_manifest.metrics and not create_measure_proxy_metrics:
            issues.append(
                ValidationError(
                    message="No metrics present in the model.",
                )
            )
        return issues

    @staticmethod
    @validate_safely("running model validation rule ensuring metrics and semantic models are defined")
    def validate_manifest(semantic_manifest: PydanticSemanticManifest) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        issues += NonEmptyRule._check_model_has_semantic_models(semantic_manifest=semantic_manifest)
        issues += NonEmptyRule._check_model_has_metrics(semantic_manifest=semantic_manifest)
        return issues
