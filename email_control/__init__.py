"""Production-disabled KenigEvents email control-plane foundation."""

from .eligibility import RecommendationAdmissionGate, RecommendationIssue, SendEligibility
from .models import EmailMessage, ProviderResult, Stream

__all__ = [
    "EmailMessage",
    "ProviderResult",
    "RecommendationAdmissionGate",
    "RecommendationIssue",
    "SendEligibility",
    "Stream",
]
