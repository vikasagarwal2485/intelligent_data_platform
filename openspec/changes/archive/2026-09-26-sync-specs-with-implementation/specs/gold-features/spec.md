# Spec Delta

## REMOVED Requirements

### Requirement: REQ-GLD-03 — Optional scoring
**Reason**: Contradicts REQ-ML-07. Since phase 13, the Gold stage builds feature
tables only and never loads models; scoring is the separate Score stage
(`idp score`).
**Migration**: Produce `credit_scores`/`fraud_scores` with `idp score` (REQ-ML-07).
Callers that passed models to `to_gold` must call the Score stage instead.
