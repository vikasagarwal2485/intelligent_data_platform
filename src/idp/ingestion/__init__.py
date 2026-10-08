"""Source-side ingestion: the file landing zone of TM Forum
Open API resources (``idp generate`` / ``idp produce --from-landing``) and the
profile-driven TMF adapter (``idp.ingestion.tmf``).

Bronze itself is written by ``idp.pipeline.to_bronze`` into Iceberg (REQ-BRZ-01).
"""
from idp.ingestion.landing import LandingFile, read_landing, write_landing

__all__ = ["write_landing", "read_landing", "LandingFile"]
