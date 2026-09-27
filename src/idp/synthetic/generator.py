"""Synthetic data generator (plan section 3.0).

Produces referentially-consistent records across all seven TMF entities
(parties -> accounts -> agreements -> products, plus voice/data usage and
recharge events) for one or more tenants, together with a ground-truth label
table for the two ML use cases.

Design goals (from reviewer decision "synthetic data generation is required"):

* **Coherent**: a subscriber's MSISDN/IMSI/IMEI is consistent across its
  agreement and all of its usage/recharge events.
* **Multi-tenant**: every record is stamped with ``tenant_id``.
* **Labelled & learnable**: a configurable fraction of subscribers exhibit
  fraud (sim_box / card_fraud / roaming_fraud) with *matching* behaviour, and
  a configurable fraction default on extended credit. The ``true_credit_limit``
  is a deterministic function of behaviour so the regression target is real.

The generator is fully deterministic given a seed.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

FRAUD_TYPES = ("sim_box", "card_fraud", "roaming_fraud")
# The categorical values below double as Avro enum symbols (config/avro/*.avsc),
# so they must be Avro-enum-safe: letters/digits/underscore only, no hyphens.
PAYMENT_METHODS = ("credit_card", "cash", "voucher")
NETWORK_TECH = ("3G", "4G", "4G+", "5G")
ACCOUNT_TYPES = ("salaried", "self-employed", "student", "retired", "company", "normal")
SEGMENTS = ("B2C", "B2B", "B2B2C")
BUSINESS_LINES = ("mobile", "broadband", "iptv", "fixed_line_telephony", "iot", "energy", "insurance")

# Product price book: name -> (category, monthly_fee EUR). Categories mirror the
# TMF product categories used by fee charges. Fees drive product-fee revenue.
PRODUCT_CATALOG = {
    "Data 1 GB": ("addon", 9.99),
    "Data 5 GB": ("addon", 19.99),
    "Voice 100": ("subscription", 14.99),
    "Unlimited Social": ("addon", 7.99),
    "Roaming Pass": ("roaming", 12.99),
}
PRODUCT_NAMES = tuple(PRODUCT_CATALOG)
# Recharge channels (weighted). Avro-enum-safe symbols (config/avro/recharge_event.avsc);
# was a constant "manual", which made the recharge-type split degenerate for analytics.
RECHARGE_TYPES = ("voucher", "topup")
RECHARGE_TYPE_WEIGHTS = (0.4, 0.6)


@dataclass
class SyntheticDataset:
    """Container of generated entity/event/label DataFrames."""

    parties: pd.DataFrame
    accounts: pd.DataFrame
    agreements: pd.DataFrame
    products: pd.DataFrame
    voice_events: pd.DataFrame
    data_events: pd.DataFrame
    recharge_events: pd.DataFrame
    fee_charges: pd.DataFrame
    labels: pd.DataFrame

    def tables(self) -> dict[str, pd.DataFrame]:
        return asdict(self)  # type: ignore[return-value]

    @property
    def event_tables(self) -> dict[str, pd.DataFrame]:
        return {
            "voice_events": self.voice_events,
            "data_events": self.data_events,
            "recharge_events": self.recharge_events,
            "fee_charges": self.fee_charges,
        }


# The source entity tables of a SyntheticDataset (every field but ``labels``), in
# landing order. This is the one definition: the landing zone, Bronze
# (``pipeline.ENTITIES``) and the Kafka topic map all cover exactly these.
ENTITIES = (
    "parties", "accounts", "agreements", "products",
    "voice_events", "data_events", "recharge_events", "fee_charges",
)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def generate(
    tenant_id: str,
    *,
    subscribers: int = 200,
    days: int = 60,
    fraud_rate: float = 0.05,
    default_rate: float = 0.12,
    seed: int = 42,
    start_date: datetime | None = None,
) -> SyntheticDataset:
    """Generate a coherent, labelled dataset for a single tenant."""
    # Deterministic per-tenant seed (builtin hash() is randomized per process).
    digest = hashlib.sha256(f"{tenant_id}:{seed}".encode()).hexdigest()
    rng = np.random.default_rng(int(digest[:8], 16))
    # Separate stream for the additive analytics fields (recharge_type + fee
    # charges) so the main `rng` sequence — which drives behaviour and labels —
    # stays byte-identical to before these fields existed (ADR-0022).
    aux_rng = np.random.default_rng(int(digest[8:16], 16))
    if start_date is None:
        start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)

    parties, accounts, agreements, products = [], [], [], []
    voice_rows, data_rows, recharge_rows, label_rows = [], [], [], []
    fee_rows: list[dict] = []

    # Month-start timestamps within the generation window (for recurring fees).
    month_starts: list[datetime] = []
    _m = start_date.replace(day=1)
    _end = start_date + timedelta(days=days)
    while _m < _end:
        if _m >= start_date:
            month_starts.append(_m)
        _m = (_m.replace(day=28) + timedelta(days=7)).replace(day=1)

    is_fraud_all = rng.random(subscribers) < fraud_rate
    fraud_type_idx = rng.integers(0, len(FRAUD_TYPES), subscribers)

    for i in range(subscribers):
        suffix = f"{i:06d}"
        party_id = f"{tenant_id}-party-{suffix}"
        account_id = f"{tenant_id}-acct-{suffix}"
        agreement_id = f"{tenant_id}-agr-{suffix}"
        product_id = f"{tenant_id}-prod-{suffix}"
        msisdn = f"5959{rng.integers(10_000_000, 99_999_999)}"
        imsi = f"7440{rng.integers(10_000_000_000, 99_999_999_999)}"
        imei = f"3550{rng.integers(10_000_000_000, 99_999_999_999)}"

        is_fraud = bool(is_fraud_all[i])
        fraud_type = FRAUD_TYPES[fraud_type_idx[i]] if is_fraud else None

        # latent "ability to pay" drives recharge size, ARPU and default risk
        ability = float(np.clip(rng.normal(0.5, 0.2), 0.02, 0.99))
        tenure_days = int(rng.integers(30, 1500))
        acct_valid_from = start_date - timedelta(days=tenure_days)
        account_type = ACCOUNT_TYPES[rng.integers(0, len(ACCOUNT_TYPES))]
        is_company = account_type == "company"
        segment = "B2B" if is_company else SEGMENTS[rng.integers(0, len(SEGMENTS))]
        business_line = BUSINESS_LINES[rng.integers(0, len(BUSINESS_LINES))]
        consent_analytics = bool(rng.random() < 0.9)
        consent_marketing = bool(rng.random() < 0.6)
        has_device = bool(rng.random() < 0.3)
        num_products = int(rng.integers(1, 4))

        birth_year = int(rng.integers(1955, 2005))

        parties.append({
            "tenant_id": tenant_id, "id": party_id, "role": "owner",
            "first_name": "First", "last_name": f"User{suffix}",
            "date_of_birth": f"{birth_year}-01-01", "language": "en-US",
            "phone": f"+{msisdn}", "email": f"user{suffix}@example.com",
            "consent_marketing": consent_marketing, "consent_analytics": consent_analytics,
            "account_id": account_id,
        })
        accounts.append({
            "tenant_id": tenant_id, "id": account_id, "account_type": account_type,
            "account_segment": segment, "lifecycle_status": "active",
            "valid_from": _iso(acct_valid_from), "party_id": party_id,
            "is_company": is_company,
        })
        agreements.append({
            "tenant_id": tenant_id, "id": agreement_id,
            "agreement_type": "device" if has_device else "subscription",
            "business_line": business_line, "lifecycle_status": "active",
            "valid_from": _iso(acct_valid_from), "msisdn": msisdn, "imsi": imsi,
            "account_id": account_id,
        })
        sub_products: list[dict] = []
        for p in range(num_products):
            name = PRODUCT_NAMES[rng.integers(0, len(PRODUCT_NAMES))]
            category, monthly_fee = PRODUCT_CATALOG[name]
            prod = {
                "tenant_id": tenant_id, "id": f"{product_id}-{p}",
                "product_type": "addon", "name": name, "category": category,
                "monthly_fee": monthly_fee,
                "lifecycle_status": "active", "agreement_id": agreement_id,
            }
            products.append(prod)
            sub_products.append(prod)

        # ---- behaviour profiles ------------------------------------------------
        # base daily rates
        base_calls = max(1.0, rng.normal(6, 2))
        base_data = max(1.0, rng.normal(8, 3))
        # frequent recharges -> success-rate / avg-amount are well estimated
        recharge_prob = 0.40 + 0.30 * ability  # chance of recharge on a given day
        recharge_mean = 5 + 45 * ability
        # Roaming is a per-subscriber trait (drawn on the aux stream so it does not
        # perturb the main rng): most subscribers never roam, ~25% do. Without this
        # a flat per-event probability makes *every* subscriber a roamer over a long
        # window, collapsing the roamer/non-roamer split. Fraud overrides below.
        is_roamer = bool(aux_rng.random() < 0.25)
        roaming_prob = 0.08 if is_roamer else 0.0
        originating_ratio = 0.55
        # wide failure spread so recharge_success_rate is a strong credit signal
        fail_prob = 0.02 + 0.45 * (1 - ability)

        # Fraud behaviour is elevated but overlaps the normal population so the
        # detection problem has irreducible error (avoids a fake "perfect" model).
        if fraud_type == "sim_box":
            base_calls = rng.normal(55, 15)          # high call volume
            originating_ratio = 0.92                   # mostly outgoing
            recharge_prob = 0.03
        elif fraud_type == "card_fraud":
            fail_prob = 0.55                          # many failed card attempts
            recharge_mean = 70 + 90 * rng.random()   # large amounts
            recharge_prob = 0.45
        elif fraud_type == "roaming_fraud":
            roaming_prob = 0.7                        # frequently roaming
            base_data = rng.normal(30, 8)

        total_recharge = 0.0
        n_recharge = 0
        n_recharge_ok = 0
        for d in range(days):
            day = start_date + timedelta(days=d)
            n_calls = int(max(0, rng.poisson(max(0.1, base_calls))))
            for _ in range(n_calls):
                ts = day + timedelta(seconds=int(rng.integers(0, 86400)))
                is_orig = rng.random() < originating_ratio
                duration = int(max(1, rng.exponential(90)))
                amount = round(duration / 60 * 0.05, 5)
                voice_rows.append({
                    "tenant_id": tenant_id, "id": f"{agreement_id}-v-{d}-{_}",
                    "service_id": msisdn, "agreement_id": agreement_id,
                    "event_type": "voice", "started_at": _iso(ts), "created_at": _iso(ts),
                    "net_amount": amount, "tax_amount": round(amount * 0.1, 5),
                    "currency": "EUR", "qty": duration, "unit": "seconds",
                    "is_roaming": bool(rng.random() < roaming_prob),
                    "network_tech": None,
                    "traffic_case": "originating" if is_orig else "terminating",
                    "other_party_number": f"5959{rng.integers(10_000_000, 99_999_999)}",
                    "imei": imei,
                })
            n_data = int(max(0, rng.poisson(max(0.1, base_data))))
            for _ in range(n_data):
                ts = day + timedelta(seconds=int(rng.integers(0, 86400)))
                kb = int(max(1, rng.exponential(2000)))
                amount = round(kb / 1024 * 0.01, 5)
                data_rows.append({
                    "tenant_id": tenant_id, "id": f"{agreement_id}-d-{d}-{_}",
                    "service_id": msisdn, "agreement_id": agreement_id,
                    "event_type": "data", "started_at": _iso(ts), "created_at": _iso(ts),
                    "net_amount": amount, "tax_amount": round(amount * 0.1, 5),
                    "currency": "EUR", "qty": kb * 1024, "unit": "bytes",
                    "is_roaming": bool(rng.random() < roaming_prob),
                    "network_tech": NETWORK_TECH[rng.integers(0, len(NETWORK_TECH))],
                    "traffic_case": None, "other_party_number": None, "imei": imei,
                })
            if rng.random() < recharge_prob:
                ts = day + timedelta(seconds=int(rng.integers(0, 86400)))
                failed = rng.random() < fail_prob
                amount = round(max(1.0, rng.normal(recharge_mean, recharge_mean * 0.3)), 2)
                n_recharge += 1
                if not failed:
                    n_recharge_ok += 1
                    total_recharge += amount
                recharge_type = RECHARGE_TYPES[
                    aux_rng.choice(len(RECHARGE_TYPES), p=RECHARGE_TYPE_WEIGHTS)
                ]
                recharge_rows.append({
                    "tenant_id": tenant_id, "id": f"{agreement_id}-r-{d}",
                    "service_id": msisdn, "agreement_id": agreement_id,
                    "created_at": _iso(ts), "amount": amount, "currency": "EUR",
                    "payment_method": PAYMENT_METHODS[rng.integers(0, len(PAYMENT_METHODS))],
                    "status": "failed" if failed else "successful",
                    "failure_reason_code": "insufficient-funds" if failed else None,
                    "source": "web", "recharge_type": recharge_type,
                })

        # ---- product fee charges ----------------------------------------------
        # A fee charge is associated with a PRODUCT only (product_id FK); the
        # product carries name/category and its agreement carries the service, so
        # those are NOT denormalized onto the charge (they are re-derived in Silver
        # by joining products). Recurring monthly fees + occasional one-time/
        # overage/discount charges. Analytics-only (not fed to the ML models); see
        # ADR-0022.
        fc_seq = 0
        for prod in sub_products:
            for m in month_starts:
                fc_seq += 1
                fee_rows.append({
                    "tenant_id": tenant_id, "id": f"{agreement_id}-fc-{fc_seq}",
                    "product_id": prod["id"], "charge_type": "recurring",
                    "amount": round(prod["monthly_fee"], 2), "currency": "EUR",
                    "charged_at": _iso(m),
                })
                # occasional data/roaming overage on top of the plan
                if aux_rng.random() < 0.15:
                    fc_seq += 1
                    fee_rows.append({
                        "tenant_id": tenant_id, "id": f"{agreement_id}-fc-{fc_seq}",
                        "product_id": prod["id"], "charge_type": "overage",
                        "amount": round(float(2 + 18 * ability * aux_rng.random()), 2),
                        "currency": "EUR",
                        "charged_at": _iso(m + timedelta(days=int(aux_rng.integers(1, 27)))),
                    })
        # one-time device/activation charge (on the subscriber's first product)
        if has_device and month_starts:
            fc_seq += 1
            fee_rows.append({
                "tenant_id": tenant_id, "id": f"{agreement_id}-fc-{fc_seq}",
                "product_id": f"{product_id}-0", "charge_type": "one_time",
                "amount": round(float(29 + 170 * aux_rng.random()), 2), "currency": "EUR",
                "charged_at": _iso(start_date),
            })
        # small fraction get a loyalty discount (negative amount) on a random month
        if month_starts and aux_rng.random() < 0.20:
            fc_seq += 1
            m = month_starts[aux_rng.integers(0, len(month_starts))]
            fee_rows.append({
                "tenant_id": tenant_id, "id": f"{agreement_id}-fc-{fc_seq}",
                "product_id": f"{product_id}-0", "charge_type": "discount",
                "amount": round(float(-(2 + 8 * aux_rng.random())), 2), "currency": "EUR",
                "charged_at": _iso(m),
            })

        # ---- credit label -----------------------------------------------------
        # Default risk is a mid-range function of behaviour so realized labels
        # genuinely overlap (Bayes-optimal AUC well below 1.0).
        success_rate = (n_recharge_ok / n_recharge) if n_recharge else 0.0
        avg_recharge = (total_recharge / n_recharge_ok) if n_recharge_ok else 0.0
        tenure_factor = min(1.0, tenure_days / 365.0)        # used for limit sizing
        tenure_norm = min(1.0, tenure_days / 1500.0)         # full-range risk signal
        recharge_capacity = min(1.0, avg_recharge / 60.0)
        # Risk is driven by *observable* behaviour so a model on the feature set
        # is genuinely predictive; the Bernoulli draw keeps AUC below 1.0.
        raw_risk = (
            0.45 * (1 - success_rate) + 0.30 * (1 - tenure_norm)
            + 0.25 * (1 - recharge_capacity)
            + (0.20 if fraud_type == "card_fraud" else 0.0)
        )
        # steep, well-separated mapping keeps a strong but non-perfect signal
        default_propensity = float(np.clip(1.25 * raw_risk - 0.10, 0.02, 0.95))
        will_default = bool(rng.random() < default_propensity)
        # "true" approved limit the model should learn to approximate
        true_limit = float(np.clip(
            3.0 * avg_recharge * tenure_factor * max(0.1, success_rate) * (1 - default_propensity),
            5.0, 500.0,
        ))

        # small label noise simulates mislabeled / unknown cases (irreducible error)
        observed_fraud = is_fraud
        if rng.random() < 0.03:
            observed_fraud = not is_fraud

        label_rows.append({
            "tenant_id": tenant_id, "service_id": msisdn,
            "is_fraud": observed_fraud, "fraud_type": fraud_type or "none",
            "will_default": will_default, "true_credit_limit": round(true_limit, 2),
        })

    return SyntheticDataset(
        parties=pd.DataFrame(parties),
        accounts=pd.DataFrame(accounts),
        agreements=pd.DataFrame(agreements),
        products=pd.DataFrame(products),
        voice_events=pd.DataFrame(voice_rows),
        data_events=pd.DataFrame(data_rows),
        recharge_events=pd.DataFrame(recharge_rows),
        fee_charges=pd.DataFrame(fee_rows),
        labels=pd.DataFrame(label_rows),
    )


def generate_for_tenants(
    tenant_ids: list[str], **kwargs
) -> SyntheticDataset:
    """Generate and concatenate datasets for multiple tenants."""
    parts = [generate(t, **kwargs) for t in tenant_ids]
    return SyntheticDataset(
        parties=pd.concat([p.parties for p in parts], ignore_index=True),
        accounts=pd.concat([p.accounts for p in parts], ignore_index=True),
        agreements=pd.concat([p.agreements for p in parts], ignore_index=True),
        products=pd.concat([p.products for p in parts], ignore_index=True),
        voice_events=pd.concat([p.voice_events for p in parts], ignore_index=True),
        data_events=pd.concat([p.data_events for p in parts], ignore_index=True),
        recharge_events=pd.concat([p.recharge_events for p in parts], ignore_index=True),
        fee_charges=pd.concat([p.fee_charges for p in parts], ignore_index=True),
        labels=pd.concat([p.labels for p in parts], ignore_index=True),
    )
