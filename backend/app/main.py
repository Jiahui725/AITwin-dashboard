from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlmodel import Session, func, select

from app.database import create_db_and_tables, get_session
from app.models import (
    DiagnosticCategory,
    Interaction,
    InteractionDiagnostic,
    InteractionDiagnosticTag,
    Twin,
    User,
)

app = FastAPI(title="AI Twin Analytics API")

# Dashboard time semantics are fixed by workspace configuration.
WORKSPACE_TIMEZONE = "America/Los_Angeles"
WORKSPACE_TZ = ZoneInfo(WORKSPACE_TIMEZONE)
WEEK_START = "monday"

# Token proxy pricing (USD per 1K tokens)
PRICING_CONFIG = {
    "currency": "USD",
    "input_price_per_1k_tokens": 0.003,
    "output_price_per_1k_tokens": 0.015,
    "is_token_proxy": True,
}

PresetKey = Literal["this_month", "last_30_days", "this_quarter", "ytd"]
Granularity = Literal["week", "month"]


class TimeWindow(TypedDict):
    start: datetime
    end: datetime


class PeriodContext(TypedDict):
    preset: PresetKey
    current: TimeWindow
    previous: TimeWindow


DIAGNOSTIC_CATEGORY_ORDER = [
    DiagnosticCategory.HALLUCINATION,
    DiagnosticCategory.OUTDATED_INFO,
    DiagnosticCategory.TONE,
    DiagnosticCategory.INSTRUCTIONS_UNFOLLOWED,
]
DIAGNOSTIC_CATEGORY_VALUES = [category.value for category in DIAGNOSTIC_CATEGORY_ORDER]


class DiagnosticUpsertRequest(BaseModel):
    categories: List[DiagnosticCategory]


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    create_db_and_tables()


def _floor_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _quarter_start(dt: datetime) -> datetime:
    quarter_month = ((dt.month - 1) // 3) * 3 + 1
    return dt.replace(month=quarter_month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _week_start(dt: datetime) -> datetime:
    start = _floor_day(dt)
    return start - timedelta(days=start.weekday())


def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month_start(dt: datetime) -> datetime:
    month_start = _month_start(dt)
    if month_start.month == 12:
        return month_start.replace(year=month_start.year + 1, month=1)
    return month_start.replace(month=month_start.month + 1)


def _parse_preset(preset: str) -> PresetKey:
    allowed = {"this_month", "last_30_days", "this_quarter", "ytd"}
    if preset not in allowed:
        raise HTTPException(
            status_code=400,
            detail="Invalid preset. Allowed: this_month, last_30_days, this_quarter, ytd.",
        )
    return preset  # type: ignore[return-value]


def build_period_context(preset: PresetKey, now_utc: Optional[datetime] = None) -> PeriodContext:
    now_local = (now_utc or datetime.now(timezone.utc)).astimezone(WORKSPACE_TZ)
    current_end_local = now_local

    if preset == "this_month":
        current_start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif preset == "last_30_days":
        current_start_local = _floor_day(now_local) - timedelta(days=29)
    elif preset == "this_quarter":
        current_start_local = _quarter_start(now_local)
    else:
        current_start_local = now_local.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    current_start_utc = current_start_local.astimezone(timezone.utc)
    current_end_utc = current_end_local.astimezone(timezone.utc)

    duration = current_end_utc - current_start_utc
    previous_end_utc = current_start_utc
    previous_start_utc = previous_end_utc - duration

    return {
        "preset": preset,
        "current": {"start": current_start_utc, "end": current_end_utc},
        "previous": {"start": previous_start_utc, "end": previous_end_utc},
    }


def _serialize_period(preset: PresetKey, start: datetime, end: datetime) -> Dict[str, str]:
    return {
        "preset": preset,
        "start_at": start.isoformat(),
        "end_at": end.isoformat(),
        "timezone": WORKSPACE_TIMEZONE,
        "week_start": WEEK_START,
    }


def _serialize_comparison_period(start: datetime, end: datetime) -> Dict[str, str]:
    return {"start_at": start.isoformat(), "end_at": end.isoformat()}


def _duration_days(start: datetime, end: datetime) -> int:
    return max(1, int((end - start).total_seconds() // 86400) + 1)


def _auto_granularity(start: datetime, end: datetime) -> Granularity:
    return "week" if _duration_days(start, end) <= 90 else "month"


def _generate_buckets(start_utc: datetime, end_utc: datetime, granularity: Granularity) -> List[Dict[str, datetime]]:
    start_local = start_utc.astimezone(WORKSPACE_TZ)
    end_local = end_utc.astimezone(WORKSPACE_TZ)

    if granularity == "week":
        cursor = _week_start(start_local)
        step = timedelta(days=7)
        buckets = []
        while cursor < end_local:
            bucket_start = cursor
            bucket_end = cursor + step
            buckets.append({
                "start_local": bucket_start,
                "end_local": bucket_end,
                "start_utc": bucket_start.astimezone(timezone.utc),
                "end_utc": bucket_end.astimezone(timezone.utc),
            })
            cursor = bucket_end
        return buckets

    cursor = _month_start(start_local)
    buckets = []
    while cursor < end_local:
        bucket_start = cursor
        bucket_end = _next_month_start(cursor)
        buckets.append({
            "start_local": bucket_start,
            "end_local": bucket_end,
            "start_utc": bucket_start.astimezone(timezone.utc),
            "end_utc": bucket_end.astimezone(timezone.utc),
        })
        cursor = bucket_end
    return buckets


def _ensure_aware_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _bucket_index(ts_utc: datetime, buckets: List[Dict[str, datetime]]) -> Optional[int]:
    normalized_ts = _ensure_aware_utc(ts_utc)
    for idx, bucket in enumerate(buckets):
        if bucket["start_utc"] <= normalized_ts < bucket["end_utc"]:
            return idx
    return None


def _rate_pct(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return round((numerator / denominator) * 100, 1)


def _delta_pct(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        return 0.0 if current == 0 else None
    return round(((current - previous) / previous) * 100, 1)


def _delta_pp(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous is None:
        return None
    return round(current - previous, 1)


def _round_optional(value: Optional[float], digits: int = 1) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def _interaction_metrics(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, float]:
    filters = (Interaction.created_at >= start_at, Interaction.created_at < end_at)

    total_interactions = session.exec(select(func.count(Interaction.id)).where(*filters)).one()
    active_twins = session.exec(
        select(func.count(func.distinct(Interaction.twin_id))).where(*filters)
    ).one()
    avg_latency = session.exec(select(func.avg(Interaction.processing_time_ms)).where(*filters)).one()

    total_prompt = session.exec(select(func.sum(Interaction.prompt_length)).where(*filters)).one() or 0
    total_response = session.exec(select(func.sum(Interaction.response_length)).where(*filters)).one() or 0
    total_tokens = total_prompt + total_response

    return {
        "total_interactions": float(total_interactions),
        "active_twins": float(active_twins),
        "avg_latency_ms": float(round(avg_latency, 2)) if avg_latency else 0.0,
        "total_prompt_tokens": float(total_prompt),
        "total_response_tokens": float(total_response),
        "total_tokens": float(total_tokens),
    }


def _twin_metrics(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, float]:
    filters = (Twin.created_at >= start_at, Twin.created_at < end_at)

    total_twins = session.exec(select(func.count(Twin.id)).where(*filters)).one()
    public_twins = session.exec(
        select(func.count(Twin.id)).where(*filters).where(Twin.visibility == "team")
    ).one()
    private_twins = session.exec(
        select(func.count(Twin.id)).where(*filters).where(Twin.visibility == "private")
    ).one()

    public_twin_ratio_pct = round((public_twins / total_twins) * 100, 1) if total_twins else 0.0

    return {
        "total_twins": float(total_twins),
        "public_twins": float(public_twins),
        "private_twins": float(private_twins),
        "public_twin_ratio_pct": float(public_twin_ratio_pct),
    }


def _overview_business_metrics(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    interaction_rows = session.exec(
        select(
            Interaction.user_id,
            Twin.owner_id,
            Interaction.is_helpful,
            Interaction.prompt_length,
            Interaction.response_length,
            Interaction.created_at,
            User.created_at,
        )
        .join(Twin, Twin.id == Interaction.twin_id)
        .join(User, User.id == Interaction.user_id)
        .where(Interaction.created_at >= start_at, Interaction.created_at < end_at)
    ).all()

    total_interactions = len(interaction_rows)
    active_user_ids = set()
    colleague_interactions = 0
    helpful_count = 0
    thumb_down_count = 0
    total_prompt_tokens = 0
    total_response_tokens = 0

    for user_id, owner_id, is_helpful, prompt_length, response_length, interaction_created_at, user_created_at in interaction_rows:
        interaction_created_at_utc = _ensure_aware_utc(interaction_created_at)
        user_created_at_utc = _ensure_aware_utc(user_created_at)
        if user_created_at_utc <= interaction_created_at_utc:
            active_user_ids.add(user_id)
        if user_id != owner_id:
            colleague_interactions += 1
        if is_helpful is True:
            helpful_count += 1
        elif is_helpful is False:
            thumb_down_count += 1
        total_prompt_tokens += int(prompt_length)
        total_response_tokens += int(response_length)

    total_registered_users = session.exec(
        select(func.count(User.id)).where(User.created_at < end_at)
    ).one()

    new_user_rows = session.exec(
        select(User.id, User.created_at).where(User.created_at >= start_at, User.created_at < end_at)
    ).all()
    new_registered_users = len(new_user_rows)

    activated_new_users_7d = 0
    if new_user_rows:
        new_user_created_at = {
            str(user_id): _ensure_aware_utc(created_at) for user_id, created_at in new_user_rows
        }
        new_user_ids = list(new_user_created_at.keys())
        twin_rows = session.exec(
            select(Twin.owner_id, Twin.created_at)
            .where(Twin.owner_id.in_(new_user_ids))
            .where(Twin.created_at < end_at)
        ).all()

        earliest_twin_by_owner: Dict[str, datetime] = {}
        for owner_id, created_at in twin_rows:
            owner_key = str(owner_id)
            created_at_utc = _ensure_aware_utc(created_at)
            if owner_key not in earliest_twin_by_owner or created_at_utc < earliest_twin_by_owner[owner_key]:
                earliest_twin_by_owner[owner_key] = created_at_utc

        for user_id, user_created_at in new_user_created_at.items():
            first_twin_at = earliest_twin_by_owner.get(user_id)
            if first_twin_at is None:
                continue
            activation_deadline = user_created_at + timedelta(days=7)
            if user_created_at <= first_twin_at < activation_deadline:
                activated_new_users_7d += 1

    feedback_count = helpful_count + thumb_down_count
    total_tokens = total_prompt_tokens + total_response_tokens
    estimated_spend_usd = (
        (total_prompt_tokens / 1000.0) * PRICING_CONFIG["input_price_per_1k_tokens"]
        + (total_response_tokens / 1000.0) * PRICING_CONFIG["output_price_per_1k_tokens"]
    )

    return {
        "active_users": float(len(active_user_ids)),
        "total_registered_users": float(total_registered_users),
        "active_rate_pct": _rate_pct(float(len(active_user_ids)), float(total_registered_users)),
        "new_registered_users": float(new_registered_users),
        "activated_new_users_7d": float(activated_new_users_7d),
        "new_user_activation_rate_7d_pct": _rate_pct(float(activated_new_users_7d), float(new_registered_users)),
        "colleague_interactions": float(colleague_interactions),
        "colleague_usage_share_pct": _rate_pct(float(colleague_interactions), float(total_interactions)),
        "helpful_count": float(helpful_count),
        "thumb_down_count": float(thumb_down_count),
        "feedback_count": float(feedback_count),
        "helpful_rate_pct": _rate_pct(float(helpful_count), float(feedback_count)),
        "thumb_down_rate_pct_feedback": _rate_pct(float(thumb_down_count), float(feedback_count)),
        "feedback_coverage_pct": round((feedback_count / total_interactions) * 100, 1) if total_interactions else 0.0,
        "estimated_spend_usd": float(round(estimated_spend_usd, 4)),
        "total_tokens": float(total_tokens),
    }


def compute_kpis(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    metrics.update(_interaction_metrics(session, start_at, end_at))
    metrics.update(_twin_metrics(session, start_at, end_at))
    metrics.update(_overview_business_metrics(session, start_at, end_at))
    return metrics


def _build_kpi_delta(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Dict[str, Optional[float]]]:
    numeric_kpis = [
        "total_interactions",
        "active_twins",
        "avg_latency_ms",
        "total_tokens",
        "total_twins",
        "public_twins",
        "private_twins",
        "active_users",
        "total_registered_users",
        "new_registered_users",
        "activated_new_users_7d",
        "colleague_interactions",
        "helpful_count",
        "thumb_down_count",
        "feedback_count",
        "estimated_spend_usd",
    ]

    delta_payload: Dict[str, Dict[str, Optional[float]]] = {}
    for key in numeric_kpis:
        delta_payload[key] = {
            "current": current[key],
            "previous": previous[key],
            "delta_pct": _delta_pct(current[key], previous[key]),
        }

    ratio_kpis = [
        "public_twin_ratio_pct",
        "active_rate_pct",
        "new_user_activation_rate_7d_pct",
        "colleague_usage_share_pct",
        "helpful_rate_pct",
        "thumb_down_rate_pct_feedback",
        "feedback_coverage_pct",
    ]

    for key in ratio_kpis:
        delta_payload[key] = {
            "current": current[key],
            "previous": previous[key],
            "delta_pp": _delta_pp(current[key], previous[key]),
        }

    return delta_payload


def _token_cost(prompt_tokens: int, response_tokens: int) -> float:
    input_cost = (prompt_tokens / 1000.0) * PRICING_CONFIG["input_price_per_1k_tokens"]
    output_cost = (response_tokens / 1000.0) * PRICING_CONFIG["output_price_per_1k_tokens"]
    return round(input_cost + output_cost, 6)


def _growth_snapshot(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Optional[float]]:
    registered_users = session.exec(
        select(func.count(User.id)).where(User.created_at >= start_at, User.created_at < end_at)
    ).one()

    users_with_twin = session.exec(
        select(func.count(func.distinct(Twin.owner_id))).where(
            Twin.created_at >= start_at,
            Twin.created_at < end_at,
        )
    ).one()

    created_twins = session.exec(
        select(func.count(Twin.id)).where(Twin.created_at >= start_at, Twin.created_at < end_at)
    ).one()

    public_twins = session.exec(
        select(func.count(Twin.id)).where(
            Twin.created_at >= start_at,
            Twin.created_at < end_at,
            Twin.visibility == "team",
        )
    ).one()

    new_user_rows = session.exec(
        select(User.id, User.created_at).where(User.created_at >= start_at, User.created_at < end_at)
    ).all()
    new_registered_users = len(new_user_rows)
    activated_new_users_7d = 0

    if new_user_rows:
        new_user_created_at = {
            str(user_id): _ensure_aware_utc(created_at) for user_id, created_at in new_user_rows
        }
        new_user_ids = list(new_user_created_at.keys())
        twin_rows = session.exec(
            select(Twin.owner_id, Twin.created_at)
            .where(Twin.owner_id.in_(new_user_ids))
            .where(Twin.created_at < end_at)
        ).all()

        earliest_twin_by_owner: Dict[str, datetime] = {}
        for owner_id, created_at in twin_rows:
            owner_key = str(owner_id)
            created_at_utc = _ensure_aware_utc(created_at)
            if owner_key not in earliest_twin_by_owner or created_at_utc < earliest_twin_by_owner[owner_key]:
                earliest_twin_by_owner[owner_key] = created_at_utc

        for user_id, user_created_at in new_user_created_at.items():
            first_twin_at = earliest_twin_by_owner.get(user_id)
            if first_twin_at is None:
                continue
            activation_deadline = user_created_at + timedelta(days=7)
            if user_created_at <= first_twin_at < activation_deadline:
                activated_new_users_7d += 1

    twin_creation_rate = _rate_pct(float(users_with_twin), float(registered_users))
    new_user_activation_rate_7d = _rate_pct(float(activated_new_users_7d), float(new_registered_users))
    public_twin_rate = _rate_pct(float(public_twins), float(created_twins))

    return {
        "registered_users": float(registered_users),
        "users_with_twin": float(users_with_twin),
        "new_registered_users": float(new_registered_users),
        "activated_new_users_7d": float(activated_new_users_7d),
        "created_twins": float(created_twins),
        "public_twins": float(public_twins),
        "twin_creation_rate": twin_creation_rate,
        "new_user_activation_rate_7d": new_user_activation_rate_7d,
        "public_twin_rate": public_twin_rate,
    }


def _growth_summary(current: Dict[str, Optional[float]], previous: Dict[str, Optional[float]]) -> Dict[str, Dict[str, Optional[float]]]:
    summary: Dict[str, Dict[str, Optional[float]]] = {}

    for key in [
        "registered_users",
        "users_with_twin",
        "new_registered_users",
        "activated_new_users_7d",
        "created_twins",
        "public_twins",
    ]:
        current_val = current[key] or 0.0
        previous_val = previous[key] or 0.0
        summary[key] = {
            "current": current_val,
            "previous": previous_val,
            "delta_pct": _delta_pct(current_val, previous_val),
        }

    for key in ["twin_creation_rate", "new_user_activation_rate_7d", "public_twin_rate"]:
        current_val = current[key]
        previous_val = previous[key]
        summary[key] = {
            "current": current_val,
            "previous": previous_val,
            "delta_pp": _delta_pp(current_val, previous_val),
        }

    return summary


def _growth_cumulative_series(session: Session, start_at: datetime, end_at: datetime) -> List[Dict[str, Any]]:
    buckets = _generate_buckets(start_at, end_at, "week")

    registered_by_bucket = [0] * len(buckets)
    created_by_bucket = [0] * len(buckets)
    public_by_bucket = [0] * len(buckets)

    registered_before_period = session.exec(
        select(func.count(User.id)).where(User.created_at < start_at)
    ).one()
    created_twins_before_period = session.exec(
        select(func.count(Twin.id)).where(Twin.created_at < start_at)
    ).one()
    public_twins_before_period = session.exec(
        select(func.count(Twin.id)).where(Twin.created_at < start_at, Twin.visibility == "team")
    ).one()

    users = session.exec(
        select(User.created_at).where(User.created_at >= start_at, User.created_at < end_at)
    ).all()
    twins = session.exec(
        select(Twin.created_at, Twin.visibility).where(Twin.created_at >= start_at, Twin.created_at < end_at)
    ).all()

    for created_at in users:
        idx = _bucket_index(created_at, buckets)
        if idx is not None:
            registered_by_bucket[idx] += 1

    for created_at, visibility in twins:
        idx = _bucket_index(created_at, buckets)
        if idx is not None:
            created_by_bucket[idx] += 1
            if visibility == "team":
                public_by_bucket[idx] += 1

    reg_cum = 0
    twin_cum = 0
    pub_cum = 0
    series: List[Dict[str, Any]] = []

    for idx, bucket in enumerate(buckets):
        reg_cum += registered_by_bucket[idx]
        twin_cum += created_by_bucket[idx]
        pub_cum += public_by_bucket[idx]
        series.append(
            {
                "bucket_start": bucket["start_local"].isoformat(),
                "bucket_end": bucket["end_local"].isoformat(),
                "registered_users_net_new": registered_by_bucket[idx],
                "created_twins_net_new": created_by_bucket[idx],
                "public_twins_net_new": public_by_bucket[idx],
                "registered_users_total_as_of_bucket_end": int(registered_before_period + reg_cum),
                "created_twins_total_as_of_bucket_end": int(created_twins_before_period + twin_cum),
                "public_twins_total_as_of_bucket_end": int(public_twins_before_period + pub_cum),
                "registered_users_cum": reg_cum,
                "created_twins_cum": twin_cum,
                "public_twins_cum": pub_cum,
            }
        )

    return series


def _usage_source_mix(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    granularity = _auto_granularity(start_at, end_at)
    buckets = _generate_buckets(start_at, end_at, granularity)

    points = [
        {
            "bucket_start": b["start_local"].isoformat(),
            "bucket_end": b["end_local"].isoformat(),
            "slack_dm": 0,
            "slack_channel": 0,
            "web_app": 0,
            "total": 0,
        }
        for b in buckets
    ]

    rows = session.exec(
        select(Interaction.created_at, Interaction.source_channel).where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
        )
    ).all()

    for created_at, source_channel in rows:
        idx = _bucket_index(created_at, buckets)
        if idx is None:
            continue
        if source_channel not in {"slack_dm", "slack_channel", "web_app"}:
            continue
        points[idx][source_channel] += 1
        points[idx]["total"] += 1

    return {"granularity": granularity, "series": points}


def _usage_self_colleague_share(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    granularity = _auto_granularity(start_at, end_at)
    buckets = _generate_buckets(start_at, end_at, granularity)

    points = [
        {
            "bucket_start": b["start_local"].isoformat(),
            "bucket_end": b["end_local"].isoformat(),
            "self_count": 0,
            "colleague_count": 0,
            "self_share_pct": 0.0,
            "colleague_share_pct": 0.0,
        }
        for b in buckets
    ]

    rows = session.exec(
        select(Interaction.created_at, Interaction.user_id, Twin.owner_id)
        .join(Twin, Twin.id == Interaction.twin_id)
        .where(Interaction.created_at >= start_at, Interaction.created_at < end_at)
    ).all()

    for created_at, user_id, owner_id in rows:
        idx = _bucket_index(created_at, buckets)
        if idx is None:
            continue
        if user_id == owner_id:
            points[idx]["self_count"] += 1
        else:
            points[idx]["colleague_count"] += 1

    for point in points:
        total = point["self_count"] + point["colleague_count"]
        if total == 0:
            continue
        point["self_share_pct"] = round((point["self_count"] / total) * 100, 1)
        point["colleague_share_pct"] = round((point["colleague_count"] / total) * 100, 1)

    return {"granularity": granularity, "series": points}


def _cost_pareto(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    rows = session.exec(
        select(Interaction.user_id, Interaction.prompt_length, Interaction.response_length).where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
        )
    ).all()

    per_user: Dict[str, float] = {}
    for user_id, prompt_length, response_length in rows:
        per_user[user_id] = per_user.get(user_id, 0.0) + _token_cost(prompt_length, response_length)

    ranked = sorted(per_user.items(), key=lambda item: item[1], reverse=True)
    total_cost = sum(cost for _, cost in ranked)

    cumulative = 0.0
    series = []
    for idx, (user_id, cost) in enumerate(ranked, start=1):
        share_pct = round((cost / total_cost) * 100, 2) if total_cost else 0.0
        cumulative += share_pct
        series.append(
            {
                "rank": idx,
                "user_id": user_id,
                "cost": round(cost, 4),
                "cost_share_pct": share_pct,
                "cumulative_cost_share_pct": round(min(cumulative, 100.0), 2),
            }
        )

    return {
        "series": series,
        "total_cost": round(total_cost, 4),
        "currency": PRICING_CONFIG["currency"],
        "pricing": PRICING_CONFIG,
    }


def _cost_efficiency(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    rows = session.exec(
        select(
            Interaction.user_id,
            Twin.owner_id,
            Interaction.prompt_length,
            Interaction.response_length,
            Interaction.is_helpful,
        )
        .join(Twin, Twin.id == Interaction.twin_id)
        .where(Interaction.created_at >= start_at, Interaction.created_at < end_at)
    ).all()

    colleague_cost = 0.0
    colleague_helpful_solutions = 0

    for user_id, owner_id, prompt_length, response_length, is_helpful in rows:
        if user_id == owner_id:
            continue
        colleague_cost += _token_cost(prompt_length, response_length)
        if is_helpful is True:
            colleague_helpful_solutions += 1

    avg_cost = None
    if colleague_helpful_solutions > 0:
        avg_cost = round(colleague_cost / colleague_helpful_solutions, 4)

    return {
        "avg_cost_per_colleague_solution": avg_cost,
        "colleague_total_cost": round(colleague_cost, 4),
        "colleague_helpful_solutions": colleague_helpful_solutions,
        "currency": PRICING_CONFIG["currency"],
        "pricing": PRICING_CONFIG,
    }


def _normalize_diagnostic_categories(
    categories: List[DiagnosticCategory],
) -> List[DiagnosticCategory]:
    unique_categories: List[DiagnosticCategory] = []
    seen: set[DiagnosticCategory] = set()
    for category in categories:
        if category in seen:
            continue
        seen.add(category)
        unique_categories.append(category)

    order = {category: index for index, category in enumerate(DIAGNOSTIC_CATEGORY_ORDER)}
    unique_categories.sort(key=lambda category: order[category])
    return unique_categories


def _serialize_diagnostic_payload(
    interaction_id: int,
    diagnostic: InteractionDiagnostic,
    tags: List[InteractionDiagnosticTag],
) -> Dict[str, Any]:
    return {
        "interaction_id": interaction_id,
        "diagnostic_id": diagnostic.id,
        "categories": [tag.category.value for tag in tags],
        "created_at": diagnostic.created_at.isoformat(),
        "updated_at": diagnostic.updated_at.isoformat(),
    }


def _get_diagnostic_with_tags(
    session: Session,
    interaction_id: int,
) -> Tuple[Optional[InteractionDiagnostic], List[InteractionDiagnosticTag]]:
    diagnostic = session.exec(
        select(InteractionDiagnostic).where(
            InteractionDiagnostic.interaction_id == interaction_id
        )
    ).first()
    if diagnostic is None:
        return None, []

    tags = session.exec(
        select(InteractionDiagnosticTag)
        .where(InteractionDiagnosticTag.diagnostic_id == diagnostic.id)
        .order_by(InteractionDiagnosticTag.created_at)
    ).all()
    return diagnostic, tags


def _quality_diagnostic_summary(
    session: Session,
    start_at: datetime,
    end_at: datetime,
) -> Dict[str, Any]:
    thumb_down_cases = session.exec(
        select(func.count(Interaction.id)).where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
            Interaction.is_helpful.is_(False),
        )
    ).one()

    diagnosed_cases = session.exec(
        select(func.count(func.distinct(InteractionDiagnostic.id)))
        .join(Interaction, Interaction.id == InteractionDiagnostic.interaction_id)
        .where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
            Interaction.is_helpful.is_(False),
        )
    ).one()

    category_counts: Dict[DiagnosticCategory, int] = {
        category: 0 for category in DIAGNOSTIC_CATEGORY_ORDER
    }
    category_rows = session.exec(
        select(
            InteractionDiagnosticTag.category,
            func.count(func.distinct(InteractionDiagnosticTag.diagnostic_id)),
        )
        .join(
            InteractionDiagnostic,
            InteractionDiagnostic.id == InteractionDiagnosticTag.diagnostic_id,
        )
        .join(Interaction, Interaction.id == InteractionDiagnostic.interaction_id)
        .where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
            Interaction.is_helpful.is_(False),
        )
        .group_by(InteractionDiagnosticTag.category)
    ).all()

    for category, count in category_rows:
        category_enum = (
            category
            if isinstance(category, DiagnosticCategory)
            else DiagnosticCategory(category)
        )
        category_counts[category_enum] = int(count)

    summary = []
    for category in DIAGNOSTIC_CATEGORY_ORDER:
        case_count = category_counts[category]
        case_share = round((case_count / diagnosed_cases) * 100, 1) if diagnosed_cases else 0.0
        summary.append(
            {
                "category": category.value,
                "case_count": case_count,
                "case_share_pct": case_share,
            }
        )

    granularity = _auto_granularity(start_at, end_at)
    buckets = _generate_buckets(start_at, end_at, granularity)
    series = []
    bucket_case_ids: List[set[int]] = []
    for bucket in buckets:
        point = {
            "bucket_start": bucket["start_local"].isoformat(),
            "bucket_end": bucket["end_local"].isoformat(),
            "total_cases": 0,
        }
        for category in DIAGNOSTIC_CATEGORY_ORDER:
            point[category.value] = 0
        series.append(point)
        bucket_case_ids.append(set())

    trend_rows = session.exec(
        select(
            Interaction.created_at,
            InteractionDiagnosticTag.diagnostic_id,
            InteractionDiagnosticTag.category,
        )
        .join(
            InteractionDiagnostic,
            InteractionDiagnostic.id == InteractionDiagnosticTag.diagnostic_id,
        )
        .join(Interaction, Interaction.id == InteractionDiagnostic.interaction_id)
        .where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
            Interaction.is_helpful.is_(False),
        )
    ).all()

    for created_at, diagnostic_id, category in trend_rows:
        bucket_idx = _bucket_index(created_at, buckets)
        if bucket_idx is None:
            continue
        category_enum = (
            category
            if isinstance(category, DiagnosticCategory)
            else DiagnosticCategory(category)
        )
        series[bucket_idx][category_enum.value] += 1
        bucket_case_ids[bucket_idx].add(int(diagnostic_id))

    for idx, diagnostic_ids in enumerate(bucket_case_ids):
        series[idx]["total_cases"] = len(diagnostic_ids)

    diagnostic_coverage_pct = (
        round((diagnosed_cases / thumb_down_cases) * 100, 1)
        if thumb_down_cases
        else 0.0
    )

    return {
        # total_cases is preserved for backward compatibility with existing consumers.
        "total_cases": int(diagnosed_cases),
        "thumb_down_cases": int(thumb_down_cases),
        "diagnosed_cases": int(diagnosed_cases),
        "diagnostic_coverage_pct": float(diagnostic_coverage_pct),
        "summary": summary,
        "trend": {
            "granularity": granularity,
            "series": series,
        },
    }


def _quality_rate_snapshot(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, float]:
    total_interactions = session.exec(
        select(func.count(Interaction.id)).where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
        )
    ).one()
    thumb_down_count = session.exec(
        select(func.count(Interaction.id)).where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
            Interaction.is_helpful.is_(False),
        )
    ).one()
    thumb_down_rate_pct = round((thumb_down_count / total_interactions) * 100, 1) if total_interactions else 0.0

    return {
        "thumb_down_count": float(thumb_down_count),
        "total_interactions": float(total_interactions),
        "thumb_down_rate_pct": float(thumb_down_rate_pct),
    }


def _quality_health_trend(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    granularity = _auto_granularity(start_at, end_at)
    buckets = _generate_buckets(start_at, end_at, granularity)

    series = [
        {
            "bucket_start": bucket["start_local"].isoformat(),
            "bucket_end": bucket["end_local"].isoformat(),
            "down_count": 0,
            "total_interactions": 0,
            "down_rate_pct": 0.0,
        }
        for bucket in buckets
    ]

    rows = session.exec(
        select(Interaction.created_at, Interaction.is_helpful).where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
        )
    ).all()

    for created_at, is_helpful in rows:
        idx = _bucket_index(created_at, buckets)
        if idx is None:
            continue
        series[idx]["total_interactions"] += 1
        if is_helpful is False:
            series[idx]["down_count"] += 1

    for point in series:
        total = point["total_interactions"]
        point["down_rate_pct"] = round((point["down_count"] / total) * 100, 1) if total else 0.0

    return {"granularity": granularity, "series": series}


def _quality_defect_breakdown(session: Session, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    granularity = _auto_granularity(start_at, end_at)
    buckets = _generate_buckets(start_at, end_at, granularity)

    series = []
    for bucket in buckets:
        point: Dict[str, Any] = {
            "bucket_start": bucket["start_local"].isoformat(),
            "bucket_end": bucket["end_local"].isoformat(),
            "total_tag_occurrences": 0,
        }
        for category in DIAGNOSTIC_CATEGORY_VALUES:
            point[category] = 0
        series.append(point)

    rows = session.exec(
        select(Interaction.created_at, InteractionDiagnosticTag.category)
        .join(InteractionDiagnostic, InteractionDiagnostic.id == InteractionDiagnosticTag.diagnostic_id)
        .join(Interaction, Interaction.id == InteractionDiagnostic.interaction_id)
        .where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
            Interaction.is_helpful.is_(False),
        )
    ).all()

    for created_at, category in rows:
        idx = _bucket_index(created_at, buckets)
        if idx is None:
            continue
        category_value = category.value if isinstance(category, DiagnosticCategory) else str(category)
        if category_value not in DIAGNOSTIC_CATEGORY_VALUES:
            continue
        series[idx][category_value] += 1
        series[idx]["total_tag_occurrences"] += 1

    return {"granularity": granularity, "series": series}


def _quality_department_risk(
    session: Session,
    start_at: datetime,
    end_at: datetime,
    selected_department: Optional[str],
) -> Dict[str, Any]:
    rows = session.exec(
        select(User.department, Interaction.is_helpful, func.count(Interaction.id))
        .join(Twin, Twin.owner_id == User.id)
        .join(Interaction, Interaction.twin_id == Twin.id)
        .where(
            Interaction.created_at >= start_at,
            Interaction.created_at < end_at,
        )
        .group_by(User.department, Interaction.is_helpful)
    ).all()

    department_metrics: Dict[str, Dict[str, float]] = {}
    for department, is_helpful, count in rows:
        dept = department or "Unknown"
        if dept not in department_metrics:
            department_metrics[dept] = {
                "department": dept,
                "thumb_down_count": 0.0,
                "total_interactions": 0.0,
                "thumb_down_rate_pct": 0.0,
            }
        department_metrics[dept]["total_interactions"] += float(count)
        if is_helpful is False:
            department_metrics[dept]["thumb_down_count"] += float(count)

    ranking = []
    for department, metrics in department_metrics.items():
        total = metrics["total_interactions"]
        down = metrics["thumb_down_count"]
        down_rate_pct = round((down / total) * 100, 1) if total else 0.0
        ranking.append(
            {
                "department": department,
                "thumb_down_count": int(down),
                "total_interactions": int(total),
                "thumb_down_rate_pct": down_rate_pct,
            }
        )

    ranking.sort(
        key=lambda item: (
            -item["thumb_down_rate_pct"],
            -item["total_interactions"],
            item["department"],
        )
    )

    selected = selected_department
    valid_departments = {item["department"] for item in ranking}
    if selected not in valid_departments:
        selected = ranking[0]["department"] if ranking else None

    selected_breakdown = []
    selected_metrics = None
    if selected is not None:
        selected_metrics = next((item for item in ranking if item["department"] == selected), None)
        breakdown_rows = session.exec(
            select(InteractionDiagnosticTag.category, func.count(InteractionDiagnosticTag.id))
            .join(InteractionDiagnostic, InteractionDiagnostic.id == InteractionDiagnosticTag.diagnostic_id)
            .join(Interaction, Interaction.id == InteractionDiagnostic.interaction_id)
            .join(Twin, Twin.id == Interaction.twin_id)
            .join(User, User.id == Twin.owner_id)
            .where(
                Interaction.created_at >= start_at,
                Interaction.created_at < end_at,
                Interaction.is_helpful.is_(False),
                User.department == selected,
            )
            .group_by(InteractionDiagnosticTag.category)
        ).all()

        counts_by_category = {category: 0 for category in DIAGNOSTIC_CATEGORY_VALUES}
        for category, count in breakdown_rows:
            category_value = category.value if isinstance(category, DiagnosticCategory) else str(category)
            if category_value in counts_by_category:
                counts_by_category[category_value] = int(count)

        selected_breakdown = [
            {"category": category, "count": counts_by_category[category]}
            for category in DIAGNOSTIC_CATEGORY_VALUES
        ]
        selected_breakdown.sort(key=lambda item: (-item["count"], item["category"]))

    return {
        "ranking": ranking,
        "selected_department": selected,
        "selected_department_metrics": selected_metrics,
        "selected_breakdown": selected_breakdown,
    }


@app.get("/")
def read_root() -> Dict[str, str]:
    return {"message": "AI Twin Analytics API is running. Visit /docs for Swagger UI."}


@app.get("/api/metrics/kpi")
def get_kpi_metrics(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)

    current_metrics = compute_kpis(session, period["current"]["start"], period["current"]["end"])
    previous_metrics = compute_kpis(session, period["previous"]["start"], period["previous"]["end"])

    return {
        "total_interactions": int(current_metrics["total_interactions"]),
        "active_twins": int(current_metrics["active_twins"]),
        "avg_latency_ms": round(current_metrics["avg_latency_ms"], 2),
        "total_tokens": int(current_metrics["total_tokens"]),
        "total_twins": int(current_metrics["total_twins"]),
        "public_twins": int(current_metrics["public_twins"]),
        "private_twins": int(current_metrics["private_twins"]),
        "public_twin_ratio_pct": round(current_metrics["public_twin_ratio_pct"], 1),
        "active_users": int(current_metrics["active_users"]),
        "total_registered_users": int(current_metrics["total_registered_users"]),
        "active_rate_pct": _round_optional(current_metrics["active_rate_pct"], 1),
        "new_registered_users": int(current_metrics["new_registered_users"]),
        "activated_new_users_7d": int(current_metrics["activated_new_users_7d"]),
        "new_user_activation_rate_7d_pct": _round_optional(
            current_metrics["new_user_activation_rate_7d_pct"], 1
        ),
        "colleague_interactions": int(current_metrics["colleague_interactions"]),
        "colleague_usage_share_pct": _round_optional(current_metrics["colleague_usage_share_pct"], 1),
        "helpful_count": int(current_metrics["helpful_count"]),
        "thumb_down_count": int(current_metrics["thumb_down_count"]),
        "feedback_count": int(current_metrics["feedback_count"]),
        "helpful_rate_pct": _round_optional(current_metrics["helpful_rate_pct"], 1),
        "thumb_down_rate_pct_feedback": _round_optional(
            current_metrics["thumb_down_rate_pct_feedback"], 1
        ),
        "feedback_coverage_pct": _round_optional(current_metrics["feedback_coverage_pct"], 1),
        "estimated_spend_usd": round(current_metrics["estimated_spend_usd"], 4),
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        "comparison_period": _serialize_comparison_period(
            period["previous"]["start"], period["previous"]["end"]
        ),
        "delta": _build_kpi_delta(current_metrics, previous_metrics),
    }


@app.get("/api/metrics/trends")
def get_interaction_trends(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> List[Dict[str, Any]]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)

    statement = (
        select(
            func.date(Interaction.created_at).label("date"),
            Interaction.source_channel,
            func.count(Interaction.id).label("count"),
        )
        .where(Interaction.created_at >= period["current"]["start"])
        .where(Interaction.created_at < period["current"]["end"])
        .group_by(func.date(Interaction.created_at), Interaction.source_channel)
        .order_by(func.date(Interaction.created_at))
    )
    results = session.exec(statement).all()

    trend_dict: Dict[str, Dict[str, Any]] = {}
    for row in results:
        if row.date not in trend_dict:
            trend_dict[row.date] = {
                "date": row.date,
                "slack_dm": 0,
                "slack_channel": 0,
                "web_app": 0,
            }
        trend_dict[row.date][row.source_channel] = row.count

    return list(trend_dict.values())


@app.get("/api/metrics/sources")
def get_source_distribution(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> List[Dict[str, Any]]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)

    statement = (
        select(Interaction.source_channel, func.count(Interaction.id).label("count"))
        .where(Interaction.created_at >= period["current"]["start"])
        .where(Interaction.created_at < period["current"]["end"])
        .group_by(Interaction.source_channel)
        .order_by(func.count(Interaction.id).desc())
    )
    results = session.exec(statement).all()
    return [{"source": row.source_channel, "count": row.count} for row in results]


@app.get("/api/modules/growth/overview")
def get_growth_overview(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)

    current = _growth_snapshot(session, period["current"]["start"], period["current"]["end"])
    previous = _growth_snapshot(session, period["previous"]["start"], period["previous"]["end"])

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        "comparison_period": _serialize_comparison_period(
            period["previous"]["start"], period["previous"]["end"]
        ),
        "summary": _growth_summary(current, previous),
        "series": _growth_cumulative_series(session, period["current"]["start"], period["current"]["end"]),
    }


@app.get("/api/modules/usage/source-mix")
def get_usage_source_mix(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)
    result = _usage_source_mix(session, period["current"]["start"], period["current"]["end"])

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        "granularity": result["granularity"],
        "series": result["series"],
    }


@app.get("/api/modules/usage/self-colleague-share")
def get_usage_self_colleague_share(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)
    result = _usage_self_colleague_share(session, period["current"]["start"], period["current"]["end"])

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        "granularity": result["granularity"],
        "series": result["series"],
    }


@app.get("/api/modules/cost/pareto")
def get_cost_pareto(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)
    result = _cost_pareto(session, period["current"]["start"], period["current"]["end"])

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        **result,
    }


@app.get("/api/modules/cost/efficiency")
def get_cost_efficiency(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)
    result = _cost_efficiency(session, period["current"]["start"], period["current"]["end"])

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        **result,
    }


@app.get("/api/modules/quality/overview")
def get_quality_overview(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)

    current_snapshot = _quality_rate_snapshot(
        session,
        period["current"]["start"],
        period["current"]["end"],
    )
    previous_snapshot = _quality_rate_snapshot(
        session,
        period["previous"]["start"],
        period["previous"]["end"],
    )
    health_trend = _quality_health_trend(
        session,
        period["current"]["start"],
        period["current"]["end"],
    )
    defect_breakdown = _quality_defect_breakdown(
        session,
        period["current"]["start"],
        period["current"]["end"],
    )

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        "comparison_period": _serialize_comparison_period(
            period["previous"]["start"], period["previous"]["end"]
        ),
        "summary": {
            "current_thumb_down_rate_pct": current_snapshot["thumb_down_rate_pct"],
            "previous_thumb_down_rate_pct": previous_snapshot["thumb_down_rate_pct"],
            "thumb_down_rate_delta_pp": _delta_pp(
                current_snapshot["thumb_down_rate_pct"],
                previous_snapshot["thumb_down_rate_pct"],
            ),
            "current_thumb_down_count": int(current_snapshot["thumb_down_count"]),
            "current_total_interactions": int(current_snapshot["total_interactions"]),
        },
        "health_trend": health_trend["series"],
        "health_trend_granularity": health_trend["granularity"],
        "defect_breakdown": defect_breakdown["series"],
        "defect_breakdown_granularity": defect_breakdown["granularity"],
    }


@app.get("/api/modules/quality/department-risk")
def get_quality_department_risk(
    preset: str = Query(default="this_month"),
    department: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)
    result = _quality_department_risk(
        session,
        period["current"]["start"],
        period["current"]["end"],
        department,
    )

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        **result,
    }


@app.get("/api/interactions/thumb-down/recent")
def get_recent_thumb_down_interactions(
    preset: str = Query(default="this_month"),
    limit: int = Query(default=20, ge=1, le=100),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)

    rows = session.exec(
        select(
            Interaction.id,
            Interaction.created_at,
            Interaction.user_id,
            Interaction.twin_id,
            Interaction.source_channel,
            Twin.owner_id,
            Twin.name,
            InteractionDiagnostic.id,
        )
        .join(Twin, Twin.id == Interaction.twin_id)
        .join(
            InteractionDiagnostic,
            InteractionDiagnostic.interaction_id == Interaction.id,
            isouter=True,
        )
        .where(
            Interaction.created_at >= period["current"]["start"],
            Interaction.created_at < period["current"]["end"],
            Interaction.is_helpful.is_(False),
        )
        .order_by(Interaction.created_at.desc())
        .limit(limit)
    ).all()

    diagnostic_ids = [row[7] for row in rows if row[7] is not None]
    tags_by_diagnostic_id: Dict[int, List[str]] = {}
    if diagnostic_ids:
        tag_rows = session.exec(
            select(InteractionDiagnosticTag.diagnostic_id, InteractionDiagnosticTag.category)
            .where(InteractionDiagnosticTag.diagnostic_id.in_(diagnostic_ids))
            .order_by(InteractionDiagnosticTag.created_at)
        ).all()
        for diagnostic_id, category in tag_rows:
            category_value = (
                category.value
                if isinstance(category, DiagnosticCategory)
                else str(category)
            )
            tags_by_diagnostic_id.setdefault(diagnostic_id, []).append(category_value)

    items = []
    for (
        interaction_id,
        created_at,
        user_id,
        twin_id,
        source_channel,
        owner_id,
        twin_name,
        diagnostic_id,
    ) in rows:
        items.append(
            {
                "interaction_id": interaction_id,
                "created_at": _ensure_aware_utc(created_at).isoformat(),
                "user_id": user_id,
                "twin_id": twin_id,
                "twin_name": twin_name,
                "owner_id": owner_id,
                "source_channel": source_channel,
                "has_diagnostic": diagnostic_id is not None,
                "diagnostic_categories": tags_by_diagnostic_id.get(diagnostic_id, [])
                if diagnostic_id is not None
                else [],
            }
        )

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        "items": items,
    }


@app.post("/api/interactions/{interaction_id}/diagnostic")
def upsert_interaction_diagnostic(
    interaction_id: int,
    payload: DiagnosticUpsertRequest,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """Manual backfill/correction endpoint for diagnostic tags."""
    if not payload.categories:
        raise HTTPException(status_code=422, detail="At least one diagnostic category is required.")

    interaction = session.get(Interaction, interaction_id)
    if interaction is None:
        raise HTTPException(status_code=404, detail="Interaction not found.")
    if interaction.is_helpful is not False:
        raise HTTPException(status_code=400, detail="Diagnostics are only supported for thumb-down interactions.")

    categories = _normalize_diagnostic_categories(payload.categories)
    now = datetime.now(timezone.utc)

    diagnostic, existing_tags = _get_diagnostic_with_tags(session, interaction_id)
    if diagnostic is None:
        diagnostic = InteractionDiagnostic(
            interaction_id=interaction_id,
            created_at=now,
            updated_at=now,
        )
        session.add(diagnostic)
        session.flush()
    else:
        diagnostic.updated_at = now
        for tag in existing_tags:
            session.delete(tag)
        session.flush()

    for category in categories:
        session.add(
            InteractionDiagnosticTag(
                diagnostic_id=diagnostic.id,
                category=category,
                created_at=now,
            )
        )

    session.commit()

    refreshed_diagnostic, refreshed_tags = _get_diagnostic_with_tags(session, interaction_id)
    if refreshed_diagnostic is None:
        raise HTTPException(status_code=500, detail="Diagnostic upsert failed unexpectedly.")

    response = _serialize_diagnostic_payload(interaction_id, refreshed_diagnostic, refreshed_tags)
    response["updated_via"] = "manual_backfill"
    return response


@app.get("/api/interactions/{interaction_id}/diagnostic")
def get_interaction_diagnostic(
    interaction_id: int,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    interaction = session.get(Interaction, interaction_id)
    if interaction is None:
        raise HTTPException(status_code=404, detail="Interaction not found.")

    diagnostic, tags = _get_diagnostic_with_tags(session, interaction_id)
    if diagnostic is None:
        raise HTTPException(status_code=404, detail="Diagnostic not found for this interaction.")

    return _serialize_diagnostic_payload(interaction_id, diagnostic, tags)


@app.get("/api/modules/quality/diagnostic-summary")
def get_quality_diagnostic_summary(
    preset: str = Query(default="this_month"),
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    preset_key = _parse_preset(preset)
    period = build_period_context(preset_key)
    summary = _quality_diagnostic_summary(
        session,
        period["current"]["start"],
        period["current"]["end"],
    )

    return {
        "period": _serialize_period(
            period["preset"], period["current"]["start"], period["current"]["end"]
        ),
        **summary,
    }
