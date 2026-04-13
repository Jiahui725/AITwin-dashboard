import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker
from sqlmodel import Session, func, select

from app.database import DEPARTMENT_NAMES, create_db_and_tables, engine, sample_department
from app.models import (
    DiagnosticCategory,
    Interaction,
    InteractionDiagnostic,
    InteractionDiagnosticTag,
    Twin,
    User,
)

NUM_USERS = 100
NUM_TWINS = 40
NUM_INTERACTIONS = 6000
TIME_WINDOW_DAYS = 60

TEAM_BASE_COLLEAGUE_RATIO = 0.30
TEAM_TARGET_COLLEAGUE_RATIO = 0.50
DIAGNOSTIC_CATEGORIES = [
    DiagnosticCategory.HALLUCINATION,
    DiagnosticCategory.OUTDATED_INFO,
    DiagnosticCategory.TONE,
    DiagnosticCategory.INSTRUCTIONS_UNFOLLOWED,
]

fake = Faker()


def ensure_aware_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def reset_database() -> None:
    """Delete SQLite file and recreate tables for deterministic demo data."""
    engine.dispose()
    db_file = engine.url.database or "ai_twin.db"
    db_path = Path(db_file)
    if not db_path.is_absolute():
        db_path = Path(__file__).resolve().parent / db_path
    if db_path.exists():
        db_path.unlink()
    create_db_and_tables()


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def build_team_owner_ids(owner_users: list[User]) -> set[str]:
    team_owner_ids = {owner.id for owner in owner_users if random.random() < 0.6}
    if not team_owner_ids:
        team_owner_ids.add(random.choice(owner_users).id)
    return team_owner_ids


def build_random_diagnostic_categories() -> list[DiagnosticCategory]:
    tag_count = random.choice((1, 2))
    return random.sample(DIAGNOSTIC_CATEGORIES, k=tag_count)


def validate_user_departments(session: Session) -> None:
    department_rows = session.exec(select(User.department)).all()
    allowed_departments = set(DEPARTMENT_NAMES)
    invalid_departments = [
        department
        for department in department_rows
        if department not in allowed_departments
    ]

    if invalid_departments:
        raise RuntimeError(
            f"Seed validation failed: found invalid departments: {sorted(set(invalid_departments))}."
        )

    unknown_count = session.exec(
        select(func.count(User.id)).where(
            (User.department == None)  # noqa: E711
            | (User.department == "")
            | (User.department == "Unknown")
        )
    ).one()
    if unknown_count != 0:
        raise RuntimeError(
            "Seed validation failed: some users still have null/empty/Unknown department."
        )


def validate_seed_data(session: Session) -> None:
    team_twins = session.exec(select(func.count(Twin.id)).where(Twin.visibility == "team")).one()

    private_colleague_calls = session.exec(
        select(func.count(Interaction.id))
        .join(Twin, Twin.id == Interaction.twin_id)
        .where(Twin.visibility == "private")
        .where(Interaction.user_id != Twin.owner_id)
    ).one()

    team_colleague_calls = session.exec(
        select(func.count(Interaction.id))
        .join(Twin, Twin.id == Interaction.twin_id)
        .where(Twin.visibility == "team")
        .where(Interaction.user_id != Twin.owner_id)
    ).one()
    interaction_before_user_registration = session.exec(
        select(func.count(Interaction.id))
        .join(User, User.id == Interaction.user_id)
        .where(Interaction.created_at < User.created_at)
    ).one()
    interaction_before_twin_creation = session.exec(
        select(func.count(Interaction.id))
        .join(Twin, Twin.id == Interaction.twin_id)
        .where(Interaction.created_at < Twin.created_at)
    ).one()

    thumb_down_interactions = session.exec(
        select(func.count(Interaction.id)).where(Interaction.is_helpful.is_(False))
    ).one()
    diagnostics_count = session.exec(
        select(func.count(InteractionDiagnostic.id))
    ).one()
    diagnostics_on_thumb_up = session.exec(
        select(func.count(InteractionDiagnostic.id))
        .join(Interaction, Interaction.id == InteractionDiagnostic.interaction_id)
        .where(Interaction.is_helpful.is_(True))
    ).one()
    diagnostics_on_no_feedback = session.exec(
        select(func.count(InteractionDiagnostic.id))
        .join(Interaction, Interaction.id == InteractionDiagnostic.interaction_id)
        .where(Interaction.is_helpful.is_(None))
    ).one()

    if team_twins < 1:
        raise RuntimeError("Seed validation failed: expected at least one team twin.")
    if private_colleague_calls != 0:
        raise RuntimeError("Seed validation failed: private twins contain colleague interactions.")
    if team_colleague_calls <= 0:
        raise RuntimeError("Seed validation failed: team twins contain no colleague interactions.")
    if interaction_before_user_registration != 0:
        raise RuntimeError("Seed validation failed: interactions exist before user registration.")
    if interaction_before_twin_creation != 0:
        raise RuntimeError("Seed validation failed: interactions exist before twin creation.")
    if thumb_down_interactions != diagnostics_count:
        raise RuntimeError(
            "Seed validation failed: thumb-down interaction count does not match diagnostic count."
        )
    if diagnostics_on_thumb_up != 0 or diagnostics_on_no_feedback != 0:
        raise RuntimeError(
            "Seed validation failed: diagnostics exist on non-thumb-down interactions."
        )

    tag_rows = session.exec(
        select(InteractionDiagnosticTag.diagnostic_id, InteractionDiagnosticTag.category)
    ).all()
    tags_by_diagnostic: dict[int, list[str]] = defaultdict(list)
    for diagnostic_id, category in tag_rows:
        category_value = category.value if isinstance(category, DiagnosticCategory) else str(category)
        tags_by_diagnostic[int(diagnostic_id)].append(category_value)

    diagnostic_ids = session.exec(select(InteractionDiagnostic.id)).all()
    allowed_categories = {category.value for category in DIAGNOSTIC_CATEGORIES}
    for diagnostic_id in diagnostic_ids:
        categories = tags_by_diagnostic.get(int(diagnostic_id), [])
        if len(categories) not in {1, 2}:
            raise RuntimeError(
                f"Seed validation failed: diagnostic {diagnostic_id} has {len(categories)} tags."
            )
        if len(categories) != len(set(categories)):
            raise RuntimeError(
                f"Seed validation failed: diagnostic {diagnostic_id} contains duplicate tags."
            )
        if any(category not in allowed_categories for category in categories):
            raise RuntimeError(
                f"Seed validation failed: diagnostic {diagnostic_id} contains invalid tag values."
            )

    validate_user_departments(session)


def print_weekly_team_colleague_ratio(session: Session, start_date: datetime, now: datetime) -> None:
    rows = session.exec(
        select(Interaction.created_at, Interaction.user_id, Twin.owner_id, Twin.visibility)
        .join(Twin, Twin.id == Interaction.twin_id)
        .where(Interaction.created_at >= start_date)
        .where(Interaction.created_at < now)
        .order_by(Interaction.created_at)
    ).all()

    bucket_totals: dict[str, int] = defaultdict(int)
    bucket_colleague: dict[str, int] = defaultdict(int)

    for created_at, user_id, owner_id, visibility in rows:
        created_at = ensure_aware_utc(created_at)
        if visibility != "team":
            continue

        week_start = created_at - timedelta(days=created_at.weekday())
        label = week_start.strftime("%Y-%m-%d")
        bucket_totals[label] += 1
        if user_id != owner_id:
            bucket_colleague[label] += 1

    if not bucket_totals:
        print("No team interactions found for weekly ratio check.")
        return

    print("Weekly team colleague ratio:")
    for label in sorted(bucket_totals.keys()):
        total = bucket_totals[label]
        colleague = bucket_colleague[label]
        ratio = (colleague / total) * 100 if total else 0
        print(f"  {label}: {ratio:.1f}% ({colleague}/{total})")


def generate_mock_data() -> None:
    """Seed the database with realistic 30-day demo data."""
    print("Initializing database tables (reset + recreate)...")
    reset_database()

    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=TIME_WINDOW_DAYS)

    print("Seeding users...")
    with Session(engine) as session:
        users: list[User] = []
        for _ in range(NUM_USERS):
            user = User(
                id=f"U{fake.unique.random_number(digits=8)}",
                name=fake.name(),
                department=sample_department(),
                created_at=fake.date_time_between(start_date=start_date, end_date=now, tzinfo=timezone.utc),
            )
            users.append(user)
            session.add(user)
        session.commit()

        print("Seeding twins...")
        twins: list[Twin] = []
        owner_users = random.sample(users, NUM_TWINS)
        team_owner_ids = build_team_owner_ids(owner_users)

        for owner in owner_users:
            owner_created_at = ensure_aware_utc(owner.created_at)
            proposed_created_at = owner_created_at + timedelta(days=random.randint(0, 2))
            twin_created_at = min(proposed_created_at, now)
            visibility = "team" if owner.id in team_owner_ids else "private"

            twin = Twin(
                owner_id=owner.id,
                name=f"{owner.name.split()[0]}'s Expert Twin",
                visibility=visibility,
                created_at=twin_created_at,
            )
            twins.append(twin)
            session.add(twin)
        session.commit()

        for twin in twins:
            session.refresh(twin)

        user_by_id = {user.id: user for user in users}
        user_created_at_by_id = {user.id: ensure_aware_utc(user.created_at) for user in users}

        print("Seeding interactions with time-progressive team adoption...")

        twin_weights = [random.expovariate(1.2) for _ in twins]

        for _ in range(NUM_INTERACTIONS):
            selected_twin = random.choices(twins, weights=twin_weights, k=1)[0]
            selected_twin_created_at = ensure_aware_utc(selected_twin.created_at)
            interaction_window_start = max(start_date, selected_twin_created_at)
            interaction_created_at = fake.date_time_between(
                start_date=interaction_window_start,
                end_date=now,
                tzinfo=timezone.utc,
            )
            interaction_created_at = ensure_aware_utc(interaction_created_at)
            if interaction_created_at < selected_twin_created_at:
                interaction_created_at = selected_twin_created_at

            owner_user = user_by_id[selected_twin.owner_id]

            if selected_twin.visibility == "private":
                interacting_user = owner_user
            else:
                progress = (interaction_created_at - start_date).total_seconds() / (now - start_date).total_seconds()
                progress = clamp(progress, 0.0, 1.0)

                p_colleague = TEAM_BASE_COLLEAGUE_RATIO + progress * (TEAM_TARGET_COLLEAGUE_RATIO - TEAM_BASE_COLLEAGUE_RATIO)
                colleague_pool = [
                    u
                    for u in users
                    if u.id != selected_twin.owner_id and user_created_at_by_id[u.id] <= interaction_created_at
                ]

                if colleague_pool and random.random() < p_colleague:
                    interacting_user = random.choice(colleague_pool)
                else:
                    interacting_user = owner_user

            interacting_user_created_at = user_created_at_by_id[interacting_user.id]
            if interaction_created_at < interacting_user_created_at:
                interaction_created_at = interacting_user_created_at

            prompt_len = random.randint(10, 300)
            response_len = random.randint(50, 1500)
            processing_time = 300 + int(response_len * 1.5) + random.randint(10, 200)
            feedback = random.choices([True, False, None], weights=[0.4, 0.1, 0.5], k=1)[0]

            interaction = Interaction(
                twin_id=selected_twin.id,
                user_id=interacting_user.id,
                source_channel=random.choices(["slack_dm", "slack_channel", "web_app"], weights=[0.6, 0.3, 0.1], k=1)[0],
                prompt_length=prompt_len,
                response_length=response_len,
                processing_time_ms=processing_time,
                is_helpful=feedback,
                created_at=interaction_created_at,
            )
            session.add(interaction)

            if feedback is False:
                session.flush()
                diagnostic = InteractionDiagnostic(
                    interaction_id=interaction.id,
                    created_at=interaction_created_at,
                    updated_at=interaction_created_at,
                )
                session.add(diagnostic)
                session.flush()

                for category in build_random_diagnostic_categories():
                    session.add(
                        InteractionDiagnosticTag(
                            diagnostic_id=diagnostic.id,
                            category=category,
                            created_at=interaction_created_at,
                        )
                    )

        session.commit()

        validate_seed_data(session)

        print("Seed completed successfully.")
        print_weekly_team_colleague_ratio(session, start_date, now)


if __name__ == "__main__":
    generate_mock_data()
