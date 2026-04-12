import random

from sqlalchemy import event, text
from sqlmodel import SQLModel, Session, create_engine
from app.models import Interaction, InteractionDiagnostic, InteractionDiagnosticTag, Twin, User

# The SQLite database file will be created in the backend root directory
sqlite_file_name = "ai_twin.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

# CRITICAL: "check_same_thread": False is required for SQLite + FastAPI.
# FastAPI can process multiple requests concurrently in different threads. 
# By default, SQLite prevents this. This argument disables that strict check.
connect_args = {"check_same_thread": False}

# The engine is the core interface to the database
engine = create_engine(sqlite_url, connect_args=connect_args)

DEPARTMENT_DISTRIBUTION = [
    ("Engineering", 35),
    ("Sales", 20),
    ("HR", 10),
    ("Finance", 10),
    ("Marketing", 10),
    ("Ops", 8),
    ("Support", 5),
    ("Legal", 2),
]
DEPARTMENT_NAMES = [item[0] for item in DEPARTMENT_DISTRIBUTION]
DEPARTMENT_WEIGHTS = [item[1] for item in DEPARTMENT_DISTRIBUTION]


def sample_department() -> str:
    return random.choices(DEPARTMENT_NAMES, weights=DEPARTMENT_WEIGHTS, k=1)[0]


@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record) -> None:
    # SQLite requires explicit opt-in for FK constraints and cascading deletes.
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

def ensure_schema_compatibility():
    """
    Lightweight schema compatibility for existing SQLite databases.
    Adds Twin.visibility and User.department when upgrading from older versions.
    """
    with engine.begin() as connection:
        twin_table_exists = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='twin'")
        ).first()
        if twin_table_exists:
            twin_columns = connection.execute(text("PRAGMA table_info(twin)")).fetchall()
            twin_column_names = {row[1] for row in twin_columns}

            if "visibility" not in twin_column_names:
                connection.execute(
                    text("ALTER TABLE twin ADD COLUMN visibility TEXT NOT NULL DEFAULT 'private'")
                )

            # Defensive backfill in case legacy rows have empty/null values.
            connection.execute(
                text("UPDATE twin SET visibility = 'private' WHERE visibility IS NULL OR visibility = ''")
            )

        user_table_exists = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='user'")
        ).first()
        if not user_table_exists:
            return

        user_columns = connection.execute(text("PRAGMA table_info(user)")).fetchall()
        user_column_names = {row[1] for row in user_columns}
        if "department" not in user_column_names:
            connection.execute(
                text("ALTER TABLE user ADD COLUMN department TEXT NOT NULL DEFAULT 'Unknown'")
            )

        rows_to_fill = connection.execute(
            text(
                """
                SELECT id
                FROM user
                WHERE department IS NULL OR department = '' OR department = 'Unknown'
                """
            )
        ).fetchall()

        if rows_to_fill:
            updates = [{"id": row[0], "department": sample_department()} for row in rows_to_fill]
            connection.execute(
                text("UPDATE user SET department = :department WHERE id = :id"),
                updates,
            )

        connection.execute(
            text("UPDATE user SET department = 'Unknown' WHERE department IS NULL OR department = ''")
        )

def create_db_and_tables():
    """
    Creates the database file and all tables based on our SQLModel classes.
    If the tables already exist, it safely ignores them.
    """
    SQLModel.metadata.create_all(engine)
    ensure_schema_compatibility()

def get_session():
    """
    Dependency function for FastAPI. 
    Yields a database session for each request and automatically closes it after.
    """
    with Session(engine) as session:
        yield session
