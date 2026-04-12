from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import Column, ForeignKey, Integer, UniqueConstraint
from sqlmodel import Field, SQLModel


class DiagnosticCategory(str, Enum):
    HALLUCINATION = "Hallucination"
    OUTDATED_INFO = "OutdatedInfo"
    TONE = "Tone"
    INSTRUCTIONS_UNFOLLOWED = "InstructionsUnfollowed"


class User(SQLModel, table=True):
    """
    Dimension Table: Represents the people interacting with the application (e.g., Slack users).
    """
    id: str = Field(primary_key=True, description="Unique identifier for the user (e.g., Slack ID).")
    name: str = Field(description="Display name of the user.")
    department: str = Field(
        default="Unknown",
        description="Department label (e.g., Engineering, Sales, HR).",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Twin(SQLModel, table=True):
    """
    Dimension Table: Represents the AI digital twins created in the system.
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    owner_id: str = Field(foreign_key="user.id", description="The user who created and owns this Twin.")
    name: str = Field(description="Name of the AI Twin (e.g., 'Alex's Work Twin').")
    visibility: str = Field(
        default="private",
        description="Enum: 'private' (owner only) or 'team' (visible to colleagues).",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Interaction(SQLModel, table=True):
    """
    Fact Table: The core table recording every conversation event. 
    Crucial for generating dashboard metrics.
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    twin_id: int = Field(foreign_key="twin.id", description="Which Twin was queried.")
    user_id: str = Field(foreign_key="user.id", description="Who initiated the query.")
    
    # --- Product Metrics ---
    source_channel: str = Field(description="Enum: 'slack_dm', 'slack_channel', 'web_app'. Tracks where the interaction occurred.")
    prompt_length: int = Field(description="Character/token count of the user's input. Useful for cost estimation.")
    response_length: int = Field(description="Character/token count of the AI's response.")
    is_helpful: Optional[bool] = Field(default=None, description="User feedback: True (Thumbs up), False (Thumbs down), Null (No feedback).")
    
    # --- Engineering Metrics ---
    processing_time_ms: int = Field(description="Time taken to generate the response (ms). Used for latency aggregation.")
    
    # --- Time Series ---
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class InteractionDiagnostic(SQLModel, table=True):
    """
    Diagnostic case for a single thumb-down interaction.
    Keeps high-frequency Interaction fact table lean.
    """
    __table_args__ = (UniqueConstraint("interaction_id", name="uq_interaction_diagnostic_interaction_id"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    interaction_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("interaction.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        description="One-to-one reference to the diagnosed interaction.",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class InteractionDiagnosticTag(SQLModel, table=True):
    """
    Structured multi-tag reasons attached to one diagnostic case.
    """
    __table_args__ = (UniqueConstraint("diagnostic_id", "category", name="uq_diagnostic_tag_category"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    diagnostic_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("interactiondiagnostic.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        description="Parent diagnostic case id.",
    )
    category: DiagnosticCategory = Field(description="Diagnostic reason category.")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
