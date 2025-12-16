from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set

DATE_FORMAT = "%Y-%m-%d"


def date_key(ts: Optional[datetime] = None) -> str:
    ts = ts or datetime.utcnow()
    return ts.strftime(DATE_FORMAT)


def date_range(days: int, now: Optional[datetime] = None) -> Set[str]:
    """Return date keys (YYYY-MM-DD) for the last `days` days inclusive."""
    now = now or datetime.utcnow()
    return {date_key(now - timedelta(days=delta)) for delta in range(days)}


@dataclass
class DailyUserStats:
    messages: int = 0
    reactions_given: int = 0


@dataclass
class UserDocument:
    _id: str
    guild_id: int
    user_id: int
    total_messages: int = 0
    reactions_given: int = 0
    reactions_received: int = 0
    daily_stats: Dict[str, DailyUserStats] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DailyServerStats:
    messages: int = 0
    reactions: int = 0
    active_users: List[int] = field(default_factory=list)


@dataclass
class ServerDocument:
    _id: str
    guild_id: int
    stats_channel_id: Optional[int] = None
    stats_message_id: Optional[int] = None
    daily_stats: Dict[str, DailyServerStats] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class MetaDocument:
    _id: str
    guild_id: Optional[int] = None
    data: dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


def user_key(guild_id: int, user_id: int) -> str:
    return f"{guild_id}:{user_id}"


def relevant_dates(days: int, now: Optional[datetime] = None) -> Iterable[str]:
    now = now or datetime.utcnow()
    for delta in range(days):
        yield date_key(now - timedelta(days=delta))
