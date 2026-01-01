from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

from src.config import config


class Base(DeclarativeBase):
    pass


class ProcessedUser(Base):
    __tablename__ = "processed_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    messages: Mapped[list["Message"]] = relationship(back_populates="user")


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_message_id: Mapped[int] = mapped_column(BigInteger)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    chat_title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    chat_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("processed_users.id"))
    text: Mapped[str] = mapped_column(Text)
    is_lead: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime)
    analyzed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    source: Mapped[str] = mapped_column(String(20), default="telegram")  # telegram or facebook

    user: Mapped[ProcessedUser] = relationship(back_populates="messages")


class ChatState(Base):
    __tablename__ = "chat_state"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    last_message_id: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class FacebookGroupState(Base):
    """Tracks last scan time for each Facebook group"""
    __tablename__ = "facebook_group_state"

    group_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    group_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_scan_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class FacebookProcessedPost(Base):
    """Tracks which Facebook posts have already been processed"""
    __tablename__ = "facebook_processed_posts"

    post_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    group_id: Mapped[str] = mapped_column(String(100), index=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


engine = create_engine(config.db_url)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(engine)

