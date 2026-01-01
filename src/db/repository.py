from datetime import datetime

from sqlalchemy.orm import Session

from src.db.models import ChatState, FacebookGroupState, FacebookProcessedPost, Message, ProcessedUser, SessionLocal


class Repository:
    def __init__(self):
        self.session: Session = SessionLocal()

    def close(self):
        self.session.close()

    def get_or_create_user(self, telegram_user_id: int, username: str | None, first_name: str | None) -> ProcessedUser:
        user = self.session.query(ProcessedUser).filter_by(telegram_user_id=telegram_user_id).first()
        if not user:
            user = ProcessedUser(
                telegram_user_id=telegram_user_id,
                username=username,
                first_name=first_name,
            )
            self.session.add(user)
            self.session.commit()
        return user

    def is_user_processed(self, telegram_user_id: int) -> bool:
        return self.session.query(ProcessedUser).filter_by(telegram_user_id=telegram_user_id).first() is not None

    def save_message(
        self,
        telegram_message_id: int,
        chat_id: int,
        chat_title: str | None,
        chat_username: str | None,
        user: ProcessedUser,
        text: str,
        created_at: datetime,
        source: str = "telegram",
    ) -> Message:
        msg = Message(
            telegram_message_id=telegram_message_id,
            chat_id=chat_id,
            chat_title=chat_title,
            chat_username=chat_username,
            user_id=user.id,
            text=text,
            created_at=created_at,
            source=source,
        )
        self.session.add(msg)
        self.session.commit()
        return msg

    def update_message_lead_status(self, message_id: int, is_lead: bool, confidence: float = 0.0, reason: str = ""):
        msg = self.session.query(Message).filter_by(id=message_id).first()
        if msg:
            msg.is_lead = is_lead
            msg.confidence = confidence
            msg.reason = reason
            msg.analyzed_at = datetime.utcnow()
            self.session.commit()

    def get_last_message_id(self, chat_id: int) -> int | None:
        state = self.session.query(ChatState).filter_by(chat_id=chat_id).first()
        return state.last_message_id if state else None

    def update_last_message_id(self, chat_id: int, message_id: int):
        state = self.session.query(ChatState).filter_by(chat_id=chat_id).first()
        if state:
            state.last_message_id = message_id
            state.updated_at = datetime.utcnow()
        else:
            state = ChatState(chat_id=chat_id, last_message_id=message_id)
            self.session.add(state)
        self.session.commit()

    # Facebook methods
    def get_facebook_group_state(self, group_id: str) -> datetime | None:
        """Get last scan time for a Facebook group"""
        state = self.session.query(FacebookGroupState).filter_by(group_id=group_id).first()
        return state.last_scan_time if state else None

    def update_facebook_group_state(self, group_id: str, group_name: str, scan_time: datetime):
        """Update last scan time for a Facebook group"""
        state = self.session.query(FacebookGroupState).filter_by(group_id=group_id).first()
        if state:
            state.group_name = group_name
            state.last_scan_time = scan_time
            state.updated_at = datetime.utcnow()
        else:
            state = FacebookGroupState(
                group_id=group_id,
                group_name=group_name,
                last_scan_time=scan_time
            )
            self.session.add(state)
        self.session.commit()

    def is_facebook_post_processed(self, post_id: str) -> bool:
        """Check if a Facebook post has already been processed"""
        return self.session.query(FacebookProcessedPost).filter_by(post_id=post_id).first() is not None

    def mark_facebook_post_processed(self, post_id: str, group_id: str):
        """Mark a Facebook post as processed"""
        if not self.is_facebook_post_processed(post_id):
            post = FacebookProcessedPost(post_id=post_id, group_id=group_id)
            self.session.add(post)
            self.session.commit()

    def mark_facebook_posts_batch(self, posts: list[tuple[str, str]]):
        """Mark multiple Facebook posts as processed. posts = [(post_id, group_id), ...]"""
        for post_id, group_id in posts:
            if not self.is_facebook_post_processed(post_id):
                post = FacebookProcessedPost(post_id=post_id, group_id=group_id)
                self.session.add(post)
        self.session.commit()


