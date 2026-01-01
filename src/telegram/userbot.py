import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import DialogFilter

from telethon.tl.functions.messages import GetDialogFiltersRequest

from src.config import config
from src.utils.logger import logger

SESSION_PATH = Path(__file__).parent.parent.parent / "session" / "userbot"


@dataclass
class ParsedMessage:
    message_id: int
    chat_id: int
    chat_title: str | None
    chat_username: str | None
    user_id: int
    username: str | None
    first_name: str | None
    text: str
    date: datetime
    topic_id: int | None = None  # For forum chats with topics
    reply_to_text: str | None = None  # Text of the message being replied to (truncated)


class UserBot:
    def __init__(self):
        self.client = TelegramClient(
            str(SESSION_PATH),
            config.telegram_api_id,
            config.telegram_api_hash,
        )

    async def start(self):
        await self.client.start(phone=config.telegram_phone)
        logger.info("Userbot started")

    async def stop(self):
        await self.client.disconnect()
        logger.info("Userbot stopped")

    async def get_all_group_chats(self) -> list:
        """Get all group chats from account dialogs (excludes broadcast channels)"""
        from telethon.tl.types import Channel, Chat
        
        peers = []
        async for dialog in self.client.iter_dialogs():
            entity = dialog.entity
            # Only groups: Chat (small groups) or Channel with megagroup=True
            # Skip broadcast channels (megagroup=False)
            if isinstance(entity, Chat):
                peers.append(entity)
            elif isinstance(entity, Channel) and getattr(entity, 'megagroup', False):
                peers.append(entity)
        
        logger.info(f"Found {len(peers)} group chats in account")
        return peers

    async def get_folder_chats(self, folder_name: str) -> list:
        """Get chat entities from a Telegram Desktop folder (groups/channels only)
        Use '*' or empty string to get all chats from account.
        """
        # Special case: get all chats
        if not folder_name or folder_name == "*":
            return await self.get_all_group_chats()
        
        result = await self.client(GetDialogFiltersRequest())

        # Log available folders for debugging
        folder_names = [f.title for f in result.filters if isinstance(f, DialogFilter)]
        logger.info(f"Available folders in account: {folder_names}")
        
        for dialog_filter in result.filters:
            if isinstance(dialog_filter, DialogFilter) and dialog_filter.title.lower() == folder_name.lower():
                peers = []
                for peer in dialog_filter.include_peers:
                    # Skip personal chats (PeerUser), only process groups and channels
                    if hasattr(peer, "channel_id"):
                        peers.append(peer)
                    elif hasattr(peer, "chat_id"):
                        peers.append(peer)
                    # Skip PeerUser - personal messages not supported
                
                logger.info(f"Found folder '{folder_name}' with {len(peers)} group/channel chats")
                return peers

        logger.warning(f"Folder '{folder_name}' not found")
        return []

    async def get_new_messages(
        self,
        peer,
        min_id: int | None = None,
    ) -> list[ParsedMessage]:
        """Get new messages from a chat after min_id"""
        from telethon.tl.types import Channel, Chat
        
        messages = []
        
        # Extract chat_id from peer (handle both peer refs and entities)
        if hasattr(peer, "channel_id"):
            chat_id = peer.channel_id
        elif hasattr(peer, "chat_id"):
            chat_id = peer.chat_id
        elif isinstance(peer, (Channel, Chat)):
            chat_id = peer.id
        else:
            return []

        try:
            # Get entity (might already be an entity)
            if isinstance(peer, (Channel, Chat)):
                entity = peer
            else:
                entity = await self.client.get_entity(peer)
            chat_title = getattr(entity, "title", None)
            chat_username = getattr(entity, "username", None)
            is_forum = getattr(entity, "forum", False)

            async for msg in self.client.iter_messages(
                entity,
                min_id=min_id or 0,
                limit=100,
            ):
                if not msg.text or not msg.sender_id:
                    continue

                sender = await msg.get_sender()
                if not sender:
                    continue

                # Extract topic_id for forum chats
                topic_id = None
                if is_forum and msg.reply_to:
                    topic_id = getattr(msg.reply_to, "reply_to_top_id", None) or getattr(msg.reply_to, "reply_to_msg_id", None)

                # Fetch reply-to message text (truncated to 200 chars)
                reply_to_text = None
                if msg.reply_to and not is_forum:
                    reply_to_msg_id = getattr(msg.reply_to, "reply_to_msg_id", None)
                    if reply_to_msg_id:
                        try:
                            reply_msg = await self.client.get_messages(entity, ids=reply_to_msg_id)
                            if reply_msg and reply_msg.text:
                                reply_to_text = reply_msg.text[:200]
                        except:
                            pass  # Ignore errors fetching reply

                messages.append(
                    ParsedMessage(
                        message_id=msg.id,
                        chat_id=chat_id,
                        chat_title=chat_title,
                        chat_username=chat_username,
                        user_id=sender.id,
                        username=getattr(sender, "username", None),
                        first_name=getattr(sender, "first_name", None),
                        text=msg.text,
                        date=msg.date,
                        topic_id=topic_id,
                        reply_to_text=reply_to_text,
                    )
                )

        except Exception as e:
            logger.error(f"Error fetching messages from chat {chat_id}: {e}")

        return messages


async def auth():
    """Interactive authentication for first run"""
    client = TelegramClient(
        str(SESSION_PATH),
        config.telegram_api_id,
        config.telegram_api_hash,
    )
    await client.start(phone=config.telegram_phone)
    print("Authentication successful! Session saved.")
    await client.disconnect()


if __name__ == "__main__":
    import asyncio

    if "--auth" in sys.argv:
        SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
        asyncio.run(auth())
