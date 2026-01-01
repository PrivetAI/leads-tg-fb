from src.config import config
from src.telegram.userbot import ParsedMessage
from src.utils.logger import logger


def filter_messages(messages: list[ParsedMessage]) -> list[ParsedMessage]:
    """
    Filter messages by exclude words.
    All users' messages are analyzed (no user deduplication).
    """
    filtered = []

    for msg in messages:
        text_lower = msg.text.lower()

        # Check exclude words
        excluded = False
        for word in config.exclude_words:
            if word in text_lower:
                excluded = True
                break

        if not excluded:
            filtered.append(msg)

    logger.info(f"Filtered {len(messages)} -> {len(filtered)} messages")
    return filtered

