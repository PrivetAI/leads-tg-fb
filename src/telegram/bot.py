import asyncio
from datetime import datetime
from typing import Callable

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler
from telegram.error import BadRequest

from src.config import config
from src.utils.logger import logger


def escape_markdown(text: str) -> str:
    """Escape Markdown special characters in text"""
    if not text:
        return ""
    # Escape backslash first, then other special chars
    text = text.replace('\\', '\\\\')
    for char in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        text = text.replace(char, '\\' + char)
    return text


class NotificationBot:
    def __init__(self):
        self.app = Application.builder().token(config.bot_token).build()
        self.bot = self.app.bot
        self.chat_id = config.admin_chat_id
        self._process_callback: Callable | None = None
        self._facebook_callback: Callable | None = None
        self._reset_callback: Callable | None = None
        self.is_paused = False
        self.is_facebook_paused = False
        self.last_scan_time: datetime | None = None
        self.last_facebook_scan_time: datetime | None = None

    def set_process_callback(self, callback: Callable):
        """Set callback for manual Telegram processing trigger"""
        self._process_callback = callback

    def set_facebook_callback(self, callback: Callable):
        """Set callback for manual Facebook processing trigger"""
        self._facebook_callback = callback

    def set_reset_callback(self, callback: Callable):
        """Set callback for resetting chat states"""
        self._reset_callback = callback

    async def start(self):
        """Start bot with command handlers"""
        self.app.add_handler(CommandHandler("start", self._handle_start))
        self.app.add_handler(CommandHandler("scan", self._handle_scan))
        self.app.add_handler(CommandHandler("scanfb", self._handle_scanfb))
        self.app.add_handler(CommandHandler("pause", self._handle_pause))
        self.app.add_handler(CommandHandler("pausefb", self._handle_pausefb))
        self.app.add_handler(CommandHandler("resume", self._handle_resume))
        self.app.add_handler(CommandHandler("resumefb", self._handle_resumefb))
        self.app.add_handler(CommandHandler("status", self._handle_status))
        self.app.add_handler(CommandHandler("reset", self._handle_reset))
        self.app.add_handler(CommandHandler("help", self._handle_help))
        self.app.add_handler(CallbackQueryHandler(self._handle_callback))

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True, poll_interval=10.0)
        logger.info("Bot started with command handlers")

    async def stop(self):
        """Stop bot"""
        await self.app.updater.stop()
        await self.app.stop()
        await self.app.shutdown()

    async def _handle_start(self, update: Update, context):
        """Handle /start command"""
        keyboard = [
            [InlineKeyboardButton("📱 Telegram", callback_data="scan")],
            [InlineKeyboardButton("📘 Facebook", callback_data="scanfb")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "🤖 *Lead Parser Bot*\n\n"
            "Выберите источник для сканирования:",
            parse_mode="Markdown",
            reply_markup=reply_markup,
        )

    async def _handle_scan(self, update: Update, context):
        """Handle /scan command (Telegram)"""
        if self._process_callback:
            await update.message.reply_text("🔄 Запускаю сканирование Telegram...")
            asyncio.create_task(self._process_callback())
        else:
            await update.message.reply_text("❌ Обработчик Telegram не настроен")

    async def _handle_scanfb(self, update: Update, context):
        """Handle /scanfb command (Facebook)"""
        if self._facebook_callback:
            await update.message.reply_text("🔄 Запускаю сканирование Facebook...")
            asyncio.create_task(self._facebook_callback())
        else:
            await update.message.reply_text("❌ Обработчик Facebook не настроен")

    async def _handle_callback(self, update: Update, context):
        """Handle button callbacks"""
        query = update.callback_query
        await query.answer()

        if query.data == "scan" and self._process_callback:
            await query.edit_message_text("🔄 Запускаю сканирование Telegram...")
            asyncio.create_task(self._process_callback())
        elif query.data == "scanfb" and self._facebook_callback:
            await query.edit_message_text("🔄 Запускаю сканирование Facebook...")
            asyncio.create_task(self._facebook_callback())

    async def _handle_pause(self, update: Update, context):
        """Handle /pause command (Telegram)"""
        self.is_paused = True
        await update.message.reply_text("⏸️ Telegram сканирование приостановлено.\n/scan — ручной запуск\n/resume — возобновить")

    async def _handle_pausefb(self, update: Update, context):
        """Handle /pausefb command (Facebook)"""
        self.is_facebook_paused = True
        await update.message.reply_text("⏸️ Facebook сканирование приостановлено.\n/scanfb — ручной запуск\n/resumefb — возобновить")

    async def _handle_resume(self, update: Update, context):
        """Handle /resume command (Telegram)"""
        self.is_paused = False
        await update.message.reply_text("▶️ Telegram сканирование возобновлено.")

    async def _handle_resumefb(self, update: Update, context):
        """Handle /resumefb command (Facebook)"""
        self.is_facebook_paused = False
        await update.message.reply_text("▶️ Facebook сканирование возобновлено.")

    async def _handle_status(self, update: Update, context):
        """Handle /status command"""
        tg_status = "⏸️ Приостановлено" if self.is_paused else "▶️ Активно"
        fb_status = "⏸️ Приостановлено" if self.is_facebook_paused else "▶️ Активно"
        fb_enabled = "✅" if config.facebook_enabled else "❌"
        
        last_tg = self.last_scan_time.strftime("%H:%M:%S") if self.last_scan_time else "—"
        last_fb = self.last_facebook_scan_time.strftime("%H:%M:%S") if self.last_facebook_scan_time else "—"
        
        await update.message.reply_text(
            f"📊 *Статус бота*\n\n"
            f"📱 *Telegram*: {tg_status}\n"
            f"🕐 Последнее: {last_tg}\n\n"
            f"📘 *Facebook* {fb_enabled}: {fb_status}\n"
            f"🕐 Последнее: {last_fb}",
            parse_mode="Markdown"
        )

    async def _handle_reset(self, update: Update, context):
        """Handle /reset command - clear chat states for re-processing"""
        if self._reset_callback:
            count = await self._reset_callback()
            await update.message.reply_text(
                f"🔄 Сброшено {count} чатов.\n"
                f"Следующий /scan загрузит сообщения за 24ч из всех чатов."
            )
        else:
            await update.message.reply_text("❌ Обработчик сброса не настроен")

    async def _handle_help(self, update: Update, context):
        """Handle /help command - show all available commands"""
        help_text = (
            "📚 *Доступные команды:*\n\n"
            "*Сканирование:*\n"
            "/scan — Запустить сканирование Telegram\n"
            "/scanfb — Запустить сканирование Facebook\n\n"
            "*Управление:*\n"
            "/pause — Приостановить авто-сканирование Telegram\n"
            "/pausefb — Приостановить авто-сканирование Facebook\n"
            "/resume — Возобновить Telegram\n"
            "/resumefb — Возобновить Facebook\n\n"
            "*Сброс:*\n"
            "/reset — Сбросить состояние чатов Telegram (для переобработки сообщений за 24ч)\n\n"
            "*Инфо:*\n"
            "/status — Статус бота\n"
            "/help — Эта справка"
        )
        await update.message.reply_text(help_text, parse_mode="Markdown")

    async def send_lead(
        self,
        username: str | None,
        user_id: int,
        first_name: str | None,
        chat_id: int,
        chat_title: str | None,
        chat_username: str | None,
        message_id: int,
        text: str,
        topic_id: int | None = None,
        confidence: float = 0.0,
        reason: str = "",
        lead_type: str = "property",
    ):
        """Send lead notification to admin chat"""
        if username:
            contact = f"@{username}"
        else:
            name = first_name or "Пользователь"
            contact = f"[{escape_markdown(name)}](tg://user?id={user_id})"

        # Build message link
        if chat_username:
            # Public chat
            if topic_id:
                msg_link = f"https://t.me/{chat_username}/{topic_id}/{message_id}"
            else:
                msg_link = f"https://t.me/{chat_username}/{message_id}"
            chat_link = f"[{escape_markdown(chat_title)}]({msg_link})"
        else:
            # Private chat - use internal link format
            chat_id_positive = abs(chat_id) % (10**10)  # Convert to positive format
            if topic_id:
                msg_link = f"https://t.me/c/{chat_id_positive}/{topic_id}/{message_id}"
            else:
                msg_link = f"https://t.me/c/{chat_id_positive}/{message_id}"
            chat_title_safe = escape_markdown(chat_title or 'Приватный чат')
            chat_link = f"[{chat_title_safe}]({msg_link})"

        # Type-specific emoji and label
        type_emoji = "🏠" if lead_type == "property" else "🚗"
        type_label = "Недвижимость" if lead_type == "property" else "Транспорт"

        confidence_pct = int(confidence * 100)
        message = (
            f"{type_emoji} *Новый лид!* ({confidence_pct}%)\n"
            f"📋 Тип: {type_label}\n\n"
            f"👤 Контакт: {contact}\n"
            f"💬 Чат: {chat_link}\n"
            f"📝 Сообщение:\n{escape_markdown(text[:400])}\n\n"
            f"💡 _{escape_markdown(reason)}_"
        )

        try:
            logger.debug(f"Sending lead (len={len(message)}): {message[:300]}...")
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown",
            )
            logger.info(f"Lead sent: user_id={user_id}")
        except BadRequest as e:
            # Fallback to plain text if Markdown parsing fails
            logger.warning(f"Markdown error for user_id={user_id}: {e}. Retrying plain text...")
            try:
                plain_contact = f"@{username}" if username else f"{first_name or 'Пользователь'} (ID: {user_id})"
                plain_message = (
                    f"{type_emoji} Новый лид! ({confidence_pct}%)\n"
                    f"📋 Тип: {type_label}\n\n"
                    f"👤 Контакт: {plain_contact}\n"
                    f"💬 Чат: {chat_title or 'Неизвестный'}\n"
                    f"🔗 Ссылка: {msg_link}\n"
                    f"📝 Сообщение:\n{text[:400]}\n\n"
                    f"💡 {reason}"
                )
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=plain_message,
                )
                logger.info(f"Lead sent (plain fallback): user_id={user_id}")
            except Exception as e2:
                logger.error(f"Failed to send lead (plain): {e2}")
        except Exception as e:
            logger.error(f"Failed to send lead: {e}")

    async def send_stats(
        self,
        total: int,
        filtered: int,
        analyzed: int,
        leads: int,
        source: str = "telegram",
        groups_count: int = 0,
    ):
        """Send statistics summary to admin chat"""
        source_emoji = "📱" if source == "telegram" else "📘"
        source_name = "Telegram" if source == "telegram" else "Facebook"
        callback = "scan" if source == "telegram" else "scanfb"
        
        groups_line = f"📂 Групп просканировано: {groups_count}\n" if groups_count > 0 else ""
        
        message = (
            f"{source_emoji} *{source_name} статистика:*\n\n"
            f"{groups_line}"
            f"📨 Сообщений получено: {total}\n"
            f"🔍 После фильтрации: {filtered}\n"
            f"🤖 Проанализировано: {analyzed}\n"
            f"🎯 Лидов найдено: {leads}"
        )

        # Add scan button after stats
        keyboard = [[InlineKeyboardButton(f"🔍 {source_name} снова", callback_data=callback)]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown",
                reply_markup=reply_markup,
            )
            logger.info(f"Stats sent ({source}): total={total}, filtered={filtered}, analyzed={analyzed}, leads={leads}")
        except Exception as e:
            logger.error(f"Failed to send stats: {e}")

    async def send_facebook_lead(
        self,
        author_name: str,
        author_id: str | None,
        group_name: str,
        post_url: str,
        text: str,
        confidence: float = 0.0,
        reason: str = "",
        lead_type: str = "property",
    ):
        """Send Facebook lead notification to admin chat"""
        # Build contact link
        if author_id:
            contact = f"[{author_name}](https://facebook.com/profile.php?id={author_id})"
        else:
            contact = author_name

        # Type-specific emoji and label
        type_emoji = "🏠" if lead_type == "property" else "🚗"
        type_label = "Недвижимость" if lead_type == "property" else "Транспорт"

        confidence_pct = int(confidence * 100)
        message = (
            f"📘 {type_emoji} *Facebook лид!* ({confidence_pct}%)\n"
            f"📋 Тип: {type_label}\n\n"
            f"👤 Автор: {contact}\n"
            f"💬 Группа: [{escape_markdown(group_name)}]({post_url})\n"
            f"📝 Пост:\n{escape_markdown(text[:400])}\n\n"
            f"💡 _{escape_markdown(reason)}_"
        )

        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown",
            )
            logger.info(f"Facebook lead sent: author={author_name}")
        except Exception as e:
            logger.error(f"Failed to send Facebook lead: {e}")

    async def send_facebook_leads_batch(
        self,
        leads: list[dict],
    ):
        """
        Send all Facebook leads in ONE combined message.
        
        Each lead dict should have:
        - author_name: str
        - author_id: str | None
        - group_name: str
        - post_url: str
        - text: str
        - confidence: float
        - reason: str
        - lead_type: str
        """
        if not leads:
            return
        
        lines = [f"📘 *Facebook: {len(leads)} лидов найдено!*\n"]
        
        for i, lead in enumerate(leads, 1):
            author_name = lead.get("author_name", "Unknown")
            author_id = lead.get("author_id")
            group_name = lead.get("group_name", "")
            post_url = lead.get("post_url", "")
            text = lead.get("text", "")[:150]
            confidence = lead.get("confidence", 0)
            lead_type = lead.get("lead_type", "property")
            
            # Contact link
            if author_id:
                contact = f"[{author_name}](https://facebook.com/profile.php?id={author_id})"
            else:
                contact = author_name
            
            # Type emoji
            type_emoji = "🏠" if lead_type == "property" else "🚗"
            confidence_pct = int(confidence * 100)
            
            lines.append(
                f"{i}. {type_emoji} ({confidence_pct}%) {contact}\n"
                f"   📍 [{escape_markdown(group_name[:30])}]({post_url})\n"
                f"   _{escape_markdown(text[:100])}..._\n"
            )
        
        message = "\n".join(lines)
        
        # Telegram has 4096 char limit - split if needed
        if len(message) > 4000:
            message = message[:4000] + "\n\n... (обрезано)"
        
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            logger.info(f"Facebook leads batch sent: {len(leads)} leads")
        except Exception as e:
            logger.error(f"Failed to send Facebook leads batch: {e}")

