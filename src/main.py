import asyncio
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.config import config
from src.db.models import init_db
from src.db.repository import Repository
from src.processing.analyzer import analyze_messages_batch
from src.processing.filter import filter_messages
from src.telegram.bot import NotificationBot
from src.telegram.userbot import UserBot
from src.utils.logger import logger


async def process_cycle(userbot: UserBot, bot: NotificationBot, prompt_type: str = "property"):
    """One processing cycle (runs every N minutes)"""
    logger.info("=" * 50)
    logger.info(f"Starting processing cycle (prompt_type={prompt_type})")

    repo = Repository()
    total_messages = 0
    filtered_messages = []
    leads_found = 0

    try:
        # 1. Get chats from folder
        peers = await userbot.get_folder_chats(config.folder_name)
        if not peers:
            logger.warning("No chats found in folder")
            await bot.send_stats(0, 0, 0, 0)
            return

        # 2. Collect new messages from all chats
        all_messages = []
        chats_to_update = []  # Store (chat_id, max_msg_id) for update after successful analysis
        
        for peer in peers:
        # Extract chat_id for DB state tracking (handle both peer refs and entities)
            if hasattr(peer, "channel_id"):
                chat_id = peer.channel_id
            elif hasattr(peer, "chat_id"):
                chat_id = peer.chat_id
            else:
                chat_id = peer.id  # Direct entity object
            
            min_id = repo.get_last_message_id(chat_id)
            
            # New chat - get messages from last 24 hours for analysis
            if min_id is None:
                messages = await userbot.get_new_messages(peer, min_id=0)
                if messages:
                    # Filter to last 24 hours
                    from datetime import datetime, timedelta, timezone
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
                    recent_messages = [m for m in messages if m.date.replace(tzinfo=timezone.utc) > cutoff]
                    
                    max_msg_id = max(m.message_id for m in messages)
                    chats_to_update.append((chat_id, max_msg_id))
                    
                    if recent_messages:
                        all_messages.extend(recent_messages)
                        logger.info(f"New chat {chat_id}: analyzing {len(recent_messages)} messages from last 24h")
                    else:
                        logger.info(f"New chat {chat_id}: no messages in last 24h")
                continue
            
            messages = await userbot.get_new_messages(peer, min_id)

            if messages:
                max_msg_id = max(m.message_id for m in messages)
                chats_to_update.append((chat_id, max_msg_id))
                all_messages.extend(messages)

        total_messages = len(all_messages)
        logger.info(f"Fetched {total_messages} new messages from {len(peers)} chats")

        if not all_messages:
            await bot.send_stats(0, 0, 0, 0)
            return

        # 3. Filter messages
        filtered = filter_messages(all_messages)
        
        # Deduplicate by exact text match
        seen_texts = set()
        deduplicated = []
        for msg in filtered:
            if msg.text not in seen_texts:
                seen_texts.add(msg.text)
                deduplicated.append(msg)
        
        logger.info(f"After deduplication: {len(deduplicated)} unique texts (was {len(filtered)})")
        filtered = deduplicated
        
        filtered_messages = []

        # Save filtered messages to DB
        for msg in filtered:
            user = repo.get_or_create_user(msg.user_id, msg.username, msg.first_name)
            db_msg = repo.save_message(
                telegram_message_id=msg.message_id,
                chat_id=msg.chat_id,
                chat_title=msg.chat_title,
                chat_username=msg.chat_username,
                user=user,
                text=msg.text,
                created_at=msg.date,
            )
            filtered_messages.append((msg, db_msg, user))

        logger.info(f"Saved {len(filtered_messages)} messages to DB")

        if not filtered_messages:
            await bot.send_stats(total_messages, 0, 0, 0)
            return

        # 4. Analyze with Gemini (batch)
        def format_text_for_analysis(msg):
            if msg.reply_to_text:
                return f'(↩ "{msg.reply_to_text}") {msg.text}'
            return msg.text
        
        # Include user_id so LLM can group messages from same person
        texts_to_analyze = [
            (i, msg.user_id, format_text_for_analysis(msg)) 
            for i, (msg, _, _) in enumerate(filtered_messages)
        ]
        analysis_results, analysis_success = await analyze_messages_batch(texts_to_analyze, prompt_type=prompt_type)

        if not analysis_success:
            logger.warning("Analysis incomplete due to errors, messages NOT marked as analyzed")
            await bot.send_stats(total_messages, len(filtered_messages), 0, 0)
            return

        # 5. Process results and collect leads (only if analysis succeeded)
        leads_list = []
        for i, (msg, db_msg, user) in enumerate(filtered_messages):
            result = analysis_results.get(i)
            if not result:
                continue
            
            is_lead, reason, confidence, lead_type = result
            repo.update_message_lead_status(db_msg.id, is_lead, confidence, reason)

            if is_lead:
                leads_found += 1
                # Use IT emoji for it_services
                if lead_type == "it_services":
                    emoji = "💻"
                elif lead_type == "vehicle":
                    emoji = "🚗"
                else:
                    emoji = "🏠"
                logger.info(f"{emoji} Lead found: user={msg.user_id}, type={lead_type}, confidence={confidence:.0%}")

                # Format contact
                if msg.username:
                    contact = f"@{msg.username}"
                else:
                    from src.telegram.bot import escape_markdown
                    name = msg.first_name or "Пользователь"
                    contact = f"[{escape_markdown(name)}](tg://user?id={msg.user_id})"
                
                # Format chat link
                if msg.chat_username:
                    if msg.topic_id:
                        msg_link = f"https://t.me/{msg.chat_username}/{msg.topic_id}/{msg.message_id}"
                    else:
                        msg_link = f"https://t.me/{msg.chat_username}/{msg.message_id}"
                else:
                    chat_id_positive = abs(msg.chat_id) % (10**10)
                    if msg.topic_id:
                        msg_link = f"https://t.me/c/{chat_id_positive}/{msg.topic_id}/{msg.message_id}"
                    else:
                        msg_link = f"https://t.me/c/{chat_id_positive}/{msg.message_id}"
                
                from src.telegram.bot import escape_markdown
                chat_title_safe = escape_markdown(msg.chat_title or 'Чат')
                chat_link = f"[{chat_title_safe}]({msg_link})"
                
                leads_list.append({
                    "contact": contact,
                    "chat_link": chat_link,
                    "text": msg.text,
                    "confidence": confidence,
                    "reason": reason,
                    "lead_type": lead_type,
                })
        
        # Send all leads in one message
        if leads_list:
            await bot.send_leads_batch(leads_list, source="telegram")

        # 6. Update chat states (only after successful analysis)
        for chat_id, max_msg_id in chats_to_update:
            repo.update_last_message_id(chat_id, max_msg_id)

        # 7. Send stats
        await bot.send_stats(
            total=total_messages,
            filtered=len(filtered_messages),
            analyzed=len(filtered_messages),
            leads=leads_found,
        )

        logger.info(f"Cycle complete: total={total_messages}, filtered={len(filtered_messages)}, leads={leads_found}")

    except Exception as e:
        logger.error(f"Processing cycle error: {e}", exc_info=True)
    finally:
        repo.close()


async def process_facebook_cycle(fb_scraper, bot: NotificationBot, prompt_type: str = "property"):
    """Facebook processing cycle"""
    from src.facebook.scraper import FacebookScraper
    
    logger.info("=" * 50)
    logger.info(f"Starting Facebook processing cycle (prompt_type={prompt_type})")

    repo = Repository()
    total_posts = 0
    filtered_posts = []
    leads_found = 0
    scan_start_time = datetime.now()

    try:
        # 1. Get all user's groups
        groups = await fb_scraper.get_user_groups()
        if not groups:
            logger.warning("No Facebook groups found")
            await bot.send_stats(0, 0, 0, 0, source="facebook")
            return

        logger.info(f"Found {len(groups)} Facebook groups")

        # Get all processed post IDs for early-stop check
        from src.db.models import FacebookProcessedPost
        processed_ids = set()
        try:
            processed_posts = repo.session.query(FacebookProcessedPost.post_id).all()
            processed_ids = {p.post_id for p in processed_posts}
            logger.info(f"Loaded {len(processed_ids)} already-processed post IDs")
        except Exception as e:
            logger.warning(f"Could not load processed IDs: {e}")
        
        # TEST_MODE: 1 worker, 3 groups limit
        import os
        max_workers = 3
        if os.environ.get("TEST_MODE"):
            max_workers = 1
            groups = groups[:3]
            logger.info(f"TEST_MODE: 1 worker, {len(groups)} groups")

        # 2. Collect posts from all groups IN PARALLEL
        all_posts = await fb_scraper.get_groups_posts_parallel(
            groups=groups,
            limit_per_group=config.facebook_posts_per_group,
            max_workers=max_workers,
            processed_ids=processed_ids
        )
        
        # Track scanned groups
        groups_scanned = [(g["id"], g["name"]) for g in groups]

        total_posts = len(all_posts)
        logger.info(f"Fetched {total_posts} posts from {len(groups)} groups")

        if not all_posts:
            await bot.send_stats(0, 0, 0, 0, source="facebook")
            return

        # 3. Filter out already processed posts (by post_id) with colored logging
        from src.utils.logger import log_new_post, log_old_post
        
        new_posts = []
        for post in all_posts:
            if not repo.is_facebook_post_processed(post.post_id):
                new_posts.append(post)
                log_new_post(post.text, source="FB")
            else:
                log_old_post(post.post_id, source="FB")
        
        logger.info(f"New posts: {len(new_posts)} / {total_posts}")
        
        if not new_posts:
            await bot.send_stats(total_posts, 0, 0, 0, source="facebook")
            return

        # 4. Filter posts (exclude words) and track per-group counts
        filtered_posts = []
        posts_per_group = {}  # group_name -> count
        for post in new_posts:
            text_lower = post.text.lower()
            excluded = any(word in text_lower for word in config.exclude_words)
            if not excluded:
                filtered_posts.append(post)
                posts_per_group[post.group_name] = posts_per_group.get(post.group_name, 0) + 1

        logger.info(f"After exclude words filter: {len(filtered_posts)} posts")

        if not filtered_posts:
            # Mark all new posts as processed even if filtered out
            repo.mark_facebook_posts_batch([(p.post_id, p.group_id) for p in new_posts])
            await bot.send_stats(total_posts, 0, 0, 0, source="facebook")
            return

        # 5. Analyze with Gemini (batch)
        texts_to_analyze = [(i, post.text) for i, post in enumerate(filtered_posts)]
        analysis_results, analysis_success = await analyze_messages_batch(texts_to_analyze, prompt_type=prompt_type)

        if not analysis_success:
            logger.warning("Facebook analysis incomplete due to errors")
            await bot.send_stats(total_posts, len(filtered_posts), 0, 0, source="facebook")
            return

        # 6. Process results and collect leads
        leads_list = []
        leads_per_group = {}  # group_name -> count
        for i, post in enumerate(filtered_posts):
            result = analysis_results.get(i)
            if not result:
                continue
            
            is_lead, reason, confidence, lead_type = result

            if is_lead:
                leads_found += 1
                leads_per_group[post.group_name] = leads_per_group.get(post.group_name, 0) + 1
                # Use IT emoji for it_services
                if lead_type == "it_services":
                    emoji = "💻"
                elif lead_type == "vehicle":
                    emoji = "🚗"
                else:
                    emoji = "🏠"
                logger.info(f"{emoji} FB Lead: author={post.author_name}, type={lead_type}, confidence={confidence:.0%}")

                # Format contact
                if post.author_id:
                    contact = f"[{post.author_name}](https://facebook.com/profile.php?id={post.author_id})"
                else:
                    contact = post.author_name
                
                # Format chat link
                from src.telegram.bot import escape_markdown
                chat_link = f"[{escape_markdown(post.group_name[:40])}]({post.post_url})"
                
                leads_list.append({
                    "contact": contact,
                    "chat_link": chat_link,
                    "text": post.text,
                    "confidence": confidence,
                    "reason": reason,
                    "lead_type": lead_type,
                })

        # 7. Send leads in one batch message
        if leads_list:
            await bot.send_leads_batch(leads_list, source="facebook")

        # 8. Mark all new posts as processed (after successful analysis)
        repo.mark_facebook_posts_batch([(p.post_id, p.group_id) for p in new_posts])

        # 9. Update group states
        for group_id, group_name in groups_scanned:
            repo.update_facebook_group_state(group_id, group_name, scan_start_time)

        # 10. Send stats with groups count
        await bot.send_stats(
            total=total_posts,
            filtered=len(filtered_posts),
            analyzed=len(filtered_posts),
            leads=leads_found,
            source="facebook",
            groups_count=len(groups_scanned),
        )
        
        # 11. Send per-group breakdown
        await bot.send_group_breakdown(posts_per_group, leads_per_group)

        logger.info(f"Facebook cycle complete: total={total_posts}, new={len(new_posts)}, filtered={len(filtered_posts)}, leads={leads_found}")

    except Exception as e:
        logger.error(f"Facebook processing cycle error: {e}", exc_info=True)
    finally:
        repo.close()


async def main():
    logger.info("Starting Lead Parser (Telegram + Facebook)")

    # Init database
    init_db()
    logger.info("Database initialized")

    # Init Telegram clients
    userbot = UserBot()
    bot = NotificationBot()

    await userbot.start()
    await bot.start()

    # Init Facebook scraper (if enabled)
    fb_scraper = None
    if config.facebook_enabled:
        from src.facebook.scraper import FacebookScraper
        fb_scraper = FacebookScraper(config.chrome_cdp_url)
        if await fb_scraper.start():
            logger.info("Facebook scraper initialized")
        else:
            logger.warning("Facebook scraper failed to connect. Use /scanfb after starting Chrome with --remote-debugging-port=9222")
            fb_scraper = None

    # Telegram callbacks
    async def run_telegram_cycle(prompt_type: str = "property"):
        await process_cycle(userbot, bot, prompt_type)
        bot.last_scan_time = datetime.now()
    
    async def scheduled_telegram():
        if not bot.is_paused:
            await run_telegram_cycle("property")
        else:
            logger.info("Scheduled Telegram scan skipped (paused)")

    bot.set_process_callback(run_telegram_cycle)

    # Facebook callbacks
    async def run_facebook_cycle(prompt_type: str = "property"):
        nonlocal fb_scraper
        if fb_scraper is None:
            from src.facebook.scraper import FacebookScraper
            fb_scraper = FacebookScraper(config.chrome_cdp_url)
            if not await fb_scraper.start():
                logger.error("Facebook scraper not connected")
                return
        await process_facebook_cycle(fb_scraper, bot, prompt_type)
        bot.last_facebook_scan_time = datetime.now()
    
    async def scheduled_facebook():
        if not bot.is_facebook_paused and config.facebook_enabled:
            await run_facebook_cycle("property")
        else:
            logger.info("Scheduled Facebook scan skipped (paused or disabled)")

    bot.set_facebook_callback(run_facebook_cycle)

    # Reset callback
    async def reset_chat_states():
        repo = Repository()
        try:
            count = repo.reset_telegram_chat_states()
            logger.info(f"Reset {count} chat states via /reset command")
            return count
        finally:
            repo.close()

    bot.set_reset_callback(reset_chat_states)

    # Setup scheduler
    scheduler = AsyncIOScheduler()
    
    if config.scan_mode == "timer":
        scheduler.add_job(
            scheduled_telegram,
            "interval",
            minutes=config.interval_minutes,
        )
        logger.info(f"Telegram scheduler: interval={config.interval_minutes}min")
    
    if config.facebook_enabled and config.scan_mode == "timer":
        scheduler.add_job(
            scheduled_facebook,
            "interval",
            minutes=config.facebook_scan_interval,
        )
        logger.info(f"Facebook scheduler: interval={config.facebook_scan_interval}min")
    
    scheduler.start()
    logger.info(f"Scheduler started. scan_mode={config.scan_mode}, facebook_enabled={config.facebook_enabled}")

    # Run first cycles immediately
    if config.scan_mode == "timer":
        # Launch both in parallel or sequence
        asyncio.create_task(run_facebook_cycle("property"))
        await run_telegram_cycle("property")

    # Keep running
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
        scheduler.shutdown()
        await bot.stop()
        await userbot.stop()
        if fb_scraper:
            await fb_scraper.stop()


if __name__ == "__main__":
    asyncio.run(main())

