"""
Facebook Groups Scraper using Playwright with stealth mode.
Connects to existing Chrome via CDP for authenticated session.
"""

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from src.utils.logger import logger


@dataclass
class ParsedPost:
    """Parsed Facebook post data"""
    post_id: str
    group_id: str
    group_name: str
    author_name: str
    author_id: Optional[str]
    text: str
    timestamp: datetime
    post_url: str


class FacebookScraper:
    """
    Facebook Groups scraper using Playwright stealth.
    Connects to existing Chrome browser via CDP.
    """
    
    def __init__(self, cdp_url: str = "http://localhost:9222"):
        self.cdp_url = cdp_url
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
    
    async def start(self) -> bool:
        """Connect to existing Chrome via CDP"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.connect_over_cdp(self.cdp_url)
            
            # Get existing context (first one)
            contexts = self.browser.contexts
            if contexts:
                self.context = contexts[0]
                pages = self.context.pages
                if pages:
                    self.page = pages[0]
                else:
                    self.page = await self.context.new_page()
            else:
                self.context = await self.browser.new_context()
                self.page = await self.context.new_page()
            
            # Apply stealth settings
            await self._apply_stealth()
            
            logger.info(f"Facebook scraper connected to Chrome via CDP: {self.cdp_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Chrome: {e}")
            return False
    
    async def _apply_stealth(self):
        """Apply stealth mode to avoid detection"""
        if not self.page:
            return
            
        # Override navigator properties
        await self.page.add_init_script("""
            // Override webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en', 'ru']
            });
            
            // Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Override chrome property
            window.chrome = {
                runtime: {}
            };
        """)
    
    async def stop(self):
        """Disconnect from browser (doesn't close Chrome)"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Facebook scraper disconnected")
    
    async def get_user_groups(self) -> list[dict]:
        """
        Get all groups the user is a member of.
        Returns list of {id, name, url}
        """
        if not self.page:
            return []
        
        groups = []
        
        try:
            # Navigate to groups "Your groups" page (shows all joined groups)
            url = "https://web.facebook.com/groups/joins/?nav_source=tab&ordering=viewer_added"
            logger.info(f"Navigating to Facebook groups page: {url}")
            await self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Wait for group cards to appear (stable wait)
            try:
                await self.page.wait_for_selector('div[role="main"] a[role="link"][href*="/groups/"]', timeout=10000)
            except:
                pass
            
            # Scroll to load ALL groups until end of list
            prev_scroll_height = 0
            no_change_count = 0
            scroll_num = 0
            
            while True:
                scroll_num += 1
                # Get current scroll height (use documentElement for Facebook)
                scroll_height = await self.page.evaluate("document.documentElement.scrollHeight")
                
                # Check if scroll height changed
                if scroll_height == prev_scroll_height:
                    no_change_count += 1
                    if no_change_count >= 5:  # 5 attempts with no change = end of list
                        logger.debug(f"Scroll stopped after {scroll_num} scrolls, no new content")
                        break
                else:
                    no_change_count = 0
                
                prev_scroll_height = scroll_height
                
                # Scroll to bottom
                await self.page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
                
                # Wait for new content to load
                try:
                    await self.page.wait_for_function(
                        f"document.documentElement.scrollHeight > {scroll_height}",
                        timeout=3000
                    )
                except:
                    # If wait fails, still wait a fixed time for content to load
                    await asyncio.sleep(2)
            
            # Parse groups from main content area only
            group_links = await self.page.query_selector_all('div[role="main"] a[role="link"][href*="/groups/"]')
            logger.info(f"Total group links found after scrolling: {len(group_links)}")
            
            seen_ids = set()
            for link in group_links:
                try:
                    href = await link.get_attribute("href")
                    if not href:
                        continue
                    
                    # Skip special pages and navigation links
                    skip_patterns = [
                        "category=create",  # "Create new group" button
                        "/groups/feed",
                        "/groups/discover", 
                        "/groups/search",
                        "/groups/joins",
                        "/groups/notifications",
                    ]
                    if any(pattern in href for pattern in skip_patterns):
                        continue
                    
                    # Extract group ID from URL
                    # Pattern: /groups/{group_id}/
                    match = re.search(r'/groups/(\d+|[^/?]+)/?', href)
                    if not match:
                        continue
                    
                    group_id = match.group(1)
                    
                    # Get group name from link text BEFORE checking seen_ids
                    name = await link.inner_text() or ""
                    name = name.strip()
                    
                    # Skip empty or too short names
                    if len(name) < 3:
                        continue
                    
                    # Skip if name looks like a button text
                    button_texts = ["Создать", "Create", "Посмотреть", "View", "Ещё", "More"]
                    if any(name.startswith(btn) for btn in button_texts):
                        continue
                    
                    # NOW check if already seen (after all validations passed)
                    if group_id in seen_ids:
                        continue
                    seen_ids.add(group_id)
                    
                    # Clean URL - ensure it's full URL
                    if href.startswith("/"):
                        href = f"https://www.facebook.com{href}"
                    
                    groups.append({
                        "id": group_id,
                        "name": name[:100],
                        "url": href.split("?")[0]
                    })
                    
                except Exception:
                    continue
            
            logger.info(f"Found {len(groups)} unique groups")
            return groups
            
        except Exception as e:
            logger.error(f"Error fetching groups: {e}")
            return []
    async def get_group_posts(
        self, 
        group_url: str, 
        limit: int = 20
    ) -> list[ParsedPost]:
        """
        Get posts from a Facebook group.
        
        Args:
            group_url: URL of the group
            limit: Maximum number of posts to fetch
        """
        if not self.page:
            return []
        
        posts = []
        seen_texts = set()  # Dedupe by text hash
        
        try:
            # Extract group info from URL
            match = re.search(r'/groups/(\d+|[^/]+)', group_url)
            group_id = match.group(1) if match else "unknown"
            
            # Navigate to group - sort by new posts
            url_with_sort = f"{group_url}?sorting_setting=CHRONOLOGICAL"
            await self.page.goto(url_with_sort, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)  # Reduced delay
            
            # Get group name from page
            group_name = await self._get_group_name()
            
            # Scroll to load posts
            posts_found = 0
            scroll_attempts = 0
            max_scrolls = 10
            
            while posts_found < limit and scroll_attempts < max_scrolls:
                # Find all post articles
                articles = await self.page.query_selector_all('[role="article"]')
                
                for article in articles:
                    if posts_found >= limit:
                        break
                    
                    try:
                        post = await self._parse_article(article, group_id, group_name)
                        if not post:
                            continue
                        
                        # Dedupe by text hash (within this session)
                        text_hash = hash(post.text[:100])
                        if text_hash in seen_texts:
                            continue
                        seen_texts.add(text_hash)
                        
                        posts.append(post)
                        posts_found += 1
                    except Exception as e:
                        logger.debug(f"Error parsing article: {e}")
                        continue
                
                # Scroll for more
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1.5)  # Reduced delay
                scroll_attempts += 1
            
            logger.info(f"Scraped {len(posts)} new posts from {group_name}")
            return posts
            
        except Exception as e:
            logger.error(f"Error scraping group {group_url}: {e}")
            return []
    
    async def get_groups_posts_parallel(
        self,
        groups: list[dict],
        limit_per_group: int = 20,
        max_workers: int = 3,
        processed_ids: set = None
    ) -> list[ParsedPost]:
        """
        Scrape multiple groups in parallel using multiple browser pages.
        Stops scanning a group when hitting an already-processed post.
        
        Args:
            groups: List of group dicts with 'id', 'name', 'url'
            limit_per_group: Max posts per group
            max_workers: Number of parallel pages (tabs), default 3
            processed_ids: Set of already-processed post IDs for early stop
        
        Returns:
            List of all NEW posts from all groups
        """
        import time
        
        if not self.context:
            logger.error("No browser context available")
            return []
        
        if processed_ids is None:
            processed_ids = set()
        
        all_posts = []
        
        # Create worker pages
        pages = []
        try:
            num_workers = min(max_workers, len(groups))
            for _ in range(num_workers):
                page = await self.context.new_page()
                await self._apply_stealth_to_page(page)
                pages.append(page)
            
            logger.info(f"Created {len(pages)} parallel pages for scraping")
            
            # Process groups in batches
            async def scrape_group(page: Page, group: dict) -> list[ParsedPost]:
                """Scrape a single group, stop when hitting processed post"""
                group_start = time.time()
                group_name = group["name"]
                
                try:
                    group_url = group["url"]
                    group_id = group["id"]
                    
                    # Navigate to group - chronological order (newest first)
                    nav_start = time.time()
                    url_with_sort = f"{group_url}?sorting_setting=CHRONOLOGICAL"
                    await page.goto(url_with_sort, wait_until="domcontentloaded", timeout=15000)
                    logger.debug(f"[{group_name}] Navigation: {time.time() - nav_start:.1f}s")
                    
                    # Wait for posts to appear (reduced timeout)
                    wait_start = time.time()
                    try:
                        await page.wait_for_selector('[role="article"]', timeout=5000)
                    except:
                        pass
                    logger.debug(f"[{group_name}] Wait for articles: {time.time() - wait_start:.1f}s")
                    
                    # Get actual group name from page (quick, no timeout issue)
                    try:
                        h1 = await page.query_selector("h1")
                        if h1:
                            name = await h1.inner_text()
                            if name:
                                group_name = name.strip()
                    except:
                        pass
                    
                    posts = []
                    seen_texts = set()
                    posts_found = 0
                    scroll_attempts = 0
                    max_scrolls = 30
                    consecutive_empty = 0
                    hit_processed = False
                    parsed_count = 0
                    
                    # Initial wait for articles to load
                    await asyncio.sleep(1)
                    
                    while posts_found < limit_per_group and scroll_attempts < max_scrolls and not hit_processed:
                        # Get all articles currently on page
                        articles = await page.query_selector_all('[role="article"]')
                        new_articles = articles[parsed_count:]
                        
                        # Parse only NEW articles
                        for article in new_articles:
                            if posts_found >= limit_per_group or hit_processed:
                                break
                            
                            try:
                                post = await self._parse_article_fast(page, article, group_id, group_name)
                                if not post:
                                    continue
                                
                                # Check if already processed - early stop
                                if post.post_id in processed_ids:
                                    logger.info(f"[{group_name}] Hit processed post, stopping")
                                    hit_processed = True
                                    break
                                
                                # Dedupe by text hash
                                text_hash = hash(post.text[:100])
                                if text_hash in seen_texts:
                                    continue
                                seen_texts.add(text_hash)
                                
                                posts.append(post)
                                posts_found += 1
                            except:
                                continue
                        
                        parsed_count = len(articles)
                        
                        # Stop conditions
                        if hit_processed or posts_found >= limit_per_group:
                            break
                        
                        # Scroll to load more
                        prev_count = len(articles)
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        
                        # Wait for new articles to appear (up to 10s)
                        try:
                            await page.wait_for_function(
                                f"document.querySelectorAll('[role=\"article\"]').length > {prev_count}",
                                timeout=10000
                            )
                            consecutive_empty = 0  # Reset on success
                        except:
                            # No new articles loaded
                            consecutive_empty += 1
                            if consecutive_empty >= 5:  # 5 attempts = 50s of waiting
                                logger.debug(f"[{group_name}] No more content after {scroll_attempts} scrolls")
                                break
                        
                        scroll_attempts += 1
                    
                    total_time = time.time() - group_start
                    if hit_processed:
                        reason = "hit processed"
                    elif posts_found >= limit_per_group:
                        reason = "limit reached"
                    else:
                        reason = "no more content"
                    logger.info(f"[{group_name}] {len(posts)} posts in {total_time:.1f}s ({reason})")
                    return posts
                    
                except Exception as e:
                    total_time = time.time() - group_start
                    logger.error(f"[{group_name}] Error after {total_time:.1f}s: {e}")
                    return []
            
            # Process in batches using workers
            for batch_start in range(0, len(groups), len(pages)):
                batch = groups[batch_start:batch_start + len(pages)]
                batch_names = [g["name"][:20] for g in batch]
                logger.info(f"Batch {batch_start//len(pages)+1}: {batch_names}")
                
                # Assign each group to a page with timeout
                tasks = []
                for i, group in enumerate(batch):
                    page = pages[i % len(pages)]
                    # Wrap each group scrape with 45s timeout (reduced from 60)
                    task = asyncio.wait_for(scrape_group(page, group), timeout=45.0)
                    tasks.append(task)
                
                # Run batch in parallel
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for idx, result in enumerate(batch_results):
                    if isinstance(result, list):
                        all_posts.extend(result)
                    elif isinstance(result, asyncio.TimeoutError):
                        group_name = batch[idx].get('name', 'unknown') if idx < len(batch) else 'unknown'
                        logger.warning(f"TIMEOUT: {group_name} (45s)")
                    elif isinstance(result, Exception):
                        logger.error(f"Parallel scrape error: {result}")
            
        finally:
            # Close worker pages
            for page in pages:
                try:
                    await page.close()
                except:
                    pass
        
        logger.info(f"Parallel scraping complete: {len(all_posts)} total posts from {len(groups)} groups")
        return all_posts
    
    async def _parse_article_fast(self, page: Page, article, group_id: str, group_name: str) -> Optional[ParsedPost]:
        """Article parsing with 'See more' click but strict timeout to prevent hangs"""
        # Check if this is a valid post (has post link)
        post_url = ""
        post_id = ""
        try:
            post_link = await article.query_selector('a[href*="/posts/"], a[href*="/permalink/"]')
            if post_link:
                href = await post_link.get_attribute("href")
                if href:
                    if "comment_id=" in href or "reply_comment_id=" in href:
                        return None
                    post_url = href if href.startswith("http") else f"https://www.facebook.com{href}"
                    match = re.search(r'/posts/(\d+)', href) or re.search(r'/permalink/(\d+)', href)
                    if match:
                        post_id = match.group(1)
        except:
            pass
        
        if not post_url:
            return None
        
        # Get author (quick)
        author_name = "Unknown"
        author_id = None
        try:
            for selector in ['h2 a[role="link"]', 'h3 a[role="link"]']:
                author_link = await article.query_selector(selector)
                if author_link:
                    name = await author_link.inner_text()
                    if name and len(name.strip()) > 1:
                        author_name = name.strip()
                        break
        except:
            pass
        
        # Click "See more" with STRICT timeout to get full text
        try:
            see_more = await article.query_selector('div[role="button"]:has-text("See more"), div[role="button"]:has-text("Ещё")')
            if see_more:
                # Use strict 1s timeout - if it hangs, skip this click
                try:
                    await asyncio.wait_for(see_more.click(), timeout=3.0)
                    await asyncio.sleep(0.3)  # Wait for text to expand
                except asyncio.TimeoutError:
                    pass  # Click timed out, continue with truncated text
        except:
            pass
        
        # Get text (after expanding if successful)
        text = ""
        try:
            text_divs = await article.query_selector_all('div[dir="auto"]')
            for div in text_divs:
                t = await div.inner_text()
                if t and len(t) > 30 and t.strip() != author_name:
                    text = t.strip()
                    break
        except:
            pass
        
        if not text or len(text) < 10:
            return None
        
        if not post_id:
            post_id = str(abs(hash(text[:100])))
        
        return ParsedPost(
            post_id=post_id,
            group_id=group_id,
            group_name=group_name,
            author_name=author_name[:100],
            author_id=author_id,
            text=text[:2000],
            timestamp=datetime.now(),
            post_url=post_url
        )
    
    async def _apply_stealth_to_page(self, page: Page):
        """Apply stealth mode to a specific page"""
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en', 'ru'] });
            window.chrome = { runtime: {} };
        """)
    
    async def _parse_article_on_page(self, page: Page, article, group_id: str, group_name: str) -> Optional[ParsedPost]:
        """Parse article using specific page context"""
        # Reuse existing _parse_article logic but explicitly pass page context
        # First check if this is a valid post (has post link)
        post_url = ""
        post_id = ""
        try:
            post_link = await article.query_selector('a[href*="/posts/"], a[href*="/permalink/"]')
            if post_link:
                href = await post_link.get_attribute("href")
                if href:
                    # Ignore comments and replies
                    if "comment_id=" in href or "reply_comment_id=" in href:
                         logger.debug(f"Ignoring comment post: {href}")
                         return None
                         
                    post_url = href if href.startswith("http") else f"https://www.facebook.com{href}"
                    match = re.search(r'/posts/(\d+)', href) or re.search(r'/permalink/(\d+)', href)
                    if match:
                        post_id = match.group(1)
        except:
            pass
        
        if not post_url:
            return None
        
        # Get author
        author_name = "Unknown"
        author_id = None
        try:
            author_selectors = ['h2 a[role="link"]', 'h3 a[role="link"]', 'a[role="link"] strong']
            for selector in author_selectors:
                author_link = await article.query_selector(selector)
                if author_link:
                    name = await author_link.inner_text()
                    if name and len(name.strip()) > 1:
                        author_name = name.strip()
                        break
        except:
            pass
        
        # Click "See more" / "Ещё" to expand text (with timeout to prevent hang)
        try:
            see_more_selectors = [
                'div[role="button"]:has-text("See more")',
                'div[role="button"]:has-text("Ещё")',
                'div[role="button"]:has-text("Показать ещё")',
            ]
            for selector in see_more_selectors:
                see_more = await article.query_selector(selector)
                if see_more:
                    # Use timeout to prevent click from hanging
                    try:
                        await asyncio.wait_for(see_more.click(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pass  # Click timed out, continue anyway
                    break
        except:
            pass
        
        # Get text (after expanding)
        text = ""
        try:
            text_divs = await article.query_selector_all('div[dir="auto"]')
            for div in text_divs:
                t = await div.inner_text()
                if t and len(t) > 30 and t.strip() != author_name:
                    text = t.strip()
                    break
        except:
            pass
        
        if not text or len(text) < 10:
            return None
        
        if not post_id:
            post_id = str(abs(hash(text[:100])))
        
        return ParsedPost(
            post_id=post_id,
            group_id=group_id,
            group_name=group_name,
            author_name=author_name[:100],
            author_id=author_id,
            text=text[:2000],
            timestamp=datetime.now(),
            post_url=post_url
        )

    async def _get_group_name(self) -> str:
        """Extract group name from current page"""
        try:
            # Try h1 first
            h1 = await self.page.query_selector("h1")
            if h1:
                name = await h1.inner_text()
                if name:
                    return name.strip()
            
            # Fallback to title
            title = await self.page.title()
            return title.split("|")[0].strip() if title else "Unknown Group"
        except:
            return "Unknown Group"
    
    async def _parse_article(self, article, group_id: str, group_name: str) -> Optional[ParsedPost]:
        """Parse a single post article element"""
        
        # First check if this is a valid post (has post link)
        post_url = ""
        post_id = ""
        try:
            # Links to posts contain /posts/ or /permalink/
            post_link = await article.query_selector('a[href*="/posts/"], a[href*="/permalink/"]')
            if post_link:
                href = await post_link.get_attribute("href")
                if href:
                    post_url = href if href.startswith("http") else f"https://www.facebook.com{href}"
                    # Extract post ID
                    match = re.search(r'/posts/(\d+)', href) or re.search(r'/permalink/(\d+)', href)
                    if match:
                        post_id = match.group(1)
        except:
            pass
        
        # Skip articles that don't have a post link (sidebars, ads, etc.)
        if not post_url:
            return None
        
        # Try to click "See more" if present
        try:
            see_more = await article.query_selector('[role="button"]:has-text("See more"), [role="button"]:has-text("Ещё"), [role="button"]:has-text("Показать ещё")')
            if see_more:
                await see_more.click()
                await asyncio.sleep(0.3)
        except:
            pass
        
        # Get author info - look for bold text in header or profile links
        author_name = "Unknown"
        author_id = None
        try:
            # Try multiple selectors for author
            author_selectors = [
                'h2 a[role="link"]',  # Header link
                'h3 a[role="link"]',  # Alternative header
                'a[role="link"] strong',  # Bold link text
                'span[dir="auto"] > a[role="link"]',  # Span-wrapped link
            ]
            for selector in author_selectors:
                author_link = await article.query_selector(selector)
                if author_link:
                    name = await author_link.inner_text()
                    if name and len(name.strip()) > 1:
                        author_name = name.strip()
                        break
            
            # Try to get author ID from profile link
            profile_link = await article.query_selector('a[href*="/user/"], a[href*="/profile.php"]')
            if profile_link:
                href = await profile_link.get_attribute("href")
                if href:
                    match = re.search(r'/user/(\d+)', href)
                    if match:
                        author_id = match.group(1)
        except:
            pass
        
        # Get text content - filter out short metadata
        text = ""
        try:
            text_divs = await article.query_selector_all('div[dir="auto"]')
            texts = []
            for div in text_divs:
                t = await div.inner_text()
                # Skip short text (buttons, timestamps, etc.)
                if t and len(t) > 30:
                    # Skip if it's just the author name
                    if t.strip() == author_name:
                        continue
                    texts.append(t.strip())
            
            # Take first substantive text block
            if texts:
                text = texts[0]
        except:
            pass
        
        if not text or len(text) < 10:
            return None
        
        if not post_id:
            # Generate ID from text hash
            post_id = str(abs(hash(text[:100])))
        
        # Timestamp - use current time (parsing FB dates is unreliable)
        timestamp = datetime.now()
        
        return ParsedPost(
            post_id=post_id,
            group_id=group_id,
            group_name=group_name,
            author_name=author_name[:100],
            author_id=author_id,
            text=text[:2000],
            timestamp=timestamp,
            post_url=post_url
        )


async def test_scraper():
    """Test the scraper (run with Chrome in debug mode)"""
    scraper = FacebookScraper()
    
    if not await scraper.start():
        print("Failed to connect. Start Chrome with: google-chrome --remote-debugging-port=9222")
        return
    
    try:
        # Get groups
        groups = await scraper.get_user_groups()
        print(f"Found {len(groups)} groups:")
        for g in groups[:5]:
            print(f"  - {g['name']}: {g['url']}")
        
        # Get posts from first group
        if groups:
            posts = await scraper.get_group_posts(groups[0]["url"], limit=5)
            print(f"\nPosts from {groups[0]['name']}:")
            for p in posts:
                print(f"  - {p.author_name}: {p.text[:100]}...")
    finally:
        await scraper.stop()


if __name__ == "__main__":
    asyncio.run(test_scraper())
