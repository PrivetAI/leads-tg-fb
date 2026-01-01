import logging
import sys


# ANSI color codes
class Colors:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    GRAY = "\033[90m"
    BOLD = "\033[1m"


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels"""
    
    LEVEL_COLORS = {
        logging.DEBUG: Colors.GRAY,
        logging.INFO: Colors.WHITE,
        logging.WARNING: Colors.YELLOW,
        logging.ERROR: Colors.RED,
        logging.CRITICAL: Colors.RED + Colors.BOLD,
    }
    
    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, Colors.WHITE)
        record.levelname = f"{color}{record.levelname:8}{Colors.RESET}"
        return super().format(record)


# Setup colored logging
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColoredFormatter(
    fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
)

logger = logging.getLogger("leads-tg")


# Helper functions for colored debug output
def log_new_post(text_preview: str, source: str = ""):
    """Log new post in GREEN"""
    src = f"[{source}] " if source else ""
    print(f"{Colors.GREEN}✓ NEW POST {src}{Colors.RESET}{text_preview[:80]}...")


def log_old_post(post_id: str, source: str = ""):
    """Log skipped old post in GRAY"""
    src = f"[{source}] " if source else ""
    print(f"{Colors.GRAY}○ OLD POST {src}id={post_id} (already processed){Colors.RESET}")


def log_text_preview(idx: int, text: str, max_len: int = 100):
    """Log text being analyzed in CYAN"""
    preview = text[:max_len].replace('\n', ' ')
    print(f"{Colors.CYAN}[{idx}] {preview}{'...' if len(text) > max_len else ''}{Colors.RESET}")


def log_prompt(prompt: str, label: str = "LLM PROMPT"):
    """Log full prompt in YELLOW (truncated)"""
    print(f"{Colors.YELLOW}━━━ {label} ━━━{Colors.RESET}")
    # Show first 500 and last 200 chars for long prompts
    if len(prompt) > 800:
        print(f"{Colors.YELLOW}{prompt[:500]}{Colors.RESET}")
        print(f"{Colors.GRAY}... [{len(prompt) - 700} chars hidden] ...{Colors.RESET}")
        print(f"{Colors.YELLOW}{prompt[-200:]}{Colors.RESET}")
    else:
        print(f"{Colors.YELLOW}{prompt}{Colors.RESET}")
    print(f"{Colors.YELLOW}━━━ END PROMPT ━━━{Colors.RESET}")


def log_lead_found(lead_type: str, reason: str, confidence: float):
    """Log lead found in MAGENTA"""
    emoji = "🏠" if lead_type == "property" else "🚗"
    print(f"{Colors.MAGENTA}{Colors.BOLD}{emoji} LEAD FOUND: {reason} ({confidence:.0%}){Colors.RESET}")


def log_analysis_result(total: int, leads: int):
    """Log analysis summary in BLUE"""
    print(f"{Colors.BLUE}{Colors.BOLD}📊 Analysis: {total} texts → {leads} leads{Colors.RESET}")
