import asyncio
import json

import httpx

from src.config import config
from src.processing.prompts import BATCH_PROMPT
from src.utils.logger import logger, log_prompt, log_text_preview, log_lead_found, log_analysis_result

# Initialize Gemini if needed
if config.llm_provider == "gemini":
    import google.generativeai as genai
    genai.configure(api_key=config.gemini_api_key)



class AnalysisError(Exception):
    """Raised when batch analysis fails"""
    pass


async def call_gemini(prompt: str) -> str:
    """Call Gemini API"""
    import google.generativeai as genai
    model = genai.GenerativeModel(config.gemini_model)
    response = await model.generate_content_async(
        prompt,
        generation_config={"temperature": 0.1},
    )
    return response.text.strip()


async def call_openrouter(prompt: str) -> str:
    """Call OpenRouter API"""
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {config.openrouter_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.openrouter_model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
            },
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"].strip()


async def call_llm(prompt: str) -> str:
    """Call configured LLM provider"""
    if config.llm_provider == "openrouter":
        return await call_openrouter(prompt)
    else:
        return await call_gemini(prompt)


def parse_response(result_text: str) -> list[dict]:
    """Parse JSON response, handling various formats"""
    text = result_text.strip()
    
    # Empty response = no leads
    if not text:
        return []
    
    # Remove markdown code blocks
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("["):
                text = part
                break
    
    # Find JSON array in text
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end > start:
        text = text[start:end + 1]
    
    # Parse JSON, return empty on failure
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return []


async def analyze_batch(messages: list[tuple[int, str]], max_retries: int = 3) -> dict[int, tuple[bool, str, float, str]]:
    """
    Analyze batch of messages with LLM API call.
    Returns only leads with (is_lead, reason, confidence, lead_type).
    Raises AnalysisError on failure.
    """
    if not messages:
        return {}

    # Format messages for prompt - support both (idx, text) and (idx, user_id, text) formats
    formatted_lines = []
    for item in messages:
        if len(item) == 3:
            idx, user_id, text = item
            formatted_lines.append(f"[{idx}] (user:{user_id}) {text}")
        else:
            idx, text = item
            formatted_lines.append(f"[{idx}] {text}")
    
    formatted = "\n\n".join(formatted_lines)
    prompt = BATCH_PROMPT.format(messages=formatted)
    
    # Log texts being analyzed
    for item in messages:
        idx = item[0]
        text = item[-1]  # Last element is always text
        log_text_preview(idx, text)

    for attempt in range(max_retries):
        try:
            # Log prompt on first attempt
            if attempt == 0:
                log_prompt(prompt)
            
            result_text = await call_llm(prompt)
            results = parse_response(result_text)
            
            # Log found leads with color
            for r in results:
                lead_type = r.get('type', 'property')
                log_lead_found(lead_type, r.get('reason', ''), r.get('confidence', 0.5))
            
            # All returned results are leads (is_lead=True)
            return {
                r["id"]: (True, r.get("reason", ""), r.get("confidence", 0.5), r.get("type", "property"))
                for r in results
            }

        except Exception as e:
            error_str = str(e)
            
            # Check for rate limit error
            if "429" in error_str or "quota" in error_str.lower() or "rate" in error_str.lower():
                wait_time = 60
                if "retry in" in error_str.lower():
                    try:
                        import re
                        match = re.search(r'retry in (\d+)', error_str.lower())
                        if match:
                            wait_time = int(match.group(1)) + 5
                    except:
                        pass
                
                if attempt < max_retries - 1:
                    logger.warning(f"Rate limit hit, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
            
            logger.error(f"LLM batch analysis error ({config.llm_provider}): {e}")
            raise AnalysisError(f"Failed to analyze batch: {e}")
    
    raise AnalysisError("Max retries exceeded")


async def analyze_messages_batch(
    texts: list[tuple[int, str]], 
    batch_size: int = 100,
    max_parallel: int = 3
) -> tuple[dict[int, tuple[bool, str, float]], bool]:
    """
    Analyze messages in batches (parallel processing).
    
    Args:
        texts: List of (id, text) tuples
        batch_size: Messages per batch
        max_parallel: Number of parallel LLM calls
    
    Returns:
        Tuple of (results dict, success bool)
    """
    all_results = {}
    
    # Split into batches
    batches = []
    for i in range(0, len(texts), batch_size):
        batches.append(texts[i:i + batch_size])
    
    total_batches = len(batches)
    logger.info(f"Using LLM provider: {config.llm_provider}, {total_batches} batches, {max_parallel} parallel")
    
    # Process batches in parallel groups
    for group_start in range(0, total_batches, max_parallel):
        group = batches[group_start:group_start + max_parallel]
        batch_nums = list(range(group_start + 1, group_start + len(group) + 1))
        
        logger.info(f"Analyzing batches {batch_nums[0]}-{batch_nums[-1]}/{total_batches} in parallel")
        
        # Run this group in parallel
        tasks = [analyze_batch(batch) for batch in group]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for idx, result in enumerate(results):
            batch_num = batch_nums[idx]
            if isinstance(result, dict):
                all_results.update(result)
            elif isinstance(result, Exception):
                logger.error(f"Batch {batch_num} failed: {result}")
                # Continue with other batches instead of stopping
    
    log_analysis_result(len(texts), len(all_results))
    return all_results, True
