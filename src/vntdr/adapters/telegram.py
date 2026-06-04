from __future__ import annotations

import httpx
import logging

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id

    def notify(self, message: str) -> None:
        logger.info(f"Sending Telegram notification (HTML): {message}")
        try:
            # 1. Try sending as HTML
            response = httpx.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id": self.chat_id, 
                    "text": message, 
                    "parse_mode": "HTML"
                },
                timeout=20.0,
            )
            response.raise_for_status()
            logger.debug("Telegram notification sent successfully as HTML")
        except httpx.HTTPStatusError as e:
            # 2. If HTML fails, fallback to plain text
            logger.warning(f"Failed to send HTML message: {e.response.text}. Retrying with plain text fallback.")
            error_detail = ""
            try:
                err_json = e.response.json()
                error_detail = f"\n\n(Telegram parse error: {err_json.get('description', 'Unknown error')})"
            except:
                pass
            
            # Clean text for fallback: strip HTML tags
            import re
            clean_text = re.sub(r'<[^>]+>', '', message)
            
            fallback_text = (
                f"{clean_text}\n\n"
                f"⚠️ 该消息 HTML 格式 Telegram 解析失败，已转为纯文本。请检查特殊字符并重试。"
                f"{error_detail}"
            )
            
            try:
                response = httpx.post(
                    f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                    json={
                        "chat_id": self.chat_id, 
                        "text": fallback_text
                    },
                    timeout=20.0,
                )
                response.raise_for_status()
                logger.info("Telegram notification sent successfully using fallback text")
            except Exception as final_e:
                logger.error(f"Final fallback also failed: {final_e}")
                raise final_e
        except Exception as e:
            logger.error(f"Telegram notification failed (Non-HTTP error): {e}")
            raise e
