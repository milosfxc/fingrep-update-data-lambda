import config
from config import telegram_logger
import requests
import os


def post_trade_alert(image_path: str | None, message: str, link: str, channel_id: str):
    caption = f"{message}\n\n{link}"
    url = f"https://api.telegram.org/bot{config.FINGREP_BOT_TOKEN}/"
    if image_path:
        if not os.path.exists(image_path):
            telegram_logger.error("Image file not found: %s", image_path)
            return
        with open(image_path, "rb") as photo:
            response = requests.post(
                url + "sendPhoto",
                data={
                    "chat_id": channel_id,
                    "caption": caption,
                    "parse_mode": "HTML"
                },
                files={
                    "photo": photo
                }
            )
    else:
        response = requests.post(
            url + "sendMessage",
            data={
                "chat_id": channel_id,
                "text": caption,
                "parse_mode": "HTML",
                "disable_web_page_preview": False
            }
        )
    if response.status_code != 200:
        telegram_logger.error(
            "Telegram API request failed | status=%d | body=%s",
            response.status_code, response.text
        )
        return
