import argparse
import asyncio
import logging
import random
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

THREAD_REGEX = re.compile(
    r"^https?://(?:2ch\.[a-z]{2,5}/[a-z]+/res/\d+\.html|arhivach\.[a-z]{2,5}/thread/\d+/?)$"
)
THREAD_ID_REGEX = re.compile(r"/(\d+)(?:\.html)?(?:#.*)?/?$")

MEDIA_EXTENSIONS = {
    "img": {".jpg", ".jpeg", ".png", ".gif", ".webp"},
    "vid": {".mp4", ".webm", ".mkv", ".avi", ".mov"},
}
MEDIA_EXTENSIONS["both"] = MEDIA_EXTENSIONS["img"] | MEDIA_EXTENSIONS["vid"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def get_semaphore_for_url(url: str, semaphores: dict) -> asyncio.Semaphore:
    """Выбирает подходящий лимит потоков в зависимости от домена."""
    if "arhivach" in url:
        return semaphores["arhivach"]
    return semaphores["default"]


async def download_file(client, url, folder, semaphores):
    filename = url.split("/")[-1]
    filepath = folder / filename

    if filepath.exists():
        return

    semaphore = get_semaphore_for_url(url, semaphores)

    async with semaphore:
        retries = 5
        base_delay = 2

        for attempt in range(1, retries + 1):
            try:
                response = await client.get(url)
                response.raise_for_status()
                filepath.write_bytes(response.content)
                log.info(f"Сохранено: {filename}")
                return
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (429, 502, 503, 504):
                    if attempt < retries:
                        sleep_time = base_delay * attempt + random.uniform(0, 1)
                        log.warning(
                            f"Ошибка {e.response.status_code} для {filename}. Повтор {attempt}/{retries} через {sleep_time:.1f}с..."
                        )
                        await asyncio.sleep(sleep_time)
                        continue

                log.error(f"Не удалось скачать {filename}: {e}")
                break
            except (httpx.RequestError, httpx.TimeoutException) as e:
                log.error(f"Ошибка сети для {filename}: {e}")
                break
            except Exception as e:
                log.error(f"Критическая ошибка {filename}: {e}")
                break


async def process_thread(client, thread_url, base_folder, allowed_exts, semaphores):
    log.info(f"Анализ треда: {thread_url}")
    try:
        for attempt in range(3):
            try:
                response = await client.get(thread_url)
                response.raise_for_status()
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (503, 502) and attempt < 2:
                    await asyncio.sleep(2)
                    continue
                raise e
    except Exception as e:
        log.error(f"Не удалось загрузить страницу треда {thread_url}: {e}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    media_links = set()

    for tag in soup.find_all("a", href=True):
        href = tag["href"]

        full_url = urljoin(thread_url, href)

        parsed = urlparse(full_url)
        path = parsed.path.lower()

        if any(path.endswith(ext) for ext in allowed_exts):
            media_links.add(full_url)

    if not media_links:
        log.info(f"Медиафайлы не найдены в треде: {thread_url}")
        return

    thread_id_match = THREAD_ID_REGEX.search(thread_url)
    thread_id = thread_id_match.group(1) if thread_id_match else "unknown_thread"

    domain_prefix = "arhivach" if "arhivach" in thread_url else "2ch"
    save_dir = base_folder / f"{domain_prefix}_{thread_id}"
    save_dir.mkdir(parents=True, exist_ok=True)

    log.info(
        f"Тред {thread_id} ({domain_prefix}): Найдено {len(media_links)} файлов. Старт загрузки в '{save_dir.name}'"
    )

    tasks = [download_file(client, link, save_dir, semaphores) for link in media_links]
    await asyncio.gather(*tasks)


async def main():
    parser = argparse.ArgumentParser(
        description="Загрузчик медиафайлов с 2ch и arhivach"
    )
    parser.add_argument("path", type=Path, help="Папка для сохранения")
    parser.add_argument("type", choices=["img", "vid", "both"], help="Тип файлов")
    parser.add_argument("threads", nargs="+", help="Ссылки на треды")

    args = parser.parse_args()

    if not args.path.exists():
        try:
            args.path.mkdir(parents=True, exist_ok=True)
            log.info(f"Создана папка: {args.path}")
        except Exception as e:
            log.error(f"Не удалось создать папку: {e}")
            sys.exit(1)

    valid_threads = [url for url in args.threads if THREAD_REGEX.match(url)]

    if not valid_threads:
        log.error("Нет валидных ссылок. Проверьте формат.")
        sys.exit(1)

    semaphores = {
        "default": asyncio.Semaphore(10),
        "arhivach": asyncio.Semaphore(7),
    }

    target_extensions = MEDIA_EXTENSIONS[args.type]

    async with httpx.AsyncClient(headers=HEADERS, verify=False, timeout=60) as client:
        tasks = [
            process_thread(client, url, args.path, target_extensions, semaphores)
            for url in valid_threads
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n")
        log.info("Остановка...")
        sys.exit(0)
    except Exception as e:
        log.critical(f"Критическая ошибка: {e}")
        sys.exit(1)
