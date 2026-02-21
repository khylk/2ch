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

# ─── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ─── Constants ─────────────────────────────────────────────────────────────────

THREAD_REGEX = re.compile(
    r"^https?://(?:2ch\.[a-z]{2,5}/[a-z]+/res/\d+\.html|arhivach\.[a-z]{2,5}/thread/\d+/?)$"
)
THREAD_ID_REGEX = re.compile(r"/(\d+)(?:\.html)?(?:#.*)?/?$")

MEDIA_EXTENSIONS: dict[str, frozenset[str]] = {
    "img": frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp"}),
    "vid": frozenset({".mp4", ".webm", ".mkv", ".avi", ".mov"}),
}
MEDIA_EXTENSIONS["both"] = MEDIA_EXTENSIONS["img"] | MEDIA_EXTENSIONS["vid"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Статусы, при которых имеет смысл повторять запрос
RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})

# Ограничение параллельных загрузок по домену
SEMAPHORE_LIMITS: dict[str, int] = {
    "arhivach": 3,  # нестабильный хост
    "default": 8,
}

# Параметры retry
MAX_RETRIES = 7
BASE_DELAY_SEC = 1.5
MAX_DELAY_SEC = 60.0

# Минимальный размер чанка стриминга (256 KB)
CHUNK_SIZE = 256 * 1024


# ─── Utilities ─────────────────────────────────────────────────────────────────


def domain_key(url: str) -> str:
    # Возвращаем ключ домена для выбора семафора
    return "arhivach" if "arhivach" in url else "default"


def full_jitter(attempt: int) -> float:
    cap = MAX_DELAY_SEC
    ceiling = min(cap, BASE_DELAY_SEC * (2**attempt))
    return random.uniform(0, ceiling)


async def fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    extra_headers: dict | None = None,
    max_retries: int = MAX_RETRIES,
) -> httpx.Response:
    headers = extra_headers or {}
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as exc:
            if (
                exc.response.status_code in RETRYABLE_STATUSES
                and attempt < max_retries - 1
            ):
                delay = full_jitter(attempt)
                log.warning(
                    "HTTP %d для %s. Попытка %d/%d, ждём %.1f с...",
                    exc.response.status_code,
                    url,
                    attempt + 1,
                    max_retries,
                    delay,
                )
                await asyncio.sleep(delay)
                last_exc = exc
                continue
            raise
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            if attempt < max_retries - 1:
                delay = full_jitter(attempt)
                log.warning(
                    "Сетевая ошибка для %s: %s. Попытка %d/%d, ждём %.1f с...",
                    url,
                    exc,
                    attempt + 1,
                    max_retries,
                    delay,
                )
                await asyncio.sleep(delay)
                last_exc = exc
                continue
            raise

    # Сюда попадаем только если вышли из цикла без return/raise
    raise last_exc or RuntimeError(f"Не удалось выполнить запрос: {url}")


# ─── Download ──────────────────────────────────────────────────────────────────


async def download_file(
    client: httpx.AsyncClient,
    url: str,
    folder: Path,
    semaphores: dict[str, asyncio.Semaphore],
    progress: dict,
) -> None:
    filename = Path(urlparse(url).path).name
    filepath = folder / filename
    tmp_path = filepath.with_suffix(filepath.suffix + ".tmp")

    if filepath.exists():
        progress["skip"] += 1
        return

    sem = semaphores[domain_key(url)]

    async with sem:
        # Определяем, произошла ли частичная подгрузка файла
        resume_from = tmp_path.stat().st_size if tmp_path.exists() else 0
        extra_headers = {"Range": f"bytes={resume_from}-"} if resume_from else {}

        try:
            response = await fetch_with_retry(client, url, extra_headers=extra_headers)
        except httpx.HTTPStatusError as exc:
            log.error(
                "Не удалось скачать %s: HTTP %d", filename, exc.response.status_code
            )
            progress["fail"] += 1
            return
        except Exception as exc:
            log.error("Не удалось скачать %s: %s", filename, exc)
            progress["fail"] += 1
            return

        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type:
            log.warning(
                "Пропуск %s — получен HTML вместо медиафайла (возможно, редирект)",
                filename,
            )
            progress["fail"] += 1
            return

        # Потоковая запись
        mode = "ab" if resume_from and response.status_code == 206 else "wb"
        try:
            with tmp_path.open(mode) as fh:
                async for chunk in response.aiter_bytes(CHUNK_SIZE):
                    fh.write(chunk)

            # Атомарный rename: файл либо есть целиком, либо нет
            tmp_path.rename(filepath)
            log.info("✓ %s", filename)
            progress["ok"] += 1

        except Exception as exc:
            log.error("Ошибка записи %s: %s", filename, exc)
            # tmp оставляем — можно будет дозакачать при следующем запуске
            progress["fail"] += 1


# ─── Thread processor ──────────────────────────────────────────────────────────


async def process_thread(
    client: httpx.AsyncClient,
    thread_url: str,
    base_folder: Path,
    allowed_exts: frozenset[str],
    semaphores: dict[str, asyncio.Semaphore],
) -> None:
    log.info("Анализ треда: %s", thread_url)

    try:
        response = await fetch_with_retry(client, thread_url, max_retries=4)
    except Exception as exc:
        log.error("Не удалось загрузить тред %s: %s", thread_url, exc)
        return

    soup = BeautifulSoup(response.text, "html.parser")

    media_links: set[str] = set()
    for tag in soup.find_all("a", href=True):
        href: str = tag["href"]
        full_url = urljoin(thread_url, href)
        if any(urlparse(full_url).path.lower().endswith(ext) for ext in allowed_exts):
            media_links.add(full_url)

    if not media_links:
        log.info("Медиафайлы не найдены: %s", thread_url)
        return

    match = THREAD_ID_REGEX.search(thread_url)
    thread_id = match.group(1) if match else "unknown"
    domain_prefix = "arhivach" if "arhivach" in thread_url else "2ch"
    save_dir = base_folder / f"{domain_prefix}_{thread_id}"
    save_dir.mkdir(parents=True, exist_ok=True)

    log.info(
        "Тред %s (%s): %d файлов → '%s'",
        thread_id,
        domain_prefix,
        len(media_links),
        save_dir.name,
    )

    progress: dict[str, int] = {"ok": 0, "fail": 0, "skip": 0}

    tasks = [
        download_file(client, link, save_dir, semaphores, progress)
        for link in media_links
    ]
    await asyncio.gather(*tasks, return_exceptions=False)

    log.info(
        "Тред %s завершён: ✓ %d  ✗ %d  пропущено %d",
        thread_id,
        progress["ok"],
        progress["fail"],
        progress["skip"],
    )


# ─── Entry point ───────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Загрузчик медиафайлов с 2ch и arhivach"
    )
    parser.add_argument("path", type=Path, help="Папка для сохранения")
    parser.add_argument("type", choices=["img", "vid", "both"], help="Тип файлов")
    parser.add_argument("threads", nargs="+", help="Ссылки на треды")
    args = parser.parse_args()

    try:
        args.path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log.error("Не удалось создать папку %s: %s", args.path, exc)
        sys.exit(1)

    valid_threads = [u for u in args.threads if THREAD_REGEX.match(u)]
    invalid = set(args.threads) - set(valid_threads)
    if invalid:
        log.warning("Игнорируем невалидные ссылки: %s", ", ".join(invalid))
    if not valid_threads:
        log.error("Нет валидных ссылок. Проверьте формат.")
        sys.exit(1)

    semaphores = {
        key: asyncio.Semaphore(limit) for key, limit in SEMAPHORE_LIMITS.items()
    }
    target_exts = MEDIA_EXTENSIONS[args.type]

    async with httpx.AsyncClient(
        headers=HEADERS,
        verify=False,
        timeout=httpx.Timeout(connect=15.0, read=120.0, write=30.0, pool=10.0),
        follow_redirects=True,
    ) as client:
        thread_tasks = [
            process_thread(client, url, args.path, target_exts, semaphores)
            for url in valid_threads
        ]
        await asyncio.gather(*thread_tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
        log.info("Остановка по запросу пользователя.")
        sys.exit(0)
    except Exception as exc:
        log.critical("Непредвиденная ошибка: %s", exc, exc_info=True)
        sys.exit(1)
