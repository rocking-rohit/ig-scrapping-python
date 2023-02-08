import csv
import logging
import time
from pathlib import Path
from random import randint

import requests
from instagrapi import Client
from instagrapi.exceptions import LoginRequired


URLS = [
    'https://www.instagram.com/p/Cl9QQZyA1xa/',
    'https://www.instagram.com/p/Cl6wNbgvrYT/',
    'https://www.instagram.com/p/ClrIu_jJNYO/',
]

CACHE = set()
CACHE_PATH = 'cache.txt'

TIMEOUT = [5, 10]
TIMEOUT_EXCEPTION = 5

MEDIA_PATH = 'media'

CSV_FIELDS = [
    'url', 'media_type', 'media_path', 'title', 'description', 'photo', 'video', 'account_name', 'notes'
] + [f'resource_{i}' for i in range(1, 11)]
CSV_PATH = 'data.csv'

MEDIA_TYPES = {
    1: 'photo',
    2: 'video',
    8: 'album',
}

IG_USERNAME = ''
IG_PASSWORD = ''


def load_cache():
    global CACHE

    if not Path(CACHE_PATH).exists():
        with open(CACHE_PATH, 'w+', encoding='utf-8') as f:
            f.write('')
            f.close()
        return

    with open(CACHE_PATH, 'r', encoding='utf-8') as f:
        cache_string = f.read().rstrip()
        if cache_string:
            CACHE = set(cache_string.split('\n'))
        f.close()


def save_cache():
    with open(CACHE_PATH, 'w+', encoding='utf-8') as f:
        f.write('\n'.join(CACHE) + '\n')
        f.close()


def append_cache(url):
    CACHE.add(url)
    with open(CACHE_PATH, 'a+', encoding='utf-8') as f:
        f.write(url.rstrip('\n') + '\n')
        f.close()


def load_csv():
    csv_exists = Path(CSV_PATH).exists()
    if csv_exists:
        return

    with open(CSV_PATH, mode='w+', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        f.close()


def append_to_csv(data):
    with open(CSV_PATH, mode='a+', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(data)
        f.close()


def save_file(post_url, file_url, user_agent, step):
    url_id = post_url.rstrip('/').split('/')[-1]

    dir_path = Path(MEDIA_PATH).joinpath(url_id)
    dir_path.mkdir(parents=True, exist_ok=True)

    file_name = file_url.split('?')[0].split('/')[-1]
    file_path = dir_path.joinpath(file_name)

    try:
        logging.info(f'[{step}] Trying to fetch media of {post_url} ({file_url})...')
        response = requests.get(file_url, headers={'User-Agent': user_agent})
    except Exception as e:
        logging.error(f'[{step}] There was an unexpected error when trying to fetch media: {e.__class__.__name__} - {str(e)}.')
        return

    logging.info(f'[{step}] Media fetch successful, saving file to {file_path.absolute()}')
    with open(file_path, 'wb+') as f:
        f.write(response.content)
        f.close()
    logging.info(f'[{step}] File successfully saved!')

    return str(dir_path.absolute())


def scrape(client: Client, url: str):
    media_pk = client.media_pk_from_url(url)
    data = client.media_info(media_pk)
    data_json = {
        'url': url,

        # media
        'media_type': MEDIA_TYPES[data.media_type],
        'title': data.title,
        'description': data.caption_text,
        'photo': str(data.thumbnail_url) if data.thumbnail_url else None,
        'video': str(data.video_url) if data.video_url else None,

        # account
        'account_name': data.user.username,

        'notes': 'OK',
    }

    for i, r in enumerate(data.resources):
        data_json[f'resource_{i+1}'] = str(r.video_url) if r.video_url else str(r.thumbnail_url)

    for i in range(1, 11):
        key = f'resource_{i}'
        if f'resource_{i}' not in data_json:
            data_json[key] = None

    return data_json


def main():
    logging.info('Starting bot...')

    logging.info('Loading cache...')
    load_cache()

    logging.info('Loading CSV...')
    load_csv()

    total_count, unique_count, cached_count = len(URLS), len(set(URLS)), len(CACHE)
    to_process_count = unique_count - cached_count

    logging.info(
        f'Found {total_count} url(s) ({unique_count} unique). '
        f'Already processed {cached_count} url(s), need to process {to_process_count} more.'
    )

    processed_count = 0

    client = Client()
    if IG_USERNAME and IG_PASSWORD:
        client.login(IG_USERNAME, IG_PASSWORD)

    for url in URLS:
        if url in CACHE:
            continue

        try:
            logging.info(f'Trying to gather data ({url})...')

            data = scrape(client, url)

            media_urls = [data['photo'], data['video']] + [data[f'resource_{i}'] for i in range(1, 11)]
            media_urls = [i for i in media_urls if i]
            media_path = None
            for index, media_url in enumerate(media_urls):
                media_path = save_file(url, media_url, client.settings['user_agent'], index+1)

            if media_path:
                data['media_path'] = media_path

            append_to_csv(data)
            append_cache(url)
            processed_count += 1

            timeout = randint(*TIMEOUT) / 10
            logging.info(f'({round((processed_count/to_process_count)*100, 4)}% - {processed_count}/{to_process_count}) '
                         f'URL processed! '
                         f'Waiting {timeout} seconds till next request...')
            time.sleep(timeout)
        except LoginRequired as e:
            logging.info(f'This post is private (or login is required to continue)! Skipping...')
            processed_count += 1

            data = {'url': url, 'notes': 'LOGIN_REQUIRED'}
            append_to_csv(data)
            append_cache(url)

            timeout = randint(*TIMEOUT) / 10
            time.sleep(timeout)
        except Exception as e:
            logging.error(
                f'There was an unexpected error when trying to fetch data: {e.__class__.__name__} - {str(e)}. '
                f'Skipping this URL and waiting {TIMEOUT_EXCEPTION} second(s)...'
            )
            time.sleep(TIMEOUT_EXCEPTION)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler('debug.log'),
            logging.StreamHandler()
        ],
        format='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%x %X'
    )
    main()
