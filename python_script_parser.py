import os
import traceback
from datetime import datetime, timedelta

import dotenv
import pytz
import requests

dotenv.load_dotenv()

# Конфигурационные параметры
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DONORS_DB_ID = os.getenv("NOTION_DONORS_DB_ID")
NOTION_REELS_DB_ID = os.getenv("NOTION_REELS_DB_ID")
RAPIDAPI_KEY = os.getenv("RAPID_API_KEY")
DAYS_TO_FETCH = 30  # Количество дней для сбора Reels
MIN_DAYS_OLD = 3  # Минимальный возраст Reels для расчета среднего числа просмотров

# Заголовки для запросов к Notion API
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}


# Функция для получения списка доноров из Notion
def get_donors_from_notion():
    url = f"https://api.notion.com/v1/databases/{NOTION_DONORS_DB_ID}/query"
    payload = {}
    donors = []

    while True:
        response = requests.post(url, headers=notion_headers, json=payload)
        if response.status_code != 200:
            print(f"Ошибка запроса: {response.status_code}, {response.text}")
            break

        data = response.json()
        for result in data.get('results', []):
            try:
                properties = result['properties']
                username = properties['username']['title'][0]['text']['content']
                donor_id = result['id']
                donors.append({'username': username, 'donor_id': donor_id})
            except Exception:
                print(traceback.format_exc())

        # Проверяем, есть ли ещё страницы
        if data.get("has_more"):
            payload = {"start_cursor": data["next_cursor"]}
        else:
            break

    return donors


# Функция для получения Reels от доноров
def get_reels_from_donor(username):
    url = "https://instagram-social-api.p.rapidapi.com/v1/reels"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "instagram-social-api.p.rapidapi.com"
    }
    reels = []
    pagination_token = None
    threshold_date = datetime.now() - timedelta(days=DAYS_TO_FETCH)
    check_pin = False
    while True:
        querystring = {
            "username_or_id_or_url": username,
            "pagination_token": pagination_token
        }
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 200:
            data = response.json()
            items = data.get("data", {}).get("items", [])
            for item in items:
                try:
                    created_at = datetime.fromtimestamp(item.get('caption', {}).get('created_at'))

                    if created_at < threshold_date:
                        check_pin = True
                        continue
                    else:
                        check_pin = False

                    reels.append(item)
                except Exception as e:
                    pass

            if check_pin:
                return reels

            pagination_token = data.get("pagination_token")
            if not pagination_token:
                break
        else:
            print(f"Ошибка при получении Reels для {username}: {response.text}")
            break
    return reels


# Функция для добавления или обновления Reels в Notion
def upsert_reel_in_notion(reel_data, average_views):
    # Проверяем, существует ли Reel в базе Notion
    reel_id = reel_data['id']
    search_url = f"https://api.notion.com/v1/databases/{NOTION_REELS_DB_ID}/query"
    filter_data = {
        "filter": {
            "property": "ID",
            "number": {
                "equals": int(reel_id)
            }
        }
    }
    response = requests.post(search_url, headers=notion_headers, json=filter_data)
    print(response.json())
    results = response.json().get('results', [])
    if results:
        print(0)
        # Обновляем существующий Reel
        page_id = results[0]['id']
        update_url = f"https://api.notion.com/v1/pages/{page_id}"
        properties = construct_reel_properties(reel_data, average_views)
        data = {"properties": properties}
        response = requests.patch(update_url, headers=notion_headers, json=data)
    else:
        print(1)
        # Добавляем новый Reel
        create_url = "https://api.notion.com/v1/pages"
        properties = construct_reel_properties(reel_data, average_views)
        data = {
            "parent": {"database_id": NOTION_REELS_DB_ID},
            "properties": properties
        }
        response = requests.post(create_url, headers=notion_headers, json=data)
    if response.status_code in [200, 201]:
        print(f"Reel {reel_id} успешно обновлен/добавлен в Notion.")
    else:
        print(f"Ошибка при обновлении/добавлении Reel {reel_id}: {response.text}")


# Функция для построения свойств Reel для Notion
def construct_reel_properties(reel_data, average_views):
    reel_id = reel_data['id']
    created_at = datetime.fromtimestamp(reel_data.get('caption', {}).get('created_at'))
    code = reel_data.get('code', '')
    link = f"https://www.instagram.com/reel/{code}"
    username = reel_data.get('user', {}).get('username', '')
    caption_text = reel_data.get('caption', {}).get('text', '')
    if caption_text:
        caption_words = caption_text.split()
        if len(caption_words) > 7:
            title = ' '.join(caption_words[:7])
        else:
            title = caption_text
    else:
        title = reel_id
    play_count = reel_data.get('play_count', 0) or 0
    like_count = reel_data.get('like_count', 0) or 0
    comment_count = reel_data.get('comment_count', 0) or 0
    reshare_count = reel_data.get('reshare_count', 0) or 0
    ER = round((reshare_count / play_count) * 100, 2) if play_count > 0 else 0
    KF = round(play_count / average_views, 2) if average_views > 0 else 0
    properties = {
        "Дата референса": {"date": {"start": created_at.isoformat()}},
        "Референс": {"url": link},
        "Автор": {"rich_text": [{"text": {"content": username}}]},
        "Источник": {"select": {"name": "INSTA"}},
        "Название": {"title": [{"text": {"content": title}}]},
        "Просмотры": {"number": play_count},
        "Лайки": {"number": like_count},
        "Комменты": {"number": comment_count},
        "Репосты": {"number": reshare_count},
        "ER": {"number": ER},
        "КФ": {"number": KF},
        "ID": {"number": int(reel_id)},
    }
    return properties


# Функция для обновления информации о донорах
def update_donor_info(donor, average_views):
    username = donor['username']
    donor_id = donor['donor_id']

    # Получение данных из Instagram
    url = f"https://instagram-social-api.p.rapidapi.com/v1/info?username_or_id_or_url={username}"
    headers_instagram = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "instagram-social-api.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers_instagram)
    if response.status_code == 200:
        data = response.json()

        user_data = data.get('data', {})
        print(user_data)
        print(user_data.get('id'))
        follower_count = user_data.get('follower_count', 0)
        previous_followers = donor.get('followers', 0)
        growth = follower_count - previous_followers if previous_followers else 0

        # Обновление информации о доноре в Notion
        update_url = f"https://api.notion.com/v1/pages/{donor_id}"
        properties = {
            "Ссылка": {"url": "https://www.instagram.com/" + username},
            "Подписчики": {"number": follower_count},
            "Прирост за неделю": {"number": growth},
            "Среднее число просмотров": {"number": average_views},
            "ID": {"rich_text": [{"text": {"content": donor_id}}]}
        }
        response = requests.patch(update_url, headers=notion_headers, json={"properties": properties})
        if response.status_code == 200:
            print(f"Информация о доноре {username} успешно обновлена.")
        else:
            print(f"Ошибка при обновлении донора {username}: {response.text}")
    else:
        print(f"Ошибка при получении данных о доноре {username}: {response.text}")


def get_videos_from_notion():
    url = f"https://api.notion.com/v1/databases/{NOTION_REELS_DB_ID}/query"
    all_videos = []
    has_more = True
    next_cursor = None
    while has_more:
        try:
            payload = {}
            if next_cursor:
                payload['start_cursor'] = next_cursor

            response = requests.post(url, headers=notion_headers, json=payload)
            data = response.json()

            if response.status_code != 200:
                print(f"Ошибка при получении данных из Notion: {data}")

            all_videos.extend(data.get('results', []))

            # Обновляем переменные для пагинации
            has_more = data.get('has_more', False)
            next_cursor = data.get('next_cursor')
        except:
            pass

    return all_videos


def clean_old_reels():
    reels = get_videos_from_notion()
    threshold_date = datetime.now() - timedelta(days=90)
    for reel in reels:
        try:
            properties = reel['properties']
            status = properties.get('Статус', {}).get('status', {}).get('name', '')
            if properties.get('Этап', {}).get('select', {}) is not None:
                stage = properties.get('Этап', {}).get('select', {}).get('name', '')
            else:
                stage = None
                created_at_str = properties.get('Дата референса', {}).get('date', {}).get('start', '')
            if created_at_str:
                utc = pytz.UTC
                created_at = datetime.fromisoformat(created_at_str)

                if created_at < utc.localize(threshold_date):
                    if created_at < utc.localize(threshold_date) and (status == 'N/A' and stage is None):
                        # Удаляем Reel
                        page_id = reel['id']
                        print('Удаление')
                        delete_url = f"https://api.notion.com/v1/pages/{page_id}"
                        data = {"archived": True}
                        response = requests.patch(delete_url, headers=notion_headers, json=data)
                        if response.status_code == 200:
                            print(f"Reel {page_id} успешно удален.")
                        else:
                            print(f"Ошибка при удалении Reel {page_id}: {response.text}")
        except Exception as e:
            print(traceback.format_exc())


# Основная функция
def main():
    donors = get_donors_from_notion()

    for donor in donors:
        try:

            username = donor['username']
            print(username)

            reels = get_reels_from_donor(username)

            current_date = datetime.now()
            reels_for_average = [reel for reel in reels if (current_date - datetime.fromtimestamp(
                reel.get('caption', {}).get('created_at'))).days >= MIN_DAYS_OLD]
            if reels_for_average:
                total_views = sum(reel.get('play_count', 0) or 0 for reel in reels_for_average)
                average_views = total_views / len(reels_for_average)
            else:
                average_views = 0
            for reel in reels:
                try:
                    upsert_reel_in_notion(reel, average_views)
                except:
                    print(traceback.format_exc())

            update_donor_info(donor, round(average_views))
        except:
            print(traceback.format_exc())

    clean_old_reels()


if __name__ == "__main__":
    main()
