import os
import time
import traceback
from datetime import datetime

import dotenv
import ffmpeg
import requests
from aiogram import Bot
from openai import OpenAI

dotenv.load_dotenv()

bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))

# Конфигурация API ключей и базы данных Notion
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_REELS_DB_ID")
RAPIDAPI_KEY = os.getenv("RAPID_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

# Заголовки для запросов
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}
rapidapi_headers = {
    "x-rapidapi-host": "social-download-all-in-one.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY,
    "Content-Type": "application/json"
}
openai_headers = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json"
}

fatal_errors_count = 0


# Получение данных из Notion
def get_videos_from_notion():
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    notion_headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    all_videos = []
    has_more = True
    next_cursor = None

    # Добавляем фильтр для отбора записей с установленным чекбоксом "Одобрено"
    filter_conditions = {
        "and": [
            {
                "property": "Статус",  # Поле статус (Status)
                "status": {
                    "equals": "N/A"
                }
            },
            {
                "property": "Этап",  # Поле этап (Select)
                "select": {
                    "is_empty": True
                }
            },
            {
                "property": "Одобрено",  # Имя поля чекбокса в базе данных Notion
                "checkbox": {
                    "equals": True  # Отбираем записи, где "Одобрено" = True
                }
            }
        ]
    }

    while has_more:
        payload = {
            "filter": filter_conditions  # Добавляем фильтр в запрос
        }

        if next_cursor:
            payload['start_cursor'] = next_cursor

        response = requests.post(url, headers=notion_headers, json=payload)
        data = response.json()

        if response.status_code != 200:
            raise Exception(f"Ошибка при получении данных из Notion: {data}")

        all_videos.extend(data.get('results', []))

        # Обновляем переменные для пагинации
        has_more = data.get('has_more', False)
        next_cursor = data.get('next_cursor')

    return all_videos


# Скачивание видео по ссылке
def download_video(video_url):
    global fatal_errors_count
    url = "https://social-download-all-in-one.p.rapidapi.com/v1/social/autolink"
    body = {"url": video_url}
    response = requests.post(url, headers=rapidapi_headers, json=body)
    video_data = response.json()
    if video_data['error'] is True:
        if ('limit' or 'token' in video_data['message']) or fatal_errors_count >= 20:
            bot.send_message(414054050,
                             'Лимит использования rapid api закончился или произошла критическая ошибка api\n\n' +
                             video_data['message'])
            return None
    video_response = requests.get(video_data['medias'][0]['url'])
    with open("downloaded_video.mp4", 'wb') as f:
        f.write(video_response.content)
    return video_data['medias'][0]['url']


# Преобразование видео в аудио
def convert_video_to_audio(video_file):
    audio_file = video_file.replace(".mp4", ".mp3")
    ffmpeg.input(video_file).output(audio_file).run()
    return audio_file


# Транскрибация с Whisper
def transcribe_audio(audio_file):
    audio_file = open("downloaded_video.mp3", "rb")
    transcription = client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_file,
        response_format="text"
    )

    audio_file.close()

    print(transcription)

    return transcription


# Определение языка с помощью GPT
def detect_language(text):
    url = "https://api.openai.com/v1/chat/completions"
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "user",
                "content": f"Определи язык этого текста, и верни ответ в виде короткого буквенного кода (ru, en, es): {text}"
            }
        ]
    }
    response = requests.post(url, headers=openai_headers, json=data)
    language_data = response.json()
    return language_data["choices"][0]["message"]["content"].strip()


# Перевод текста с помощью OpenAI
def translate_text_with_openai(text):
    url = "https://api.openai.com/v1/chat/completions"
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "user",
                "content": f"""Переведи следующий текст на русский, соблюдая эти правила:
                * Сохрани разговорный стиль и энергию оригинала. Используй живой, современный русский язык.
                * Адаптируй идиомы и разговорные выражения к русскоязычным аналогам, не переводи их дословно.
                * Используй короткие, простые предложения. Избегай длинных, сложных конструкций.
                * Сохраняй эмоциональные восклицания и междометия, адаптируя их к русскому языку (например, "Wow!" -> "Вау!" или "Ничего себе!").
                * Используй обращение на "ты", если в оригинале есть неформальное обращение к читателю.
                * Сохраняй структуру абзацев оригинала.
                * Адаптируй числа и единицы измерения к более привычным для русскоязычной аудитории, если это уместно.
                * Если встречаются специфические термины или названия (например, названия приложений или функций), оставляй их на английском, но давай пояснение в скобках при первом упоминании.
                * Старайся передать настроение и интонацию автора, используя соответствующие частицы и междометия в русском языке.
                * Избегай буквальных переводов фраз, которые звучат неестественно на русском. Вместо этого используй эквивалентные по смыслу разговорные выражения.
                * Верни в ответ исключительно перевод текста. Без вступительных фраз по типу "Вот перевод текста" и т.д
                Текст для перевода:
                {text}"""
            }
        ]
    }
    response = requests.post(url, headers=openai_headers, json=data)
    language_data = response.json()
    return language_data["choices"][0]["message"]["content"].strip()


# Добавление транскрибации и уникализированного текста в Notion
def update_notion_properties(page_id, stage, status, new_scenario):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    current_date = datetime.now().strftime("%Y-%m-%d")

    if status is None:
        data = {
            "properties": {
                "Этап": {
                    "select": {
                        "name": stage
                    }
                }
            }
        }
    else:
        data = {
            "properties": {
                "Этап": {
                    "select": {
                        "name": stage
                    }
                },
                "Статус": {
                    "status": {
                        "name": status
                    }
                },
                "Готовый сценарий": {
                    "rich_text": [
                        {
                            "text": {
                                "content": new_scenario
                            }
                        }
                    ]
                }
            }
        }

    response = requests.patch(url, headers=notion_headers, json=data)

    if response.status_code == 200:
        print("Successfully updated Notion page properties.")
    else:
        raise Exception(f"Error updating Notion page properties: {response.json()}")


def add_notion_blocks(page_id, headers_text, transcribe):
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"

    data = {
        "children": [
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": "Транскрибация:"
                            }
                        }
                    ]
                }
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": transcribe
                            }
                        }
                    ]
                }
            },
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": "5 заголовков:"
                            }
                        }
                    ]
                }
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": headers_text
                            }
                        }
                    ]
                }
            }
        ]
    }

    response = requests.patch(url, headers=notion_headers, json=data)

    if response.status_code == 200:
        print("Successfully added blocks to Notion page.")
    else:
        raise Exception(f"Error adding blocks to Notion page: {response.json()}")


def cant_transcribe(page_id):
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"

    data = {
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": "Не удалось сделать транскрибацию. Текст отсутствует или слишком короткий."
                            }
                        }
                    ]
                }
            }
        ]
    }

    response = requests.patch(url, headers=notion_headers, json=data)


# Функция ожидания завершения работы ассистента
def wait_on_run(run, thread):
    while run.status == "queued" or run.status == "in_progress":
        run = client.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id,
        )
        time.sleep(0.5)
    return run


def submit_message(assistant_id, thread, user_message):
    client.beta.threads.messages.create(
        thread_id=thread.id, role="user", content=user_message
    )
    return client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id,
    )


def get_response(thread):
    # Получаем объект SyncCursorPage, содержащий список сообщений
    response = client.beta.threads.messages.list(thread_id=thread.id, order="asc")

    # Доступ к данным сообщений через .data
    return response.data


def create_thread_and_run(user_input, assistant_id):
    thread = client.beta.threads.create()
    run = submit_message(assistant_id, thread, user_input)
    return thread, run


def pretty_print(messages):
    print("# Messages")
    for m in messages:
        print(f"{m.role}: {m.content[0].text.value}")


def get_unique_text_from_assistant(transcription_text):
    # Создаем новый поток общения с ассистентом и отправляем запрос на уникализацию текста
    thread, run = create_thread_and_run(f"{transcription_text}", os.getenv("TRANSCRIP_ASSISTANT"))
    run = wait_on_run(run, thread)

    # Получаем ответ
    messages = get_response(thread)
    response_message = messages[-1].content[0].text.value
    return response_message


def get_headers_from_assistant(unique_text):
    # Создаем новый поток общения с ассистентом и отправляем запрос на создание заголовков
    thread, run = create_thread_and_run(f"{unique_text}", os.getenv("HEADERS_ASSISTANT"))
    run = wait_on_run(run, thread)

    # Получаем ответ
    messages = get_response(thread)
    response_message = messages[-1].content[0].text.value
    return response_message


# Основной процесс обработки
def process_videos():
    global fatal_errors_count
    while True:
        videos = get_videos_from_notion()
        print(len(videos))
        for video in videos:
            try:
                approved = video["properties"]["Одобрено"]["checkbox"]
                status = video["properties"]["Статус"]['status']['name']
                stage = video["properties"]["Этап"]['select']
                print(video["properties"]["Источник"])
                source = video["properties"]["Источник"]['select']

                if source:
                    source_name = 'INSTA'
                else:
                    source_name = video["properties"]["Источник"]['select']

                if approved and status == 'N/A' and (stage is None or stage['name'] == 'AI') and source_name != 'YOUTUBE':

                    page_id = video["id"]
                    video_url = video["properties"]["Референс"]["url"]

                    # Обновляем статус на AI
                    update_notion_properties(page_id, "AI", None, None)

                    # Скачиваем видео

                    tries = 0

                    while True:
                        try:
                            print(1)
                            video_file_url = download_video(video_url)
                            fatal_errors_count = 0
                            break
                        except Exception as e:
                            print(e)
                            fatal_errors_count += 1
                            tries += 1
                            if tries > 3:
                                break
                    if tries > 3:
                        print(f"Ошибка при скачивании видео: {video_url}")
                        continue

                    # Преобразуем видео в аудио
                    audio_file = convert_video_to_audio(os.getcwd() + "/downloaded_video.mp4")

                    # Транскрибируем аудио
                    transcript_orig = transcribe_audio(audio_file)
                    if not transcript_orig or len(transcript_orig.split()) < 5:
                        os.remove('downloaded_video.mp4')
                        os.remove('downloaded_video.mp3')
                        print('Не удалось транскрибировать')
                        cant_transcribe(page_id)
                        update_notion_properties(page_id, "СЦЕНАРИЙ", "ВЗЯТЬ В РАБОТУ")
                        continue

                    os.remove('downloaded_video.mp4')
                    os.remove('downloaded_video.mp3')

                    # Определяем язык
                    language = detect_language(transcript_orig)

                    if language != "ru":
                        transcript = translate_text_with_openai(transcript_orig)
                    else:
                        transcript = transcript_orig
                    # Уникализируем текст
                    unique_text = get_unique_text_from_assistant(transcript)

                    # Генерируем заголовки
                    headers_text = get_headers_from_assistant(unique_text)

                    # Обновляем свойства страницы в Notion
                    update_notion_properties(page_id, "СЦЕНАРИЙ", "ВЗЯТЬ В РАБОТУ", unique_text)

                    # Добавляем блоки с текстом в Notion
                    add_notion_blocks(page_id, headers_text, transcript_orig)
            except Exception as e:
                print(traceback.format_exc())

        time.sleep(60)


if __name__ == "__main__":
    process_videos()
