#!/bin/sh

echo "Установка зависимостей..."
pip install -r requirements.txt

echo "Запуск приложения..."
python python_script_AI.py
