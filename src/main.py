#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Система анализа и прогнозирования заявок
Главный модуль приложения
"""

import sys
import os
import logging
from gui import MainApplication

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Главная функция приложения"""
    try:
        logger.info("Запуск приложения")
        app = MainApplication()
        app.mainloop()
        logger.info("Приложение закрыто")
    except Exception as e:
        logger.error(f"Ошибка при выполнении приложения: {e}", exc_info=True)

if __name__ == "__main__":
    main()
