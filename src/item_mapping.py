#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для работы с базой соответствий артикулов и наименований и поиска похожих элементов
"""

import os
import json
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import logging
import re
import traceback

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ItemMapping:
    """Класс для работы с базой соответствий артикулов и наименований и поиска похожих элементов"""
    
    def __init__(self, mapping_file=None):
        """Инициализация базы соответствий
        
        Args:
            mapping_file (str, optional): Путь к файлу с базой соответствий
        """
        self.mapping_file = mapping_file or os.path.join('data', 'item_mapping.json')
        self.mappings = {}  # Словарь соответствий: {id_группы: {name: str, items: list}}
        self.similar_items_map = {}  # Словарь для хранения соответствия элементов и групп
        self.load_mappings()
    
    def load_mappings(self):
        """Загрузка базы соответствий из файла"""
        try:
            if os.path.exists(self.mapping_file):
                with open(self.mapping_file, 'r', encoding='utf-8') as f:
                    self.mappings = json.load(f)
                logger.info(f"База соответствий загружена из {self.mapping_file}")
            else:
                logger.info("Файл базы соответствий не найден, создана новая база")
                self.mappings = {}
                self.save_mappings()
        except Exception as e:
            logger.error(f"Ошибка при загрузке базы соответствий: {e}")
            self.mappings = {}
    
    def save_mappings(self):
        """Сохранение базы соответствий в файл"""
        try:
            # Создаем директорию, если она не существует
            os.makedirs(os.path.dirname(self.mapping_file), exist_ok=True)
            
            with open(self.mapping_file, 'w', encoding='utf-8') as f:
                json.dump(self.mappings, f, ensure_ascii=False, indent=2)
            logger.info(f"База соответствий сохранена в {self.mapping_file}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении базы соответствий: {e}")
            return False
    
    def add_item_to_group(self, group_id, item_name, item_code):
        """Добавление элемента в группу соответствий
        
        Args:
            group_id (str): Идентификатор группы
            item_name (str): Наименование элемента
            item_code (str): Артикул элемента
            
        Returns:
            bool: Успешность операции
        """
        try:
            # Проверяем на NaN и пустые значения
            if pd.isna(item_name):
                item_name = ""
            else:
                item_name = str(item_name)
                
            if pd.isna(item_code) or str(item_code).strip() == "":
                # Если артикул не определен, используем наименование
                item_code = item_name
            else:
                item_code = str(item_code)
                
            item = {
                'name': item_name,
                'code': item_code
            }
            
            if group_id not in self.mappings:
                # Создаем новую группу
                self.mappings[group_id] = {
                    'name': f"Группа {len(self.mappings) + 1}",
                    'items': [item]
                }
            else:
                # Проверяем, что такого элемента еще нет в группе
                existing_items = self.mappings[group_id]['items']
                for existing in existing_items:
                    if existing['name'] == item_name and existing['code'] == item_code:
                        logger.warning(f"Элемент {item_name} ({item_code}) уже существует в группе {group_id}")
                        return False
                
                # Добавляем элемент в существующую группу
                self.mappings[group_id]['items'].append(item)
            
            self.save_mappings()
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении элемента в группу: {e}")
            return False
    
    def remove_item_from_group(self, group_id, item_name, item_code):
        """Удаление элемента из группы соответствий
        
        Args:
            group_id (str): Идентификатор группы
            item_name (str): Наименование элемента
            item_code (str): Артикул элемента
            
        Returns:
            bool: Успешность операции
        """
        try:
            if group_id not in self.mappings:
                logger.warning(f"Группа {group_id} не найдена")
                return False
            
            items = self.mappings[group_id]['items']
            for i, item in enumerate(items):
                if item['name'] == item_name and item['code'] == item_code:
                    del items[i]
                    
                    # Если группа стала пустой, удаляем ее
                    if not items:
                        del self.mappings[group_id]
                    
                    self.save_mappings()
                    return True
            
            logger.warning(f"Элемент {item_name} ({item_code}) не найден в группе {group_id}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при удалении элемента из группы: {e}")
            return False
    
    def rename_group(self, group_id, new_name):
        """Переименование группы соответствий
        
        Args:
            group_id (str): Идентификатор группы
            new_name (str): Новое название группы
            
        Returns:
            bool: Успешность операции
        """
        try:
            if group_id not in self.mappings:
                logger.warning(f"Группа {group_id} не найдена")
                return False
            
            self.mappings[group_id]['name'] = new_name
            self.save_mappings()
            return True
        except Exception as e:
            logger.error(f"Ошибка при переименовании группы: {e}")
            return False
    
    def merge_groups(self, source_group_id, target_group_id):
        """Объединение двух групп соответствий
        
        Args:
            source_group_id (str): Идентификатор исходной группы
            target_group_id (str): Идентификатор целевой группы
            
        Returns:
            bool: Успешность операции
        """
        try:
            if source_group_id not in self.mappings:
                logger.warning(f"Исходная группа {source_group_id} не найдена")
                return False
            
            if target_group_id not in self.mappings:
                logger.warning(f"Целевая группа {target_group_id} не найдена")
                return False
            
            # Объединяем элементы групп
            source_items = self.mappings[source_group_id]['items']
            target_items = self.mappings[target_group_id]['items']
            
            # Добавляем только уникальные элементы
            for item in source_items:
                if not any(i['name'] == item['name'] and i['code'] == item['code'] for i in target_items):
                    target_items.append(item)
            
            # Удаляем исходную группу
            del self.mappings[source_group_id]
            
            self.save_mappings()
            return True
        except Exception as e:
            logger.error(f"Ошибка при объединении групп: {e}")
            return False
    
    def get_group_for_item(self, item_name, item_code):
        """Поиск группы для элемента
        
        Args:
            item_name (str): Наименование элемента
            item_code (str): Артикул элемента
            
        Returns:
            str: Идентификатор группы или None, если группа не найдена
        """
        try:
            for group_id, group in self.mappings.items():
                for item in group['items']:
                    if item['name'] == item_name and item['code'] == item_code:
                        return group_id
            
            return None
        except Exception as e:
            logger.error(f"Ошибка при поиске группы для элемента: {e}")
            return None
    
    def find_similar_items(self, data, name_col, code_col, similarity_threshold=85, progress_callback=None):
        """Поиск похожих наименований и артикулов
        
        Args:
            data (pd.DataFrame): Данные для анализа
            name_col (str): Имя столбца с наименованиями
            code_col (str): Имя столбца с артикулами
            similarity_threshold (int): Порог сходства (0-100)
            progress_callback (callable, optional): Функция обратного вызова для отображения прогресса.
                Принимает два аргумента: текущий прогресс (0-100) и сообщение о статусе.
            
        Returns:
            dict: Словарь групп похожих элементов
        """
        # Инициализируем пустые словари для результатов
        similar_groups = {}
        self.similar_items_map = {}
        
        try:
            logger.info("Начало поиска похожих элементов")
            
            # Проверяем наличие данных
            if data is None or data.empty:
                logger.error("Нет данных для обработки")
                return {}
            
            # Проверяем наличие столбцов с наименованиями и артикулами
            if name_col not in data.columns or code_col not in data.columns:
                logger.error(f"Не найдены столбцы: наименование - {name_col}, артикул - {code_col}")
                return {}
            
            # Получаем данные из базы соответствий
            logger.info("Получение данных из базы соответствий")
            
            # Если база соответствий пуста, логируем это, но продолжаем работу
            # для поиска 100% совпадений наименований и/или артикулов
            if not self.mappings:
                logger.info("База соответствий пуста. Будет выполнен поиск 100% совпадений наименований и/или артикулов.")
            
            # Создаем карту соответствий для быстрого поиска
            logger.info("Создание карты соответствий")
            item_to_group = {}
            for group_id, group in self.mappings.items():
                for item in group['items']:
                    item_key = (item['name'], item['code'])
                    item_to_group[item_key] = group_id
            
            # Получаем уникальные пары (наименование, артикул) из данных
            unique_items = set()
            for _, row in data.iterrows():
                name = row[name_col]
                code = row[code_col]
                
                # Проверяем на NaN и преобразуем в строки
                if pd.isna(name):
                    name = ""
                else:
                    name = str(name)
                    
                if pd.isna(code):
                    code = ""
                else:
                    code = str(code)
                    
                unique_items.add((name, code))
            
            logger.info(f"Обработка {len(unique_items)} уникальных пар")
            
            # Обрабатываем каждую уникальную пару
            total_items = len(unique_items)
            for i, item in enumerate(unique_items):
                # Обновляем прогресс
                if progress_callback and i % 10 == 0:
                    progress = int(i / total_items * 100)
                    progress_callback(progress, f"Обработано {i} из {total_items} элементов")
                
                try:
                    name, code = item
                    
                    # Проверяем, есть ли элемент в базе соответствий
                    if item in item_to_group:
                        # Элемент уже есть в базе, используем существующую группу
                        group_id = item_to_group[item]
                        self.similar_items_map[item] = group_id
                    else:
                        # Ищем похожие элементы среди уже известных групп
                        found_match = False
                        
                        # Если база соответствий не пуста, используем обычный поиск похожих элементов
                        if self.mappings:
                            for known_item, known_group_id in item_to_group.items():
                                similarity = self._calculate_similarity(item, known_item)
                                
                                if similarity >= similarity_threshold:
                                    # Нашли похожий элемент, добавляем в его группу
                                    self.similar_items_map[item] = known_group_id
                                    found_match = True
                                    break
                        # Если база пуста, ищем только среди уже обработанных элементов
                        else:
                            # Ищем 100% совпадения среди уже обработанных элементов
                            for processed_item, processed_group_id in self.similar_items_map.items():
                                name, code = item
                                proc_name, proc_code = processed_item
                                
                                # Проверяем на 100% совпадение наименования или артикула
                                if (name and name == proc_name) or (code and code == proc_code):
                                    self.similar_items_map[item] = processed_group_id
                                    found_match = True
                                    break
                        
                        if not found_match:
                            # Не нашли похожих элементов, создаем новую группу
                            # Но только если элемент не пустой
                            if name.strip() or code.strip():
                                # Используем артикул в качестве идентификатора группы
                                if code.strip():
                                    # Если есть артикул, используем его
                                    new_group_id = f"art_{code.strip()}"
                                else:
                                    # Если артикула нет, используем часть наименования
                                    # Убираем пробелы и спецсимволы
                                    clean_name = ''.join(c for c in name if c.isalnum())
                                    new_group_id = f"name_{clean_name[:10]}"
                                self.similar_items_map[item] = new_group_id
                except Exception as e:
                    logger.error(f"Ошибка при обработке элемента {item}: {e}")
                    continue
            
            # Подсчитываем количество уникальных групп
            unique_groups = set()
            for group_id in self.similar_items_map.values():
                unique_groups.add(group_id)
            
            logger.info(f"Найдено {len(unique_groups)} групп похожих элементов")
            
            # Формируем результат
            for item_key, group_id in self.similar_items_map.items():
                if group_id not in similar_groups:
                    similar_groups[group_id] = []
                similar_groups[group_id].append(item_key)
            
            return similar_groups
        except Exception as e:
            logger.error(f"Ошибка при поиске похожих элементов: {e}")
            logger.error(f"Детали ошибки: {traceback.format_exc()}")
            return {}
    
    def _normalize_text(self, text):
        """Нормализация текста для сравнения
        
        Args:
            text (str): Исходный текст
            
        Returns:
            str: Нормализованный текст
        """
        if pd.isna(text):
            return ""
        
        # Приводим к нижнему регистру
        text = str(text).lower()
        
        # Удаляем все символы, кроме букв, цифр и пробелов
        text = re.sub(r'[^\w\s]', '', text)
        
        # Удаляем лишние пробелы
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _calculate_similarity(self, item1, item2):
        """Вычисляет сходство между двумя элементами
        
        Args:
            item1: Первый элемент (наименование, артикул)
            item2: Второй элемент (наименование, артикул)
            
        Returns:
            float: Значение сходства от 0 до 100
        """
        try:
            # Проверяем, что элементы не None и не NaN
            if item1 is None or item2 is None or pd.isna(item1) or pd.isna(item2):
                return 0
            
            # Если элементы представлены как кортежи (наименование, артикул)
            if isinstance(item1, tuple) and isinstance(item2, tuple):
                name1, code1 = item1
                name2, code2 = item2
                
                # Проверяем на NaN и преобразуем в строки
                if pd.isna(name1):
                    name1 = ""
                else:
                    name1 = str(name1).lower()
                    
                if pd.isna(name2):
                    name2 = ""
                else:
                    name2 = str(name2).lower()
                    
                if pd.isna(code1):
                    code1 = ""
                else:
                    code1 = str(code1).lower()
                    
                if pd.isna(code2):
                    code2 = ""
                else:
                    code2 = str(code2).lower()
                
                # Вычисляем сходство наименований
                name_similarity = fuzz.ratio(name1, name2)
                
                # Если артикулы не пустые, учитываем их в общем сходстве
                if code1 and code2:
                    code_similarity = fuzz.ratio(code1, code2)
                    # Среднее значение с большим весом для артикула
                    return (name_similarity * 0.4 + code_similarity * 0.6)
                else:
                    # Если артикулы отсутствуют, используем только сходство наименований
                    return name_similarity
            else:
                # Если элементы представлены как строки или другие типы
                item1_str = "" if pd.isna(item1) else str(item1).lower()
                item2_str = "" if pd.isna(item2) else str(item2).lower()
                return fuzz.ratio(item1_str, item2_str)
        except Exception as e:
            logger.error(f"Ошибка при вычислении сходства: {e}")
            return 0  # В случае ошибки возвращаем 0
    
    def update_from_similar_items(self, similar_items):
        """Обновление базы соответствий из найденных похожих элементов
        
        Args:
            similar_items (dict): Словарь групп похожих элементов
            
        Returns:
            int: Количество добавленных групп
        """
        try:
            added_groups = 0
            
            for group_id, items_list in similar_items.items():
                # Формируем группу в нужном формате
                group_items = []
                group_name = ""
                
                # Ищем артикул для названия группы
                for item in items_list:
                    name, code = item
                    group_items.append({'name': name, 'code': code})
                    
                    # Используем первый непустой артикул для названия группы
                    if code and not group_name:
                        group_name = f"Группа {code}"
                
                # Если не нашли артикул, используем наименование
                if not group_name and items_list:
                    name, _ = items_list[0]
                    group_name = f"Группа {name[:20]}"
                
                # Создаем группу
                group = {
                    'name': group_name,
                    'items': group_items
                }
                
                # Проверяем, есть ли уже такая группа в базе
                exists = False
                for existing_id, existing_group in self.mappings.items():
                    if self._compare_groups(group_items, existing_group['items']):
                        exists = True
                        break
                
                if not exists and group_items:
                    # Добавляем новую группу
                    # Используем артикул в качестве идентификатора группы
                    self.mappings[group_id] = group
                    added_groups += 1
            
            if added_groups > 0:
                self.save_mappings()
            
            logger.info(f"Добавлено {added_groups} новых групп в базу соответствий")
            return added_groups
        except Exception as e:
            logger.error(f"Ошибка при обновлении базы соответствий: {e}")
            return 0
    
    def _compare_groups(self, group1, group2):
        """Сравнение двух групп элементов
        
        Args:
            group1 (list): Первая группа элементов
            group2 (list): Вторая группа элементов
            
        Returns:
            bool: True, если группы похожи, иначе False
        """
        # Если размеры групп сильно отличаются, считаем их разными
        if abs(len(group1) - len(group2)) > 2:
            return False
        
        # Считаем количество совпадающих элементов
        matches = 0
        for item1 in group1:
            for item2 in group2:
                if item1['name'] == item2['name'] and item1['code'] == item2['code']:
                    matches += 1
                    break
        
        # Если совпадает более половины элементов, считаем группы похожими
        return matches >= min(len(group1), len(group2)) / 2
    
    def get_all_mappings(self):
        """Получение всех соответствий
        
        Returns:
            dict: Словарь соответствий
        """
        return self.mappings
    
    def get_group(self, group_id):
        """Получение группы по идентификатору
        
        Args:
            group_id (str): Идентификатор группы
            
        Returns:
            dict: Группа соответствий или None, если группа не найдена
        """
        return self.mappings.get(group_id)
