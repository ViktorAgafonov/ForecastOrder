import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
from dateutil.parser import parse
import logging
from item_mapping import ItemMapping

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    """Класс для обработки данных из Excel-файлов с заявками"""
    
    def __init__(self):
        """Инициализация обработчика данных"""
        self.data = None
        self.processed_data = None
        self.similar_items_map = {}
        self.item_mapping = ItemMapping()
        self.article_delivery_times = {}  # Словарь сроков поставки по артикулам
        
    def set_item_mapping(self, item_mapping):
        """Установка базы соответствий артикулов и наименований
        
        Args:
            item_mapping: Экземпляр класса ItemMapping
        """
        self.item_mapping = item_mapping
        
    def load_data(self, file_path):
        """Загрузка данных из Excel-файла
        
        Args:
            file_path (str): Путь к Excel-файлу
            
        Returns:
            bool: Успешность загрузки
        """
        try:
            # Пробуем загрузить файл с автоматическим определением листа
            self.data = pd.read_excel(file_path)
            
            # Проверка загруженных данных
            if self.data.empty:
                logger.warning(f"Загруженный файл {file_path} не содержит данных")
                
                # Пробуем загрузить все листы и посмотреть, есть ли данные на других листах
                xls = pd.ExcelFile(file_path)
                sheet_names = xls.sheet_names
                logger.info(f"Найдены листы в файле: {sheet_names}")
                
                # Пробуем загрузить первый лист, если есть
                if sheet_names:
                    self.data = pd.read_excel(file_path, sheet_name=sheet_names[0])
                    logger.info(f"Загружен лист '{sheet_names[0]}'")
            
            # Выводим информацию о загруженных данных
            logger.info(f"Загружено строк: {len(self.data)}, столбцов: {len(self.data.columns)}")
            logger.info(f"Столбцы в файле: {list(self.data.columns)}")
            
            # Переименовываем столбцы без названий и исправляем некорректные названия
            column_mapping = {}
            
            # Проходим по всем столбцам и переименовываем их
            for i, col in enumerate(self.data.columns):
                col_str = str(col).strip()
                
                # Обрабатываем столбцы без названий (Unnamed)
                if 'unnamed' in col_str.lower():
                    # Игнорируем колонки с единицами измерения
                    if i == 6 or i == 8:  # Столбцы с единицами измерения - игнорируем
                        continue
                    else:
                        column_mapping[col] = f'столбец_{i}'
                elif '\n' in col_str:  # Убираем переносы строк в названиях столбцов
                    column_mapping[col] = col_str.replace('\n', ' ')
            
            # Применяем переименование столбцов
            self.data = self.data.rename(columns=column_mapping)
            
            # Приведение имен столбцов к нижнему регистру для упрощения поиска
            self.data.columns = [str(col).lower().strip() for col in self.data.columns]
            
            # Выводим обновленные столбцы
            logger.info(f"Обновленные столбцы: {list(self.data.columns)}")
            
            logger.info(f"Данные успешно загружены из {file_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            return False
    
    def preprocess_data(self):
        """Предварительная обработка данных
        
        Returns:
            bool: Успешность обработки
        """
        if self.data is None:
            logger.error("Нет данных для обработки")
            return False
        
        try:
            # Копируем данные для обработки
            self.processed_data = self.data.copy()
            
            # Преобразуем даты
            date_columns = [col for col in self.processed_data.columns if 'дата' in col]
            for col in date_columns:
                self.processed_data[col] = pd.to_datetime(self.processed_data[col], errors='coerce')
            
            # Поиск столбцов с наименованиями и артикулами
            name_col = next((col for col in self.processed_data.columns if 'наименование' in col or 'название' in col), None)
            article_col = next((col for col in self.processed_data.columns if 'артикул' in col or 'код' in col), None)
            
            # Логируем найденные столбцы
            logger.info(f"Найдены столбцы: наименование - {name_col}, артикул - {article_col}")
            
            # Извлечение артикулов из наименований, если артикулы отсутствуют или пустые
            if name_col:
                # Создаем временные столбцы для очищенных наименований и извлеченных артикулов
                extracted = self.processed_data[name_col].apply(self._extract_article_from_name)
                cleaned_names = [item[0] for item in extracted]
                extracted_articles = [item[1] for item in extracted]
                
                # Сохраняем очищенные наименования
                self.processed_data[f'{name_col}_очищенное'] = cleaned_names
                
                # Если артикулы отсутствуют или пустые, используем извлеченные
                if article_col:
                    # Заполняем пустые артикулы извлеченными из наименований
                    for i, article in enumerate(extracted_articles):
                        if article and (pd.isna(self.processed_data.iloc[i][article_col]) or str(self.processed_data.iloc[i][article_col]).strip() == ""):
                            self.processed_data.at[self.processed_data.index[i], article_col] = article
                else:
                    # Создаем новый столбец для артикулов
                    self.processed_data['артикул'] = extracted_articles
                    article_col = 'артикул'
            
            # Нормализация наименований и артикулов
            if name_col:
                self.processed_data[f'{name_col}_норм'] = self.processed_data[name_col].apply(
                    lambda x: self._normalize_text(str(x)) if pd.notna(x) else '')
            
            if article_col:
                self.processed_data[f'{article_col}_норм'] = self.processed_data[article_col].apply(
                    lambda x: self._normalize_text(str(x)) if pd.notna(x) else '')
            
            # Поиск столбцов с количествами
            quantity_columns = [col for col in self.processed_data.columns if 'количество' in col or 'кол-во' in col]
            logger.info(f"Найдены столбцы с количествами: {quantity_columns}")
            
            # Обработка количеств и формул поставки
            for col in quantity_columns:
                if 'числовое' not in col:  # Избегаем обработку уже обработанных столбцов
                    self.processed_data[f'{col}_числовое'] = self.processed_data[col].apply(
                        lambda x: self._extract_total_quantity(str(x)) if pd.notna(x) else 0)
            
            # Поиск столбца с комментариями
            comment_col = next((col for col in self.processed_data.columns if 'комментарий' in col or 'примечание' in col or 'коммент' in col), None)
            logger.info(f"Найден столбец с комментариями: {comment_col}")
            
            logger.info("Данные успешно предобработаны")
            return True
        except Exception as e:
            logger.error(f"Ошибка при предобработке данных: {e}")
            return False
    
    def _extract_article_from_name(self, name):
        """Извлечение артикула из наименования
        
        Args:
            name (str): Наименование товара
            
        Returns:
            tuple: Очищенное наименование и извлеченный артикул
        """
        if pd.isna(name):
            return "", ""
        
        name = str(name).strip()
        
        # Шаблоны для поиска артикулов
        patterns = [
            # Артикул в скобках
            r'\(([A-Za-z0-9\-\.]+)\)',
            # Артикул после слова "арт", "артикул", "art", "article"
            r'(?:[артикулАРТИКУЛ]|art|article)[\s:\-\.]*([A-Za-z0-9\-\.]+)',
            # Артикул в формате буквы-цифры
            r'\b([A-Za-z]+[\-\.][0-9]+[A-Za-z0-9\-\.]*)\b',
            # Артикул в формате цифры-буквы
            r'\b([0-9]+[\-\.][A-Za-z]+[A-Za-z0-9\-\.]*)\b',
            # Латинские слова с цифрами (артикул) среди кириллических слов (наименование)
            r'\b([A-Za-z0-9][A-Za-z0-9\-\.]{2,})\b'
        ]
        
        article = ""
        cleaned_name = name
        
        # Проверяем, есть ли в наименовании кириллические символы
        has_cyrillic = bool(re.search('[А-я]', name))
        
        # Если есть кириллица, пробуем найти артикул по шаблонам
        if has_cyrillic:
            # Находим все латинские слова с цифрами
            latin_words_with_digits = re.findall(r'\b([A-Za-z0-9][A-Za-z0-9\-\.]{2,})\b', name)
            
            # Если нашли латинские слова с цифрами, используем первое как артикул
            if latin_words_with_digits:
                # Проверяем, что в найденном слове есть хотя бы одна цифра
                for word in latin_words_with_digits:
                    if re.search(r'[0-9]', word):
                        article = word
                        # Удаляем найденный артикул из наименования
                        cleaned_name = re.sub(re.escape(word), '', name).strip()
                        break
        
        # Если не нашли артикул по латинским словам с цифрами, пробуем по другим шаблонам
        if not article:
            for pattern in patterns:
                match = re.search(pattern, name, re.IGNORECASE)
                if match:
                    article = match.group(1)
                    # Удаляем найденный артикул из наименования
                    cleaned_name = re.sub(pattern, '', name, flags=re.IGNORECASE).strip()
                    break
        
        # Удаляем лишние пробелы и символы
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
        cleaned_name = re.sub(r'^[\s\-\.,;:]+|[\s\-\.,;:]+$', '', cleaned_name)
        
        return cleaned_name, article
    
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
        
        # Удаляем лишние пробелы
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _extract_total_quantity(self, quantity_str):
        """Извлечение общего количества из строки с формулой
        
        Args:
            quantity_str (str): Строка с количеством или формулой
            
        Returns:
            float: Общее количество
        """
        try:
            # Проверяем, есть ли формула (например, "2+3")
            if '+' in quantity_str or '-' in quantity_str:
                # Используем безопасный eval для вычисления формулы
                # Заменяем все нецифровые и не операторные символы
                clean_formula = re.sub(r'[^\d\+\-\*\/\.]', '', quantity_str)
                if clean_formula:
                    return eval(clean_formula)
                return 0
            else:
                # Если нет формулы, просто преобразуем в число
                return float(re.sub(r'[^\d\.]', '', quantity_str) or 0)
        except Exception:
            return 0
    
    def _calculate_similarity(self, item1, item2):
        """Вычисляет сходство между двумя элементами
        
        Args:
            item1: Первый элемент (наименование, артикул)
            item2: Второй элемент (наименование, артикул)
            
        Returns:
            float: Значение сходства от 0 до 100
        """
        from fuzzywuzzy import fuzz
        
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
    
    def find_similar_items(self, similarity_threshold=85, progress_callback=None):
        """Поиск похожих наименований и артикулов
        
        Args:
            similarity_threshold (int): Порог сходства (0-100)
            progress_callback (callable, optional): Функция обратного вызова для отображения прогресса.
                Принимает два аргумента: текущий прогресс (0-100) и сообщение о статусе.
        
        Returns:
            dict: Словарь групп похожих элементов
        """
        logger.info("Начало поиска похожих элементов")
        
        # Проверяем наличие данных
        if self.processed_data is None or self.processed_data.empty:
            logger.error("Нет данных для обработки")
            return {}
        
        # Поиск столбцов с наименованиями и артикулами
        logger.info("Поиск столбцов с наименованиями и артикулами")
        name_col = next((col for col in self.processed_data.columns if 'наименование' in col), None)
        code_col = next((col for col in self.processed_data.columns if 'артикул' in col), None)
        
        logger.info(f"Найдены столбцы: наименование - {name_col}, артикул - {code_col}")
        
        # Используем ItemMapping для поиска похожих элементов
        similar_groups = self.item_mapping.find_similar_items(
            self.processed_data, 
            name_col, 
            code_col, 
            similarity_threshold, 
            progress_callback
        )
        
        # Сохраняем карту соответствий для дальнейшего использования
        self.similar_items_map = self.item_mapping.similar_items_map
        
        return similar_groups
    
    def _calculate_similarity(self, item1, item2):
        """Вычисление сходства между элементами
        
        Args:
            item1: Первый элемент
            item2: Второй элемент
            
        Returns:
            int: Процент сходства (0-100)
        """
        if isinstance(item1, tuple) and isinstance(item2, tuple):
            # Если элементы - кортежи (наименование, артикул)
            name_sim = fuzz.ratio(item1[0], item2[0])
            code_sim = fuzz.ratio(item1[1], item2[1])
            # Взвешенное среднее, артикул имеет больший вес
            return 0.4 * name_sim + 0.6 * code_sim
        else:
            # Если элементы - строки
            return fuzz.ratio(str(item1), str(item2))
    
    def analyze_order_frequency(self, progress_callback=None):
        """Анализ частоты заказов для каждого элемента
        
        Args:
            progress_callback (callable, optional): Функция обратного вызова для отображения прогресса.
                Принимает два аргумента: текущий прогресс (0-100) и сообщение о статусе.
        
        Returns:
            pd.DataFrame: Данные о частоте заказов
        """
        if self.processed_data is None:
            logger.error("Нет обработанных данных")
            return None
        
        try:
            # Подготовка данных для анализа
            analysis_data = []
            
            # Определяем ключевые столбцы
            item_cols = []
            
            # Поиск столбцов с наименованиями и артикулами (оригинальные, а не нормализованные)
            name_col = next((col for col in self.processed_data.columns if 'наименование' in col and 'норм' not in col), None)
            article_col = next((col for col in self.processed_data.columns if 'артикул' in col and 'норм' not in col), None)
            
            # Также найдем нормализованные столбцы для логирования
            name_norm_col = next((col for col in self.processed_data.columns if 'наименование' in col and 'норм' in col), None)
            article_norm_col = next((col for col in self.processed_data.columns if 'артикул' in col and 'норм' in col), None)
            
            if name_col:
                item_cols.append(name_col)
            if article_col:
                item_cols.append(article_col)
            
            # Поиск столбца с датой заявки
            date_col = next((col for col in self.processed_data.columns if 'дата заявки' in col), None)
            
            # Поиск столбца с числовым количеством
            quantity_col = next((col for col in self.processed_data.columns if 'числовое' in col), None)
            
            # Логируем найденные столбцы
            logger.info(f"Столбцы для анализа: наименование - {name_col}, артикул - {article_col}, дата - {date_col}, количество - {quantity_col}")
            logger.info(f"Нормализованные столбцы: наименование - {name_norm_col}, артикул - {article_norm_col}")
            
            if not item_cols or not date_col or not quantity_col:
                logger.error("Не найдены необходимые столбцы для анализа")
                return None
            
            # Группируем данные по элементам и датам
            # Создаем копию данных для обработки
            grouped = self.processed_data.copy()
            
            # Убедимся, что все столбцы существуют
            for col in item_cols + [date_col, quantity_col]:
                if col not in grouped.columns:
                    logger.error(f"Столбец {col} не найден в данных")
                    return None
            
            # Проверяем тип столбца с датой и преобразуем его
            if pd.api.types.is_datetime64_any_dtype(grouped[date_col]):
                # Если столбец уже datetime, преобразуем в строки для безопасной группировки
                grouped[date_col] = grouped[date_col].dt.strftime('%Y-%m-%d')
            else:
                # Если столбец не datetime, попробуем преобразовать его
                try:
                    grouped[date_col] = pd.to_datetime(grouped[date_col]).dt.strftime('%Y-%m-%d')
                except Exception as e:
                    logger.error(f"Не удалось преобразовать столбец даты: {e}")
                    return None
            
            # Убедимся, что столбец с количеством имеет числовой тип
            if not pd.api.types.is_numeric_dtype(grouped[quantity_col]):
                try:
                    # Попытаемся преобразовать в числовой тип
                    grouped[quantity_col] = pd.to_numeric(grouped[quantity_col], errors='coerce')
                    # Заменяем NaN на 0
                    grouped[quantity_col] = grouped[quantity_col].fillna(0)
                except Exception as e:
                    logger.error(f"Не удалось преобразовать столбец количества в числовой тип: {e}")
                    return None
            
            # Теперь группируем и суммируем
            logger.info(f"Группировка по столбцам: {item_cols + [date_col]}")
            logger.info(f"Суммирование по столбцу: {quantity_col}")
            
            # Используем альтернативный подход к группировке данных
            # Создаем новый DataFrame для результатов
            result_data = []
            
            # Группируем данные вручную
            # Создаем уникальные ключи для группировки
            grouped['group_key'] = grouped.apply(
                lambda row: tuple(str(row[col]) for col in item_cols + [date_col]),
                axis=1
            )
            
            # Группируем по созданному ключу и суммируем количество
            for key, group in grouped.groupby('group_key'):
                # Создаем запись для результата
                row = {}
                
                # Добавляем значения столбцов для группировки
                for i, col in enumerate(item_cols + [date_col]):
                    # Берем первое значение из группы
                    row[col] = group[col].iloc[0]
                
                # Суммируем количество
                row[quantity_col] = group[quantity_col].sum()
                
                # Добавляем запись в результат
                result_data.append(row)
            
            # Создаем новый DataFrame из результатов
            grouped = pd.DataFrame(result_data)
            
            # Преобразуем даты обратно в datetime
            try:
                grouped[date_col] = pd.to_datetime(grouped[date_col])
            except Exception as e:
                logger.error(f"Не удалось преобразовать даты обратно в datetime: {e}")
                # Продолжаем выполнение, так как это не критическая ошибка
                
            # Получаем группы похожих элементов
            # Теперь similar_items_map - это словарь, где ключи - это идентификаторы групп,
            # а значения - списки элементов в группе
            similar_groups = {}
            for item_key, group_id in self.similar_items_map.items():
                if group_id not in similar_groups:
                    similar_groups[group_id] = []
                similar_groups[group_id].append(item_key)
            
            logger.info(f"Найдено {len(similar_groups)} групп для анализа")
            if not similar_groups:
                logger.warning("Словарь similar_items_map пуст или имеет неправильный формат")
                logger.info(f"Содержимое similar_items_map: {self.similar_items_map}")
            
            # Сообщаем о начале анализа
            if progress_callback:
                progress_callback(10, f"Найдено {len(similar_groups)} групп для анализа")
            
            # Для каждой группы похожих элементов
            total_groups = len(similar_groups)
            for i, (group_id, items) in enumerate(similar_groups.items()):
                # Обновляем прогресс
                if progress_callback and total_groups > 0:
                    progress_value = 10 + int(80 * i / total_groups)  # От 10% до 90%
                    progress_callback(progress_value, f"Обработка группы {i+1} из {total_groups}")
                
                
                group_data = pd.DataFrame()
                
                # Объединяем данные для всех похожих элементов
                for item in items:
                    
                    if isinstance(item, tuple) and len(item_cols) > 1:
                        
                        mask = (self.processed_data[item_cols[0]] == item[0]) & (self.processed_data[item_cols[1]] == item[1])
                    elif len(item_cols) == 1:
                        
                        mask = self.processed_data[item_cols[0]] == item
                    else:
                        logger.warning(f"Недостаточно столбцов для поиска: {item_cols}")
                        continue
                    
                    item_data = self.processed_data[mask]
                    
                    group_data = pd.concat([group_data, item_data])
                
                if group_data.empty:
                    continue
                
                # Сортируем по дате
                group_data = group_data.sort_values(by=date_col)
                
                # Вычисляем интервалы между заказами
                dates = group_data[date_col].unique()
                intervals = []
                
                for i in range(1, len(dates)):
                    interval_days = (dates[i] - dates[i-1]).days
                    intervals.append(interval_days)
                
                if intervals:
                    # Вычисляем статистику интервалов
                    avg_interval = np.mean(intervals)
                    median_interval = np.median(intervals)
                    min_interval = np.min(intervals)
                    max_interval = np.max(intervals)
                    
                    # Общее количество заказанного
                    total_ordered = group_data[quantity_col].sum()
                    
                    # Вычисляем среднее количество в день
                    if len(dates) > 1:
                        total_period = (dates[-1] - dates[0]).days
                        if total_period > 0:
                            daily_consumption = total_ordered / total_period
                        else:
                            daily_consumption = 0
                    else:
                        daily_consumption = 0
                    
                    # Добавляем информацию в результаты анализа
                    item_info = {
                        'group_id': group_id,
                        'items': items,
                        'avg_interval_days': avg_interval,
                        'median_interval_days': median_interval,
                        'min_interval_days': min_interval,
                        'max_interval_days': max_interval,
                        'total_ordered': total_ordered,
                        'daily_consumption': daily_consumption,
                        'order_dates': dates.tolist(),
                        'order_intervals': intervals
                    }
                    
                    analysis_data.append(item_info)
            
            logger.info(f"Анализ частоты заказов завершен для {len(analysis_data)} групп элементов")
            
            # Сообщаем о завершении анализа
            if progress_callback:
                progress_callback(95, f"Анализ завершен для {len(analysis_data)} групп")
            
            result_df = pd.DataFrame(analysis_data)
            
            # Финальное сообщение о завершении
            if progress_callback:
                progress_callback(100, "Анализ частоты заказов успешно завершен")
            
            return result_df
        except Exception as e:
            logger.error(f"Ошибка при анализе частоты заказов: {e}")
            import traceback
            logger.error(f"Детали ошибки: {traceback.format_exc()}")
            return None
    
    def predict_future_orders(self, frequency_data, prediction_days=365):
        """Прогнозирование будущих заказов
        
        Args:
            frequency_data (pd.DataFrame): Данные о частоте заказов
            prediction_days (int): Количество дней для прогноза
            
        Returns:
            pd.DataFrame: Прогноз будущих заказов
        """
        if frequency_data is None or frequency_data.empty:
            logger.error("Нет данных о частоте заказов")
            return None
        
        try:
            today = datetime.now()
            forecast_end = today + timedelta(days=prediction_days)
            
            predictions = []
            
            for _, row in frequency_data.iterrows():
                group_id = row['group_id']
                items = row['items']
                avg_interval = row['avg_interval_days']
                daily_consumption = row['daily_consumption']
                last_order_date = max(row['order_dates'])
                
                # Если нет данных об интервалах или потреблении, пропускаем
                if pd.isna(avg_interval) or avg_interval <= 0 or pd.isna(daily_consumption) or daily_consumption <= 0:
                    continue
                
                # Прогнозируем следующие даты заказов
                next_date = last_order_date
                forecast_dates = []
                
                while next_date < forecast_end:
                    next_date = next_date + timedelta(days=int(avg_interval))
                    if next_date > today and next_date <= forecast_end:
                        # Прогнозируемое количество на основе дневного потребления
                        estimated_quantity = daily_consumption * avg_interval
                        
                        forecast_dates.append({
                            'date': next_date,
                            'estimated_quantity': round(estimated_quantity, 2)
                        })
                
                if forecast_dates:
                    predictions.append({
                        'group_id': group_id,
                        'items': items,
                        'avg_interval_days': avg_interval,
                        'daily_consumption': daily_consumption,
                        'last_order_date': last_order_date,
                        'forecast_dates': forecast_dates
                    })
            
            logger.info(f"Прогноз будущих заказов создан для {len(predictions)} групп элементов")
            return pd.DataFrame(predictions)
        except Exception as e:
            logger.error(f"Ошибка при прогнозировании будущих заказов: {e}")
            return None
    
    def parse_delivery_comments(self):
        """Парсинг комментариев о поставках
        
        Returns:
            pd.DataFrame: Данные с разобранными комментариями
        """
        if self.processed_data is None:
            logger.error("Нет обработанных данных")
            return None
        
        try:
            # Ищем столбцы с комментариями и количеством с более гибким поиском
            comment_col = next((col for col in self.processed_data.columns if 'комментарий' in col or 'примечание' in col or 'коммент' in col), None)
            quantity_col = next((col for col in self.processed_data.columns if ('количество' in col or 'кол-во' in col) and 'числовое' not in col), None)
            
            # Логируем найденные столбцы
            logger.info(f"Столбцы для парсинга комментариев: комментарий - {comment_col}, количество - {quantity_col}")
            
            if not comment_col or not quantity_col:
                logger.warning("Не найдены столбцы с комментариями или количеством")
                return self.processed_data.copy()
            
            # Копируем данные для обработки
            result_data = self.processed_data.copy()
            
            # Добавляем столбцы для обработки поставок
            result_data['количества_поставок'] = None
            result_data['срок_поставки_дней'] = None  # Срок поставки в днях
            
            # Счетчик строк с заполненными сроками поставки
            delivery_days_count = 0
            
            # Поиск столбца с датой поставки
            delivery_date_col = next((col for col in self.processed_data.columns 
                                     if 'дата поставки' in col or 'срок поставки' in col), None)
            logger.info(f"Найден столбец с датой поставки: {delivery_date_col}")
            
            # Регулярное выражение для поиска дат и количеств в комментариях
            # Например: "2 от 01.02.2025 +3 от 05.06.2025"
            date_pattern = r'(\d+(?:\.\d+)?)\s+от\s+(\d{1,2}\.\d{1,2}\.\d{4})'
            
            for idx, row in result_data.iterrows():
                quantity_str = str(row[quantity_col]) if pd.notna(row[quantity_col]) else ''
                comment_str = str(row[comment_col]) if pd.notna(row[comment_col]) else ''
                
                # Проверяем, есть ли формула в количестве
                if '+' in quantity_str or '-' in quantity_str:
                    # Ищем даты в комментариях
                    matches = re.findall(date_pattern, comment_str)
                    
                    if matches:
                        quantities = []
                        dates = []
                        
                        for match in matches:
                            qty, date_str = match
                            try:
                                qty = float(qty)
                                date = parse(date_str, dayfirst=True)
                                
                                quantities.append(qty)
                                dates.append(date)
                            except Exception:
                                pass
                        
                        if quantities and dates:
                            result_data.at[idx, 'количества_поставок'] = quantities
                            
                            # Расчет срока поставки в днях - разница между датой заказа и датой поставки
                            if dates:
                                # Получаем дату заказа
                                order_date_col = next((col for col in self.processed_data.columns if 'дата заявки' in col), None)
                                if order_date_col and pd.notna(row[order_date_col]):
                                    order_date = row[order_date_col]
                                    
                                    # Определяем дату поставки - берем самую позднюю или дату поставки большей части заказа
                                    if len(dates) > 1 and len(quantities) == len(dates):
                                        # Если есть несколько дат и количеств, ищем большую часть заказа
                                        total_qty = sum(quantities)
                                        half_qty = total_qty / 2
                                        
                                        # Сортируем по количеству в порядке убывания
                                        sorted_data = sorted(zip(dates, quantities), key=lambda x: x[1], reverse=True)
                                        delivery_date = sorted_data[0][0]  # Дата с наибольшим количеством
                                        
                                        # Проверяем, больше ли это половины заказа
                                        if sorted_data[0][1] < half_qty:
                                            # Если нет, то берем самую позднюю дату
                                            delivery_date = max(dates)
                                    else:
                                        # Если только одна дата или нет соответствия количеств и дат, берем самую позднюю дату
                                        delivery_date = max(dates)
                                    
                                    # Вычисляем разницу в днях между датой заказа и датой поставки
                                    days_diff = (delivery_date - order_date).days
                                    # Если разница отрицательная (дата поставки раньше даты заказа), устанавливаем 0
                                    days_diff = max(0, days_diff)
                                    result_data.at[idx, 'срок_поставки_дней'] = days_diff
                                    delivery_days_count += 1
                else:
                    # Если нет формулы, проверяем наличие даты поставки
                    if delivery_date_col and pd.notna(row[delivery_date_col]):
                        # Получаем дату заказа
                        order_date_col = next((col for col in self.processed_data.columns if 'дата заявки' in col), None)
                        if order_date_col and pd.notna(row[order_date_col]):
                            order_date = row[order_date_col]
                            delivery_date = row[delivery_date_col]
                            
                            # Вычисляем разницу в днях между датой заказа и датой поставки
                            days_diff = (delivery_date - order_date).days
                            # Если разница отрицательная (дата поставки раньше даты заказа), устанавливаем 0
                            days_diff = max(0, days_diff)
                            result_data.at[idx, 'срок_поставки_дней'] = days_diff
                            delivery_days_count += 1
            
            # Подсчитываем количество строк с заполненными сроками поставки
            filled_rows = result_data['срок_поставки_дней'].notna().sum()
            logger.info(f"Комментарии о поставках успешно разобраны. Заполнено сроков поставки: {filled_rows} из {len(result_data)}")
            
            # Если нет данных о сроках поставки, устанавливаем стандартный срок 30 дней
            if filled_rows == 0:
                logger.warning("Не найдено данных о сроках поставки. Устанавливаем стандартный срок 30 дней.")
                result_data['срок_поставки_дней'] = 30
            
            # Дополнительно создаем словарь сроков поставки по артикулам
            # Это позволит легче использовать сроки поставки при прогнозировании
            if 'артикул' in result_data.columns and 'срок_поставки_дней' in result_data.columns:
                # Фильтруем только строки с заполненными артикулами и сроками поставки
                valid_data = result_data[result_data['артикул'].notna() & result_data['срок_поставки_дней'].notna()]
                
                # Группируем по артикулам и вычисляем средний срок поставки
                article_delivery_times = valid_data.groupby('артикул')['срок_поставки_дней'].mean().to_dict()
                
                # Выводим информацию о сроках поставки по артикулам
                logger.info(f"Создан словарь сроков поставки по артикулам. Всего артикулов: {len(article_delivery_times)}")
                
                # Выводим первые 10 артикулов со сроками поставки
                for i, (article, days) in enumerate(list(article_delivery_times.items())[:10]):
                    logger.info(f"Артикул: {article}, срок поставки: {days:.1f} дней")
                
                # Сохраняем словарь сроков поставки в атрибуте класса
                self.article_delivery_times = article_delivery_times
            else:
                logger.warning("Не удалось создать словарь сроков поставки по артикулам")
            
            return result_data
        except Exception as e:
            logger.error(f"Ошибка при разборе комментариев о поставках: {e}")
            return None
