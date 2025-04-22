import pandas as pd
import numpy as np
import re
import math
from datetime import datetime, timedelta
import logging
import traceback
import os
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvassor
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrderAnalyzer:
    """Класс для анализа заказов и прогнозирования будущих потребностей"""
    
    def __init__(self, data_processor):
        """Инициализация анализатора заказов
        
        Args:
            data_processor: Экземпляр класса DataProcessor с обработанными данными
        """
        self.data_processor = data_processor
        self.frequency_data = None
        self.predictions = None
        self.seasonal_patterns = {}
    
    def analyze_orders(self, progress_callback=None):
        """Анализ заказов и расчет частоты
        
        Args:
            progress_callback (callable, optional): Функция обратного вызова для отображения прогресса.
                Принимает два аргумента: текущий прогресс (0-100) и сообщение о статусе.
        
        Returns:
            pd.DataFrame: Данные о частоте заказов
        """
        # Сообщаем о начале анализа
        if progress_callback:
            progress_callback(0, "Начало анализа заказов...")
        
        # Выполняем анализ с передачей функции обратного вызова
        self.frequency_data = self.data_processor.analyze_order_frequency(progress_callback)
        
        # Сообщаем о завершении анализа
        if progress_callback:
            progress_callback(100, "Анализ заказов завершен")
        
        return self.frequency_data
    
    def detect_seasonal_patterns(self):
        """Обнаружение сезонных паттернов в заказах
        
        Returns:
            dict: Словарь с сезонными паттернами для каждой группы
        """
        if self.frequency_data is None or self.frequency_data.empty:
            logger.error("Нет данных о частоте заказов")
            return {}
        
        try:
            for _, row in self.frequency_data.iterrows():
                group_id = row['group_id']
                order_dates = row['order_dates']
                
                if len(order_dates) < 4:  # Нужно минимум 4 даты для анализа сезонности
                    continue
                
                # Преобразуем даты в месяцы и считаем количество заказов по месяцам
                months = [date.month for date in order_dates]
                month_counts = {}
                for month in range(1, 13):
                    month_counts[month] = months.count(month)
                
                # Определяем месяцы с высокой активностью (выше среднего)
                avg_count = sum(month_counts.values()) / 12
                high_activity_months = [month for month, count in month_counts.items() if count > avg_count]
                
                # Определяем квартальные паттерны
                quarters = {
                    1: sum(month_counts.get(m, 0) for m in [1, 2, 3]),
                    2: sum(month_counts.get(m, 0) for m in [4, 5, 6]),
                    3: sum(month_counts.get(m, 0) for m in [7, 8, 9]),
                    4: sum(month_counts.get(m, 0) for m in [10, 11, 12])
                }
                
                # Определяем кварталы с высокой активностью
                avg_quarter = sum(quarters.values()) / 4
                high_activity_quarters = [q for q, count in quarters.items() if count > avg_quarter]
                
                self.seasonal_patterns[group_id] = {
                    'monthly': month_counts,
                    'high_activity_months': high_activity_months,
                    'quarterly': quarters,
                    'high_activity_quarters': high_activity_quarters
                }
            
            logger.info(f"Обнаружены сезонные паттерны для {len(self.seasonal_patterns)} групп элементов")
            return self.seasonal_patterns
        except Exception as e:
            logger.error(f"Ошибка при обнаружении сезонных паттернов: {e}")
            return {}
    
    def predict_future_needs(self, forecast_days=30, use_individual_lead_time=True, default_lead_time_days=30):
        """Прогнозирование будущих потребностей с учетом сезонности
        
        Args:
            forecast_days (int): Количество дней в будущее для прогноза
            use_individual_lead_time (bool): Использовать ли индивидуальные сроки поставки для каждого товара
            default_lead_time_days (int): Стандартное время выполнения заказа в днях (если не используются индивидуальные сроки)
            
        Returns:
            pd.DataFrame: Прогноз будущих потребностей
        """
        if self.frequency_data is None or self.frequency_data.empty:
            logger.error("Нет данных о частоте заказов")
            return None
        
        try:
            # Обнаруживаем сезонные паттерны, если еще не сделано
            if not self.seasonal_patterns:
                self.detect_seasonal_patterns()
            
            # Получаем базовый прогноз
            today = datetime.now()
            forecast_end = today + timedelta(days=forecast_days)
            base_predictions = self.data_processor.predict_future_orders(self.frequency_data, forecast_days)
            
            if base_predictions is None or base_predictions.empty:
                logger.error("Не удалось получить базовый прогноз")
                return None
            
            # Получаем данные о сроках поставки для каждой группы товаров
            item_lead_times = {}
            
            # Отладочная информация
            logger.info(f"Использование индивидуальных сроков поставки: {use_individual_lead_time}")
            
            if use_individual_lead_time:
                # Используем словарь сроков поставки из DataProcessor
                article_lead_times = self.data_processor.article_delivery_times
                logger.info(f"Используем словарь сроков поставки из DataProcessor. Всего артикулов: {len(article_lead_times)}")
                
                # Выводим первые 5 артикулов со сроками поставки
                for i, (article, days) in enumerate(list(article_lead_times.items())[:5]):
                    logger.info(f"Артикул: {article}, срок поставки: {days:.1f} дней")
                
                # Сопоставляем артикулы с группами товаров
                logger.info(f"Количество групп товаров: {len(self.data_processor.similar_items_map)}")
                
                # Выводим все артикулы со сроками поставки для отладки
                logger.info(f"Артикулы со сроками поставки: {list(article_lead_times.keys())[:10]}... (всего {len(article_lead_times)})")
                
                # Выводим первые несколько групп для отладки
                for i, (gid, items) in enumerate(list(self.data_processor.similar_items_map.items())[:5]):
                    logger.info(f"Group {gid}: {items}")
                    
                # Проверяем соответствие артикулов и групп
                matching_count = 0
                for gid in list(self.data_processor.similar_items_map.keys())[:20]:
                    clean_gid = gid
                    if isinstance(gid, str) and gid.startswith('art_'):
                        clean_gid = gid[4:]
                    
                    if clean_gid in article_lead_times:
                        matching_count += 1
                        logger.info(f"Найдено прямое соответствие: {clean_gid} (срок: {article_lead_times[clean_gid]})")
                
                logger.info(f"Найдено {matching_count} прямых соответствий из 20 проверенных групп")
                
                # Заполняем словарь сроков поставки для групп товаров
                for group_id, items_list in self.data_processor.similar_items_map.items():
                    # Сначала проверяем сам идентификатор группы
                    clean_group_id = group_id
                    if isinstance(group_id, str) and group_id.startswith('art_'):
                        clean_group_id = group_id[4:]
                    
                    if clean_group_id in article_lead_times:
                        # Если нашли соответствие для очищенного идентификатора группы
                        item_lead_times[group_id] = article_lead_times[clean_group_id]
                        continue
                    
                    # Ищем артикулы в группе
                    for item in items_list:
                        if isinstance(item, tuple) and len(item) > 1:
                            article = item[1]
                            if article in article_lead_times:
                                # Если нашли артикул в списке со сроками поставки, используем его срок
                                item_lead_times[group_id] = article_lead_times[article]
                                break
            else:
                logger.info("Столбец 'срок_поставки_дней' не найден")
            
            logger.info(f"Всего найдено групп с индивидуальными сроками поставки: {len(item_lead_times)}")
            
            # Корректируем прогноз с учетом сезонности и сроков поставки
            adjusted_predictions = []
            
            for _, row in base_predictions.iterrows():
                group_id = row['group_id']
                items = row['items']
                forecast_dates = row['forecast_dates']
                
                # Определяем срок поставки для группы
                lead_time = default_lead_time_days  # По умолчанию используем стандартный срок
                
                if use_individual_lead_time:
                    # Проверяем наличие индивидуального срока поставки
                    
                    # 1. Проверяем сам group_id
                    if group_id in item_lead_times:
                        # Округляем срок поставки в большую сторону (ceil)
                        lead_time = math.ceil(item_lead_times[group_id])
                        logger.info(f"Для группы {group_id} используется индивидуальный срок поставки: {lead_time} дней")
                    else:
                        # 2. Проверяем очищенный group_id (без префикса art_)
                        clean_group_id = group_id
                        if isinstance(group_id, str) and group_id.startswith('art_'):
                            clean_group_id = group_id[4:]
                        
                        if clean_group_id in article_lead_times:
                            # Округляем срок поставки в большую сторону (ceil)
                            lead_time = math.ceil(article_lead_times[clean_group_id])
                            logger.info(f"Для группы {group_id} используется срок поставки очищенного ID: {lead_time} дней")
                        else:
                            # 3. Проверяем артикулы в списке элементов
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, tuple) and len(item) > 1:
                                        article = item[1]
                                        if article in article_lead_times:
                                            # Округляем срок поставки в большую сторону (ceil)
                                            lead_time = math.ceil(article_lead_times[article])
                                            logger.info(f"Для группы {group_id} используется срок поставки артикула {article}: {lead_time} дней")
                                            break
                
                # Если не нашли индивидуальный срок, используем стандартный
                if lead_time == default_lead_time_days:
                    logger.info(f"Для группы {group_id} используется стандартный срок поставки: {lead_time} дней")
                
                # Применяем сезонные корректировки, если есть данные о сезонности
                if group_id in self.seasonal_patterns:
                    seasonal_data = self.seasonal_patterns[group_id]
                    monthly_pattern = seasonal_data['monthly']
                    
                    adjusted_forecast = []
                    
                    for forecast in forecast_dates:
                        date = forecast['date']
                        quantity = forecast['estimated_quantity']
                        
                        # Проверяем, что дата находится в пределах периода прогноза
                        if date <= forecast_end:
                            # Корректируем количество на основе месячного паттерна
                            month = date.month
                            month_factor = monthly_pattern.get(month, 0)
                            
                            if month_factor > 0:
                                # Нормализуем фактор относительно среднего
                                avg_factor = sum(monthly_pattern.values()) / 12
                                if avg_factor > 0:
                                    seasonal_factor = month_factor / avg_factor
                                    adjusted_quantity = quantity * seasonal_factor
                                else:
                                    adjusted_quantity = quantity
                            else:
                                adjusted_quantity = quantity
                            
                            # Учитываем время выполнения заказа
                            order_date = date - timedelta(days=lead_time)
                            
                            adjusted_forecast.append({
                                'forecast_date': date,
                                'order_date': order_date,
                                'estimated_quantity': round(adjusted_quantity, 2),
                                'original_quantity': round(quantity, 2)
                            })
                    
                    if adjusted_forecast:  # Добавляем только если есть прогнозы
                        adjusted_predictions.append({
                            'group_id': group_id,
                            'items': items,
                            'lead_time_days': lead_time,
                            'forecast': adjusted_forecast
                        })
                else:
                    # Если нет данных о сезонности, используем базовый прогноз
                    adjusted_forecast = []
                    
                    for forecast in forecast_dates:
                        date = forecast['date']
                        quantity = forecast['estimated_quantity']
                        
                        # Проверяем, что дата находится в пределах периода прогноза
                        if date <= forecast_end:
                            # Учитываем время выполнения заказа
                            order_date = date - timedelta(days=lead_time)
                            
                            adjusted_forecast.append({
                                'forecast_date': date,
                                'order_date': order_date,
                                'estimated_quantity': round(quantity, 2),
                                'original_quantity': round(quantity, 2)
                            })
                    
                    if adjusted_forecast:  # Добавляем только если есть прогнозы
                        adjusted_predictions.append({
                            'group_id': group_id,
                            'items': items,
                            'lead_time_days': lead_time,
                            'forecast': adjusted_forecast
                        })
            
            self.predictions = pd.DataFrame(adjusted_predictions)
            logger.info(f"Прогноз будущих потребностей создан для {len(adjusted_predictions)} групп элементов")
            return self.predictions
        except Exception as e:
            logger.error(f"Ошибка при прогнозировании будущих потребностей: {e}")
            return None
    
    def generate_order_recommendations(self, days_ahead=90):
        """Генерация рекомендаций по заказам на ближайший период
        
        Args:
            days_ahead (int): Количество дней вперед для рекомендаций
            
        Returns:
            pd.DataFrame: Рекомендации по заказам
        """
        if self.predictions is None or self.predictions.empty:
            logger.error("Нет прогнозов для генерации рекомендаций")
            return None
        
        try:
            today = datetime.now()
            end_date = today + timedelta(days=days_ahead)
            
            recommendations = []
            
            # Получаем данные о заказах для проверки целых/дробных количеств
            quantity_history = {}
            
            # Проверяем, есть ли у нас доступ к данным о заказах
            if hasattr(self.data_processor, 'processed_data') and self.data_processor.processed_data is not None:
                # Ищем столбец с количеством
                quantity_col = next((col for col in self.data_processor.processed_data.columns if 'числовое' in col), None)
                
                if quantity_col:
                    # Для каждой группы товаров проверяем, были ли заказы только в целых количествах
                    for _, row in self.predictions.iterrows():
                        group_id = row['group_id']
                        items = row['items']
                        
                        # Получаем все заказы для элементов этой группы
                        group_orders = []
                        
                        for item in items:
                            if isinstance(item, tuple) and len(item) >= 2:
                                # Получаем наименование и артикул
                                name, code = item[0], item[1]
                                
                                # Ищем столбцы с наименованием и артикулом
                                name_col = next((col for col in self.data_processor.processed_data.columns if 'наименование' in col and 'норм' not in col), None)
                                article_col = next((col for col in self.data_processor.processed_data.columns if 'артикул' in col and 'норм' not in col), None)
                                
                                if name_col and article_col:
                                    # Фильтруем заказы для этого товара
                                    mask = (self.data_processor.processed_data[name_col] == name) & (self.data_processor.processed_data[article_col] == code)
                                    item_orders = self.data_processor.processed_data.loc[mask, quantity_col].tolist()
                                    group_orders.extend(item_orders)
                        
                        # Проверяем, все ли заказы были в целых количествах
                        all_integer = all(float(qty).is_integer() for qty in group_orders if pd.notna(qty))
                        quantity_history[group_id] = {
                            'all_integer': all_integer,
                            'orders': group_orders
                        }
                        
                        logger.debug(f"Группа {group_id}: всего заказов - {len(group_orders)}, все целые - {all_integer}")
            
            for _, row in self.predictions.iterrows():
                group_id = row['group_id']
                items = row['items']
                forecasts = row['forecast']
                
                # Фильтруем прогнозы на указанный период
                upcoming_orders = [f for f in forecasts if today <= f['order_date'] <= end_date]
                
                if upcoming_orders:
                    # Выбираем первый элемент из группы похожих для отображения
                    representative_item = items[0] if isinstance(items, list) and items else "Неизвестный элемент"
                    
                    for order in upcoming_orders:
                        # Получаем прогнозируемое количество
                        quantity = order['estimated_quantity']
                        
                        # Проверяем, нужно ли округлять до целого числа
                        if group_id in quantity_history and quantity_history[group_id]['all_integer'] and len(quantity_history[group_id]['orders']) > 0:
                            # Если все предыдущие заказы были в целых количествах, округляем до ближайшего целого
                            quantity = round(quantity)
                            logger.debug(f"Округлено количество для группы {group_id}: {order['estimated_quantity']} -> {quantity}")
                        
                        recommendations.append({
                            'group_id': group_id,
                            'item': representative_item,
                            'similar_items': items,
                            'order_date': order['order_date'],
                            'forecast_date': order['forecast_date'],
                            'quantity': quantity
                        })
            
            # Сортируем рекомендации по дате заказа
            recommendations_df = pd.DataFrame(recommendations)
            if not recommendations_df.empty:
                recommendations_df = recommendations_df.sort_values(by='order_date')
            
            logger.info(f"Сгенерировано {len(recommendations)} рекомендаций по заказам")
            return recommendations_df
        except Exception as e:
            logger.error(f"Ошибка при генерации рекомендаций по заказам: {e}")
            return None
    
    def plot_order_history(self, group_id):
        """Построение графика истории заказов для группы элементов
        
        Args:
            group_id (str): Идентификатор группы элементов
            
        Returns:
            matplotlib.figure.Figure: Объект фигуры с графиком
        """
        if self.frequency_data is None or self.frequency_data.empty:
            logger.error("Нет данных о частоте заказов")
            return None
        
        try:
            # Находим данные для указанной группы
            group_data = self.frequency_data[self.frequency_data['group_id'] == group_id]
            
            if group_data.empty:
                logger.error(f"Группа {group_id} не найдена в данных")
                return None
            
            # Получаем даты заказов и интервалы
            order_dates = group_data.iloc[0]['order_dates']
            intervals = group_data.iloc[0]['order_intervals']
            
            if not order_dates or len(order_dates) < 2:
                logger.error(f"Недостаточно данных для построения графика для группы {group_id}")
                return None
            
            # Создаем график
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
            
            # График истории заказов
            ax1.plot(order_dates, [1] * len(order_dates), 'o-', color='blue')
            ax1.set_title('История заказов')
            ax1.set_xlabel('Дата')
            ax1.set_ylabel('Заказ')
            ax1.grid(True)
            
            # График интервалов между заказами
            ax2.bar(range(len(intervals)), intervals, color='green')
            ax2.set_title('Интервалы между заказами (дни)')
            ax2.set_xlabel('Номер интервала')
            ax2.set_ylabel('Дни')
            ax2.grid(True)
            
            plt.tight_layout()
            
            return fig
        except Exception as e:
            logger.error(f"Ошибка при построении графика истории заказов: {e}")
            return None
    
    def plot_seasonal_patterns(self, group_id):
        """Построение графика сезонных паттернов для группы элементов
        
        Args:
            group_id (str): Идентификатор группы элементов
            
        Returns:
            matplotlib.figure.Figure: Объект фигуры с графиком
        """
        if not self.seasonal_patterns or group_id not in self.seasonal_patterns:
            logger.error(f"Нет данных о сезонных паттернах для группы {group_id}")
            return None
        
        try:
            # Получаем данные о сезонности
            seasonal_data = self.seasonal_patterns[group_id]
            monthly_pattern = seasonal_data['monthly']
            quarterly_pattern = seasonal_data['quarterly']
            
            # Создаем график
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
            
            # График месячных паттернов
            months = list(range(1, 13))
            values = [monthly_pattern.get(m, 0) for m in months]
            
            ax1.bar(months, values, color='blue')
            ax1.set_title('Месячные паттерны заказов')
            ax1.set_xlabel('Месяц')
            ax1.set_ylabel('Количество заказов')
            ax1.set_xticks(months)
            ax1.set_xticklabels(['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'])
            ax1.grid(True)
            
            # График квартальных паттернов
            quarters = list(range(1, 5))
            values = [quarterly_pattern.get(q, 0) for q in quarters]
            
            ax2.bar(quarters, values, color='green')
            ax2.set_title('Квартальные паттерны заказов')
            ax2.set_xlabel('Квартал')
            ax2.set_ylabel('Количество заказов')
            ax2.set_xticks(quarters)
            ax2.set_xticklabels(['Q1', 'Q2', 'Q3', 'Q4'])
            ax2.grid(True)
            
            plt.tight_layout()
            
            return fig
        except Exception as e:
            logger.error(f"Ошибка при построении графика сезонных паттернов: {e}")
            return None
    
    def export_recommendations_to_excel(self, recommendations, file_path):
        """Экспорт рекомендаций в Excel-файл
        
        Args:
            recommendations (pd.DataFrame): Рекомендации по заказам
            file_path (str): Путь для сохранения файла
            
        Returns:
            bool: Успешность экспорта
        """
        if recommendations is None or recommendations.empty:
            logger.error("Нет рекомендаций для экспорта")
            return False
        
        try:
            # Подготовка данных для экспорта
            export_data = recommendations.copy()
            
            # Форматирование данных
            if 'similar_items' in export_data.columns:
                export_data['similar_items'] = export_data['similar_items'].apply(
                    lambda x: ', '.join(str(item) for item in x) if isinstance(x, list) else str(x))
            
            # Переименование столбцов для удобства
            column_mapping = {
                'group_id': 'ID группы',
                'item': 'Наименование',
                'similar_items': 'Похожие наименования',
                'order_date': 'Дата заказа',
                'forecast_date': 'Прогнозируемая дата потребности',
                'quantity': 'Рекомендуемое количество'
            }
            
            export_data = export_data.rename(columns=column_mapping)
            
            # Сохранение в Excel
            export_data.to_excel(file_path, index=False)
            
            logger.info(f"Рекомендации успешно экспортированы в {file_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при экспорте рекомендаций: {e}")
            return False
