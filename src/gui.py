import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import os
import logging
import threading
from datetime import datetime

# Импортируем наши модули
from data_processor import DataProcessor
from analyzer import OrderAnalyzer
from item_mapping import ItemMapping
from mapping_editor import MappingEditorDialog, ProgressDialog

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MainApplication(tk.Tk):
    """Главное окно приложения"""
    
    def __init__(self):
        """Инициализация главного окна"""
        super().__init__()
        
        # Настройка основного окна
        self.title("Система анализа и прогнозирования заявок")
        self.geometry("1200x800")
        self.minsize(800, 600)
        
        # Инициализация обработчика данных, анализатора и базы соответствий
        self.item_mapping = ItemMapping()
        self.data_processor = DataProcessor()
        self.analyzer = OrderAnalyzer(self.data_processor)
        
        # Передаем базу соответствий в обработчик данных
        self.data_processor.set_item_mapping(self.item_mapping)
        
        # Текущий загруженный файл
        self.current_file = None
        
        # Создание интерфейса
        self._create_menu()
        self._create_main_frame()
        
        # Статус
        self.status_var = tk.StringVar()
        self.status_var.set("Готов к работе")
        self.status_bar = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Центрирование окна
        self._center_window()
    
    def _center_window(self):
        """Центрирование окна на экране"""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry('{}x{}+{}+{}'.format(width, height, x, y))
    
    def _create_menu(self):
        """Создание главного меню"""
        self.menu_bar = tk.Menu(self)
        
        # Меню "Файл"
        file_menu = tk.Menu(self.menu_bar, tearoff=0)
        file_menu.add_command(label="Открыть файл данных", command=self._open_file)
        file_menu.add_separator()
        file_menu.add_command(label="Выход", command=self.quit)
        self.menu_bar.add_cascade(label="Файл", menu=file_menu)
        
        # Меню "Анализ"
        analysis_menu = tk.Menu(self.menu_bar, tearoff=0)
        analysis_menu.add_command(label="Анализировать данные", command=self._analyze_data)
        analysis_menu.add_command(label="Прогнозировать потребности", command=self._predict_needs)
        analysis_menu.add_command(label="Сформировать рекомендации", command=self._generate_recommendations)
        analysis_menu.add_separator()
        analysis_menu.add_command(label="Редактор базы соответствий", command=self._open_mapping_editor)
        self.menu_bar.add_cascade(label="Анализ потребления", menu=analysis_menu)
        
        # Меню "Экспорт"
        export_menu = tk.Menu(self.menu_bar, tearoff=0)
        export_menu.add_command(label="Экспорт рекомендаций в Excel", command=self._export_recommendations)
        self.menu_bar.add_cascade(label="Экспорт", menu=export_menu)
        
        # Меню "Справка"
        help_menu = tk.Menu(self.menu_bar, tearoff=0)
        help_menu.add_command(label="О программе", command=self._show_about)
        self.menu_bar.add_cascade(label="Справка", menu=help_menu)
        
        self.config(menu=self.menu_bar)
    
    def _create_main_frame(self):
        """Создание основного фрейма с вкладками"""
        self.notebook = ttk.Notebook(self)
        
        # Вкладка "Данные"
        self.data_frame = ttk.Frame(self.notebook)
        self._create_data_tab(self.data_frame)
        self.notebook.add(self.data_frame, text="Данные")
        
        # Вкладка "Анализ"
        self.analysis_frame = ttk.Frame(self.notebook)
        self._create_analysis_tab(self.analysis_frame)
        self.notebook.add(self.analysis_frame, text="Анализ потребления")
        
        # Вкладка "Прогноз"
        self.forecast_frame = ttk.Frame(self.notebook)
        self._create_forecast_tab(self.forecast_frame)
        self.notebook.add(self.forecast_frame, text="Прогноз")
        
        # Вкладка "Рекомендации"
        self.recommendations_frame = ttk.Frame(self.notebook)
        self._create_recommendations_tab(self.recommendations_frame)
        self.notebook.add(self.recommendations_frame, text="Рекомендации")
        
        self.notebook.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)
    
    def _create_data_tab(self, parent):
        """Создание вкладки 'Данные'"""
        # Фрейм для кнопок
        button_frame = ttk.Frame(parent)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Кнопки
        ttk.Button(button_frame, text="Открыть файл", command=self._open_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Обработать данные", command=self._process_data).pack(side=tk.LEFT, padx=5)
        
        # Информация о файле
        info_frame = ttk.LabelFrame(parent, text="Информация о файле")
        info_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.file_info_var = tk.StringVar()
        self.file_info_var.set("Файл не загружен")
        ttk.Label(info_frame, textvariable=self.file_info_var).pack(padx=5, pady=5)
        
        # Таблица данных
        data_frame = ttk.LabelFrame(parent, text="Данные")
        data_frame.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)
        
        # Создаем фрейм с прокруткой для таблицы
        table_frame = ttk.Frame(data_frame)
        table_frame.pack(expand=True, fill=tk.BOTH)
        
        # Полосы прокрутки
        x_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        y_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL)
        
        # Таблица (Treeview)
        self.data_table = ttk.Treeview(table_frame, 
                                       xscrollcommand=x_scrollbar.set,
                                       yscrollcommand=y_scrollbar.set)
        
        # Настройка полос прокрутки
        x_scrollbar.config(command=self.data_table.xview)
        y_scrollbar.config(command=self.data_table.yview)
        
        # Размещение элементов
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.data_table.pack(expand=True, fill=tk.BOTH)
    
    def _create_analysis_tab(self, parent):
        """Создание вкладки 'Анализ'"""
        # Фрейм для кнопок
        button_frame = ttk.Frame(parent)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Кнопки
        ttk.Button(button_frame, text="Анализировать данные", command=self._analyze_data).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Обнаружить сезонные паттерны", command=self._detect_seasonal_patterns).pack(side=tk.LEFT, padx=5)
        
        # Фрейм для выбора группы
        group_frame = ttk.LabelFrame(parent, text="Выбор группы для анализа")
        group_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(group_frame, text="Группа:").pack(side=tk.LEFT, padx=5, pady=5)
        self.group_var = tk.StringVar()
        self.group_combo = ttk.Combobox(group_frame, textvariable=self.group_var, state="readonly")
        self.group_combo.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5, pady=5)
        self.group_combo.bind("<<ComboboxSelected>>", self._on_group_selected)
        
        ttk.Button(group_frame, text="Показать график", command=self._show_analysis_chart).pack(side=tk.LEFT, padx=5, pady=5)
        
        # Фрейм для результатов анализа
        results_frame = ttk.LabelFrame(parent, text="Результаты анализа")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Создаем фрейм с прокруткой для таблицы
        table_frame = ttk.Frame(results_frame)
        table_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Полосы прокрутки
        x_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        y_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL)
        
        # Таблица (Treeview)
        self.analysis_table = ttk.Treeview(table_frame, 
                                          xscrollcommand=x_scrollbar.set,
                                          yscrollcommand=y_scrollbar.set)
        
        # Настройка полос прокрутки
        x_scrollbar.config(command=self.analysis_table.xview)
        y_scrollbar.config(command=self.analysis_table.yview)
        
        # Размещение элементов
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.analysis_table.pack(expand=True, fill=tk.BOTH)
        
        # Фрейм для графика
        self.chart_frame = ttk.Frame(results_frame)
        self.chart_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
    
    def _create_forecast_tab(self, parent):
        """Создание вкладки 'Прогноз'"""
        # Фрейм для параметров прогноза
        params_frame = ttk.LabelFrame(parent, text="Параметры прогноза")
        params_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Период прогноза
        ttk.Label(params_frame, text="Прогноз на (дней):").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.forecast_days_var = tk.IntVar(value=90)  # Значение по умолчанию - 90 дней
        ttk.Spinbox(params_frame, from_=7, to=365, textvariable=self.forecast_days_var, width=10).grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Скрытые переменные для совместимости
        self.use_individual_lead_time_var = tk.BooleanVar(value=True)  # Всегда используем индивидуальные сроки
        self.lead_time_var = tk.IntVar(value=30)  # Стандартный срок поставки
        
        # Кнопка прогноза
        ttk.Button(params_frame, text="Сформировать прогноз", command=self._predict_needs).grid(row=0, column=2, padx=5, pady=5)
        
        # Фрейм для выбора группы
        group_frame = ttk.LabelFrame(parent, text="Выбор группы для просмотра")
        group_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(group_frame, text="Группа:").pack(side=tk.LEFT, padx=5, pady=5)
        self.forecast_group_var = tk.StringVar()
        self.forecast_group_combo = ttk.Combobox(group_frame, textvariable=self.forecast_group_var, state="readonly")
        self.forecast_group_combo.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5, pady=5)
        self.forecast_group_combo.bind("<<ComboboxSelected>>", self._on_forecast_group_selected)
        
        ttk.Button(group_frame, text="Показать график", command=self._show_forecast_chart).pack(side=tk.LEFT, padx=5, pady=5)
        
        # Фрейм для результатов прогноза
        results_frame = ttk.LabelFrame(parent, text="Результаты прогноза")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Создаем фрейм с прокруткой для таблицы
        table_frame = ttk.Frame(results_frame)
        table_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Полосы прокрутки
        x_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        y_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL)
        
        # Таблица (Treeview)
        self.forecast_table = ttk.Treeview(table_frame, 
                                          xscrollcommand=x_scrollbar.set,
                                          yscrollcommand=y_scrollbar.set)
        
        # Настройка полос прокрутки
        x_scrollbar.config(command=self.forecast_table.xview)
        y_scrollbar.config(command=self.forecast_table.yview)
        
        # Размещение элементов
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.forecast_table.pack(expand=True, fill=tk.BOTH)
        
        # Фрейм для графика
        self.forecast_chart_frame = ttk.Frame(results_frame)
        self.forecast_chart_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
    
    def _create_recommendations_tab(self, parent):
        """Создание вкладки 'Рекомендации'"""
        # Фрейм для параметров рекомендаций
        params_frame = ttk.LabelFrame(parent, text="Параметры рекомендаций")
        params_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Период рекомендаций
        ttk.Label(params_frame, text="Период рекомендаций (дней):").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.recommendations_days_var = tk.IntVar(value=30)  # Значение по умолчанию - 30 дней
        ttk.Spinbox(params_frame, from_=7, to=365, textvariable=self.recommendations_days_var, width=10).grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Кнопка формирования рекомендаций
        ttk.Button(params_frame, text="Сформировать рекомендации", command=self._generate_recommendations).grid(row=0, column=2, padx=5, pady=5)
        
        # Кнопка экспорта
        ttk.Button(params_frame, text="Экспорт в Excel", command=self._export_recommendations).grid(row=0, column=3, padx=5, pady=5)
        
        # Фрейм для результатов рекомендаций
        results_frame = ttk.LabelFrame(parent, text="Рекомендации по заказам")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Создаем фрейм с прокруткой для таблицы
        table_frame = ttk.Frame(results_frame)
        table_frame.pack(fill=tk.BOTH, expand=True)
        
        # Полосы прокрутки
        x_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        y_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL)
        
        # Таблица (Treeview)
        self.recommendations_table = ttk.Treeview(table_frame, 
                                                 xscrollcommand=x_scrollbar.set,
                                                 yscrollcommand=y_scrollbar.set)
        
        # Настройка полос прокрутки
        x_scrollbar.config(command=self.recommendations_table.xview)
        y_scrollbar.config(command=self.recommendations_table.yview)
        
        # Размещение элементов
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.recommendations_table.pack(expand=True, fill=tk.BOTH)
    
    def _open_file(self):
        """Открытие файла данных"""
        file_path = filedialog.askopenfilename(
            title="Выберите файл данных",
            filetypes=[("Excel файлы", "*.xlsx *.xls"), ("Все файлы", "*.*")]
        )
        
        if not file_path:
            return
        
        self.status_var.set("Загрузка файла...")
        self.update_idletasks()
        
        if self.data_processor.load_data(file_path):
            self.current_file = file_path
            self.file_info_var.set(f"Файл: {os.path.basename(file_path)}\nПуть: {file_path}")
            self._display_data(self.data_processor.data)
            self.status_var.set(f"Файл {os.path.basename(file_path)} успешно загружен")
        else:
            messagebox.showerror("Ошибка", "Не удалось загрузить файл данных")
            self.status_var.set("Ошибка загрузки файла")
    
    def _process_data(self):
        """Обработка загруженных данных"""
        if self.data_processor.data is None:
            messagebox.showwarning("Предупреждение", "Сначала загрузите файл данных")
            return
        
        self.status_var.set("Обработка данных...")
        self.update_idletasks()
        
        if self.data_processor.preprocess_data():
            # Поиск похожих элементов
            self.data_processor.find_similar_items()
            
            # Разбор комментариев о поставках - явно вызываем и сохраняем результат
            delivery_data = self.data_processor.parse_delivery_comments()
            
            # Сохраняем результат в processed_data, если он успешно получен
            if delivery_data is not None:
                self.data_processor.processed_data = delivery_data
                
                # Проверяем, есть ли данные о сроках поставки
                has_delivery_days = 'срок_поставки_дней' in delivery_data.columns
                filled_delivery_days = 0
                if has_delivery_days:
                    filled_delivery_days = delivery_data['срок_поставки_дней'].notna().sum()
                
                self.status_var.set(f"Данные обработаны. Сроки поставки: {filled_delivery_days} из {len(delivery_data)}")
                
                # Фильтруем столбцы, оставляя только нужные
                display_cols = [col for col in delivery_data.columns 
                               if 'числовое' in col or 'норм' in col or 
                               'дата' in col or 'срок' in col or
                               col in ['наименование товара', 'артикул', '№ заявки', '№ поз.', 'срок_поставки_дней']]
                
                filtered_data = delivery_data[display_cols]
                self._display_data(filtered_data)
            else:
                # Фильтруем столбцы, оставляя только нужные
                display_cols = [col for col in self.data_processor.processed_data.columns 
                               if 'числовое' in col or 'норм' in col or 
                               'дата' in col or 'срок' in col or
                               col in ['наименование товара', 'артикул', '№ заявки', '№ поз.', 'срок_поставки_дней']]
                
                filtered_data = self.data_processor.processed_data[display_cols]
                self._display_data(filtered_data)
            
            self.status_var.set("Данные успешно обработаны")
            messagebox.showinfo("Информация", "Данные успешно обработаны")
        else:
            messagebox.showerror("Ошибка", "Не удалось обработать данные")
            self.status_var.set("Ошибка обработки данных")
    
    def _analyze_data(self):
        """Анализ данных в отдельном потоке с отображением прогресса"""
        if self.data_processor.processed_data is None:
            messagebox.showwarning("Предупреждение", "Сначала обработайте данные")
            return
        
        self.status_var.set("Анализ данных...")
        self.update_idletasks()
        
        # Создаем диалог прогресса
        progress_dialog = ProgressDialog(self, "Анализ данных", "Выполняется анализ данных...")
        
        # Функция для выполнения анализа в отдельном потоке
        def analyze_thread_func():
            # Функция обратного вызова для обновления прогресса
            def progress_callback(value, status_text):
                # Обновляем прогресс в основном потоке
                self.after(10, lambda: progress_dialog.update_progress(value, status_text))
            
            # Выполняем анализ с функцией обратного вызова
            frequency_data = self.analyzer.analyze_orders(progress_callback)
            
            # Функция для обновления интерфейса в основном потоке
            def update_ui():
                # Закрываем диалог прогресса
                progress_dialog.destroy()
                
                if frequency_data is not None and not frequency_data.empty:
                    # Отображение результатов анализа
                    self._display_analysis_results(frequency_data)
                    
                    # Обновление списка групп
                    self._update_group_lists(frequency_data)
                    
                    self.status_var.set("Анализ данных успешно завершен")
                    messagebox.showinfo("Информация", "Анализ данных успешно завершен")
                else:
                    messagebox.showerror("Ошибка", "Не удалось выполнить анализ данных")
                    self.status_var.set("Ошибка анализа данных")
            
            # Вызываем обновление интерфейса в основном потоке
            self.after(100, update_ui)
        
        # Запускаем поток для анализа
        analysis_thread = threading.Thread(target=analyze_thread_func)
        analysis_thread.daemon = True  # Делаем поток демоном, чтобы он завершился при закрытии приложения
        analysis_thread.start()
    
    def _detect_seasonal_patterns(self):
        """Обнаружение сезонных паттернов"""
        if self.analyzer.frequency_data is None:
            messagebox.showwarning("Предупреждение", "Сначала выполните анализ данных")
            return
        
        self.status_var.set("Обнаружение сезонных паттернов...")
        self.update_idletasks()
        
        seasonal_patterns = self.analyzer.detect_seasonal_patterns()
        
        if seasonal_patterns:
            self.status_var.set("Сезонные паттерны успешно обнаружены")
            messagebox.showinfo("Информация", f"Обнаружены сезонные паттерны для {len(seasonal_patterns)} групп элементов")
        else:
            messagebox.showwarning("Предупреждение", "Не удалось обнаружить сезонные паттерны")
            self.status_var.set("Не удалось обнаружить сезонные паттерны")
    
    def _predict_needs(self):
        """Прогнозирование будущих потребностей"""
        if self.analyzer.frequency_data is None:
            messagebox.showwarning("Предупреждение", "Сначала выполните анализ данных")
            return
        
        self.status_var.set("Прогнозирование потребностей...")
        self.update_idletasks()
        
        # Получаем параметры прогноза
        forecast_days = self.forecast_days_var.get()
        default_lead_time = self.lead_time_var.get()  # Стандартный срок поставки (если нет индивидуальных)
        
        # Проверяем, есть ли данные о сроках поставки в обработанных данных
        has_delivery_data = False
        if self.data_processor.processed_data is not None and 'срок_поставки_дней' in self.data_processor.processed_data.columns:
            has_delivery_data = True
            # Проверяем, есть ли заполненные значения
            filled_delivery_days = self.data_processor.processed_data['срок_поставки_дней'].notna().sum()
            if filled_delivery_days == 0:
                has_delivery_data = False
        
        # Если нет данных о сроках поставки, предупреждаем
        if not has_delivery_data:
            messagebox.showinfo("Информация", 
                              "Нет данных о сроках поставки. Будет использован стандартный срок поставки - {default_lead_time} дней.")
        
        # Вызываем метод прогнозирования с новыми параметрами
        predictions = self.analyzer.predict_future_needs(
            forecast_days=forecast_days, 
            use_individual_lead_time=True,  # Всегда пытаемся использовать индивидуальные сроки
            default_lead_time_days=default_lead_time
        )
        
        if predictions is not None and not predictions.empty:
            # Отображение результатов прогноза
            self._display_forecast_results(predictions)
            
            self.status_var.set("Прогноз потребностей успешно создан")
            messagebox.showinfo("Информация", "Прогноз потребностей успешно создан")
        else:
            messagebox.showerror("Ошибка", "Не удалось создать прогноз потребностей")
            self.status_var.set("Ошибка прогнозирования потребностей")
    
    def _generate_recommendations(self):
        """Генерация рекомендаций по заказам"""
        if self.analyzer.predictions is None:
            messagebox.showwarning("Предупреждение", "Сначала создайте прогноз потребностей")
            return
        
        self.status_var.set("Формирование рекомендаций...")
        self.update_idletasks()
        
        days_ahead = self.recommendations_days_var.get()
        
        recommendations = self.analyzer.generate_order_recommendations(days_ahead)
        
        if recommendations is not None and not recommendations.empty:
            # Отображение рекомендаций
            self._display_recommendations(recommendations)
            
            self.status_var.set("Рекомендации успешно сформированы")
            messagebox.showinfo("Информация", f"Сформировано {len(recommendations)} рекомендаций по заказам")
        else:
            messagebox.showwarning("Предупреждение", "Не удалось сформировать рекомендации")
            self.status_var.set("Не удалось сформировать рекомендации")
    
    def _export_recommendations(self):
        """Экспорт рекомендаций в Excel"""
        if not hasattr(self.analyzer, 'recommendations') or self.analyzer.recommendations is None:
            messagebox.showwarning("Предупреждение", "Сначала сформируйте рекомендации")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Сохранить рекомендации",
            defaultextension=".xlsx",
            filetypes=[("Excel файлы", "*.xlsx"), ("Все файлы", "*.*")],
            initialfile=f"Рекомендации_{datetime.now().strftime('%Y%m%d')}.xlsx"
        )
        
        if not file_path:
            return
        
        self.status_var.set("Экспорт рекомендаций...")
        self.update_idletasks()
        
        if self.analyzer.export_recommendations_to_excel(self.analyzer.recommendations, file_path):
            self.status_var.set(f"Рекомендации экспортированы в {os.path.basename(file_path)}")
            messagebox.showinfo("Информация", f"Рекомендации успешно экспортированы в {file_path}")
        else:
            messagebox.showerror("Ошибка", "Не удалось экспортировать рекомендации")
            self.status_var.set("Ошибка экспорта рекомендаций")
    
    def _on_group_selected(self, event):
        """Обработчик выбора группы в комбобоксе"""
        selected_group = self.group_var.get()
        if selected_group:
            self._show_analysis_chart()
    
    def _on_forecast_group_selected(self, event):
        """Обработчик выбора группы в комбобоксе прогноза"""
        selected_group = self.forecast_group_var.get()
        if selected_group:
            self._show_forecast_chart()
    
    def _show_analysis_chart(self):
        """Отображение графика анализа для выбранной группы"""
        selected_group = self.group_var.get()
        if not selected_group:
            return
        
        # Очищаем фрейм графика
        for widget in self.chart_frame.winfo_children():
            widget.destroy()
        
        # Получаем график истории заказов
        fig = self.analyzer.plot_order_history(selected_group)
        
        if fig:
            # Создаем холст для отображения графика
            canvas = FigureCanvasTkAgg(fig, self.chart_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        else:
            ttk.Label(self.chart_frame, text="Не удалось построить график").pack(padx=10, pady=10)
    
    def _show_forecast_chart(self):
        """Отображение графика прогноза для выбранной группы"""
        selected_group = self.forecast_group_var.get()
        if not selected_group:
            return
        
        # Очищаем фрейм графика
        for widget in self.forecast_chart_frame.winfo_children():
            widget.destroy()
        
        # Получаем график сезонных паттернов
        fig = self.analyzer.plot_seasonal_patterns(selected_group)
        
        if fig:
            # Создаем холст для отображения графика
            canvas = FigureCanvasTkAgg(fig, self.forecast_chart_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        else:
            ttk.Label(self.forecast_chart_frame, text="Не удалось построить график").pack(padx=10, pady=10)
    
    def _display_data(self, data):
        """Отображение данных в таблице
        
        Args:
            data (pd.DataFrame): Данные для отображения
        """
        if data is None or data.empty:
            return
        
        # Очищаем таблицу
        for item in self.data_table.get_children():
            self.data_table.delete(item)
        
        # Настраиваем столбцы
        self.data_table['columns'] = list(data.columns)
        self.data_table['show'] = 'headings'
        
        for col in data.columns:
            self.data_table.heading(col, text=col)
            self.data_table.column(col, width=100)
        
        # Добавляем данные (все строки без ограничения)
        # Используем пакетную вставку для ускорения
        batch_size = 1000  # Размер пакета для обработки
        total_rows = len(data)
        
        self.status_var.set(f"Загрузка данных в таблицу (0/{total_rows})...")
        self.update_idletasks()
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch = data.iloc[start_idx:end_idx]
            
            for _, row in batch.iterrows():
                values = []
                for col in data.columns:
                    value = row[col]
                    if pd.isna(value):
                        values.append('')
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        # Форматируем дату без времени
                        values.append(value.strftime('%d.%m.%Y'))
                    elif isinstance(value, (list, tuple)):
                        values.append(str(value))
                    else:
                        values.append(str(value))
                self.data_table.insert('', 'end', values=values)
            
            # Обновляем статус загрузки
            self.status_var.set(f"Загрузка данных в таблицу ({end_idx}/{total_rows})...")
            self.update_idletasks()
        
        self.status_var.set(f"Загружено {total_rows} строк")
    
    def _display_analysis_results(self, data):
        """Отображение результатов анализа в таблице
        
        Args:
            data (pd.DataFrame): Результаты анализа
        """
        if data is None or data.empty:
            return
        
        # Очищаем таблицу
        for item in self.analysis_table.get_children():
            self.analysis_table.delete(item)
        
        # Настраиваем столбцы
        columns = ['group_id', 'avg_interval_days', 'median_interval_days', 'min_interval_days', 
                  'max_interval_days', 'total_ordered', 'daily_consumption']
        
        self.analysis_table['columns'] = columns
        self.analysis_table['show'] = 'headings'
        
        column_names = {
            'group_id': 'ID группы',
            'avg_interval_days': 'Средний интервал (дни)',
            'median_interval_days': 'Медианный интервал (дни)',
            'min_interval_days': 'Мин. интервал (дни)',
            'max_interval_days': 'Макс. интервал (дни)',
            'total_ordered': 'Всего заказано',
            'daily_consumption': 'Дневное потребление'
        }
        
        for col in columns:
            self.analysis_table.heading(col, text=column_names.get(col, col))
            self.analysis_table.column(col, width=100)
        
        # Добавляем данные
        for _, row in data.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if pd.isna(value):
                    values.append('')
                elif isinstance(value, (datetime, pd.Timestamp)):
                    # Форматируем дату без времени
                    values.append(value.strftime('%d.%m.%Y'))
                elif isinstance(value, (float, np.float64)):
                    values.append(f"{value:.2f}")
                elif isinstance(value, (list, tuple)):
                    values.append(str(value))
                else:
                    values.append(str(value))
            self.analysis_table.insert('', 'end', values=values)
    
    def _display_forecast_results(self, data):
        """Отображение результатов прогноза в таблице
        
        Args:
            data (pd.DataFrame): Результаты прогноза
        """
        if data is None or data.empty:
            return
        
        # Очищаем таблицу
        for item in self.forecast_table.get_children():
            self.forecast_table.delete(item)
        
        # Настраиваем столбцы
        columns = ['group_id', 'items', 'lead_time_days', 'next_forecast']
        
        self.forecast_table['columns'] = columns
        self.forecast_table['show'] = 'headings'
        
        column_names = {
            'group_id': 'ID группы',
            'items': 'Элементы',
            'lead_time_days': 'Срок поставки (дни)',
            'next_forecast': 'Следующая потребность'
        }
        
        for col in columns:
            self.forecast_table.heading(col, text=column_names.get(col, col))
            self.forecast_table.column(col, width=100)
        
        # Добавляем данные
        for _, row in data.iterrows():
            values = []
            
            # ID группы
            values.append(str(row['group_id']))
            
            # Элементы (берем первый элемент из списка для отображения)
            items = row['items']
            if isinstance(items, list) and len(items) > 0:
                values.append(str(items[0]))
            else:
                values.append(str(items))
            
            # Срок поставки
            values.append(str(row['lead_time_days']))
            
            # Следующая потребность (берем первый прогноз из списка)
            forecast = row['forecast']
            if isinstance(forecast, list) and len(forecast) > 0:
                next_forecast = forecast[0]
                forecast_date = next_forecast.get('forecast_date')
                if isinstance(forecast_date, (datetime, pd.Timestamp)):
                    values.append(forecast_date.strftime('%d.%m.%Y'))
                else:
                    values.append(str(forecast_date))
            else:
                values.append('')
            
            self.forecast_table.insert('', 'end', values=values)
    
    def _display_recommendations(self, data):
        """Отображение рекомендаций в таблице
        
        Args:
            data (pd.DataFrame): Рекомендации
        """
        if data is None or data.empty:
            return
        
        # Сохраняем рекомендации для возможного экспорта
        self.analyzer.recommendations = data
        
        # Очищаем таблицу
        for item in self.recommendations_table.get_children():
            self.recommendations_table.delete(item)
        
        # Настраиваем столбцы
        columns = ['group_id', 'item', 'order_date', 'forecast_date', 'quantity']
        
        self.recommendations_table['columns'] = columns
        self.recommendations_table['show'] = 'headings'
        
        column_names = {
            'group_id': 'ID группы',
            'item': 'Наименование',
            'order_date': 'Дата заказа',
            'forecast_date': 'Дата потребности',
            'quantity': 'Количество'
        }
        
        for col in columns:
            self.recommendations_table.heading(col, text=column_names.get(col, col))
            self.recommendations_table.column(col, width=100)
        
        # Добавляем данные
        for _, row in data.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if pd.isna(value):
                    values.append('')
                elif isinstance(value, (datetime, pd.Timestamp)):
                    # Форматируем дату без времени
                    values.append(value.strftime('%d.%m.%Y'))
                elif isinstance(value, (float, np.float64)):
                    values.append(f"{value:.2f}")
                elif isinstance(value, (list, tuple)):
                    values.append(str(value))
                else:
                    values.append(str(value))
            self.recommendations_table.insert('', 'end', values=values)
    
    def _update_group_lists(self, data):
        """Обновление списков групп в комбобоксах
        
        Args:
            data (pd.DataFrame): Данные о группах
        """
        if data is None or data.empty:
            return
        
        # Получаем список групп
        groups = data['group_id'].tolist()
        
        # Обновляем комбобоксы
        self.group_combo['values'] = groups
        self.forecast_group_combo['values'] = groups
        
        # Выбираем первую группу, если есть
        if groups:
            self.group_var.set(groups[0])
            self.forecast_group_var.set(groups[0])
    
    def _open_mapping_editor(self):
        """Открытие редактора базы соответствий"""
        # Создаем диалог редактора базы соответствий
        editor_dialog = MappingEditorDialog(self, self.item_mapping, self.data_processor)
        
        # Ждем закрытия диалога
        self.wait_window(editor_dialog)
        
        # Обновляем базу соответствий в обработчике данных
        self.data_processor.set_item_mapping(self.item_mapping)
    
    def _show_about(self):
        """Отображение информации о программе"""
        messagebox.showinfo(
            "О программе",
            "Система анализа и прогнозирования заявок\n\n"
            "Версия: 1.0\n"
            "Дата: 22.04.2025\n\n"
            "Программа предназначена для анализа периодичности позиций заявок "
            "из Excel-файлов и прогнозирования будущих потребностей."
        )
