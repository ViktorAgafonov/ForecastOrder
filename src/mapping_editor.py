#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для редактирования базы соответствий артикулов и наименований
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import pandas as pd
import logging
import threading
from item_mapping import ItemMapping

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MappingEditor(ttk.Frame):
    """Класс для редактирования базы соответствий артикулов и наименований"""
    
    def __init__(self, parent, item_mapping, data_processor=None):
        """Инициализация редактора соответствий
        
        Args:
            parent: Родительский виджет
            item_mapping: Экземпляр класса ItemMapping
            data_processor: Экземпляр класса DataProcessor (опционально)
        """
        super().__init__(parent)
        self.parent = parent
        self.item_mapping = item_mapping
        self.data_processor = data_processor
        
        # Текущая выбранная группа
        self.current_group_id = None
        
        # Список для хранения идентификаторов групп
        self.group_ids = []
        
        # Создание интерфейса
        self._create_widgets()
        
        # Загрузка данных
        self._load_mappings()
    
    def _create_widgets(self):
        """Создание виджетов интерфейса"""
        # Фрейм с кнопками управления
        button_frame = ttk.Frame(self)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(button_frame, text="Обновить", command=self._load_mappings).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Создать группу", command=self._create_group).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Переименовать группу", command=self._rename_group).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Объединить группы", command=self._merge_groups).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Удалить группу", command=self._delete_group).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Автопоиск соответствий", command=self._auto_find_mappings).pack(side=tk.LEFT, padx=5)
        
        # Фрейм с группами и элементами
        content_frame = ttk.Frame(self)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Разделитель для групп и элементов
        paned = ttk.PanedWindow(content_frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)
        
        # Фрейм со списком групп
        groups_frame = ttk.LabelFrame(paned, text="Группы соответствий")
        paned.add(groups_frame, weight=1)
        
        # Создаем фрейм с прокруткой для списка групп
        groups_scroll_frame = ttk.Frame(groups_frame)
        groups_scroll_frame.pack(fill=tk.BOTH, expand=True)
        
        # Полоса прокрутки для списка групп
        groups_scrollbar = ttk.Scrollbar(groups_scroll_frame)
        groups_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Список групп
        self.groups_listbox = tk.Listbox(groups_scroll_frame, yscrollcommand=groups_scrollbar.set)
        self.groups_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        groups_scrollbar.config(command=self.groups_listbox.yview)
        
        # Привязываем событие выбора группы
        self.groups_listbox.bind('<<ListboxSelect>>', self._on_group_selected)
        
        # Фрейм со списком элементов группы
        items_frame = ttk.LabelFrame(paned, text="Элементы группы")
        paned.add(items_frame, weight=2)
        
        # Создаем фрейм с прокруткой для таблицы элементов
        items_scroll_frame = ttk.Frame(items_frame)
        items_scroll_frame.pack(fill=tk.BOTH, expand=True)
        
        # Полосы прокрутки для таблицы элементов
        items_x_scrollbar = ttk.Scrollbar(items_scroll_frame, orient=tk.HORIZONTAL)
        items_x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        
        items_y_scrollbar = ttk.Scrollbar(items_scroll_frame)
        items_y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Таблица элементов
        self.items_table = ttk.Treeview(
            items_scroll_frame,
            columns=('name', 'code'),
            show='headings',
            xscrollcommand=items_x_scrollbar.set,
            yscrollcommand=items_y_scrollbar.set
        )
        self.items_table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Настройка полос прокрутки
        items_x_scrollbar.config(command=self.items_table.xview)
        items_y_scrollbar.config(command=self.items_table.yview)
        
        # Настройка столбцов таблицы
        self.items_table.heading('name', text='Наименование')
        self.items_table.heading('code', text='Артикул')
        
        self.items_table.column('name', width=300)
        self.items_table.column('code', width=150)
        
        # Привязываем контекстное меню к таблице элементов
        self.items_table.bind('<Button-3>', self._show_item_context_menu)
        
        # Фрейм для добавления нового элемента
        add_item_frame = ttk.LabelFrame(items_frame, text="Добавить элемент")
        add_item_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(add_item_frame, text="Наименование:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.new_item_name = ttk.Entry(add_item_frame, width=40)
        self.new_item_name.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        ttk.Label(add_item_frame, text="Артикул:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.new_item_code = ttk.Entry(add_item_frame, width=20)
        self.new_item_code.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)
        
        ttk.Button(add_item_frame, text="Добавить", command=self._add_item).grid(row=2, column=0, columnspan=2, padx=5, pady=5)
    
    def _load_mappings(self):
        """Загрузка соответствий в интерфейс"""
        # Очищаем список групп
        self.groups_listbox.delete(0, tk.END)
        
        # Загружаем группы
        mappings = self.item_mapping.get_all_mappings()
        
        # Создаем список для хранения идентификаторов групп
        self.group_ids = []
        
        for group_id, group in mappings.items():
            self.groups_listbox.insert(tk.END, f"{group['name']} ({len(group['items'])} элементов)")
            # Сохраняем id группы в отдельном списке
            self.group_ids.append(group_id)
        
        # Очищаем таблицу элементов
        for item in self.items_table.get_children():
            self.items_table.delete(item)
        
        # Сбрасываем текущую группу
        self.current_group_id = None
    
    def _on_group_selected(self, event):
        """Обработчик выбора группы в списке"""
        selection = self.groups_listbox.curselection()
        if not selection:
            return
        
        index = selection[0]
        # Получаем идентификатор группы из списка group_ids
        if 0 <= index < len(self.group_ids):
            group_id = self.group_ids[index]
            self.current_group_id = group_id
            
            # Загружаем элементы выбранной группы
            self._load_group_items(group_id)
    
    def _load_group_items(self, group_id):
        """Загрузка элементов группы в таблицу
        
        Args:
            group_id (str): Идентификатор группы
        """
        # Очищаем таблицу элементов
        for item in self.items_table.get_children():
            self.items_table.delete(item)
        
        # Получаем группу
        group = self.item_mapping.get_group(group_id)
        if not group:
            return
        
        # Загружаем элементы группы
        for item in group['items']:
            self.items_table.insert('', tk.END, values=(item['name'], item['code']))
    
    def _create_group(self):
        """Создание новой группы соответствий"""
        # Запрашиваем название новой группы
        name = tk.simpledialog.askstring("Новая группа", "Введите название группы:")
        if not name:
            return
        
        # Создаем новую группу
        group_id = f"group_{len(self.item_mapping.get_all_mappings()) + 1}"
        self.item_mapping.mappings[group_id] = {
            'name': name,
            'items': []
        }
        self.item_mapping.save_mappings()
        
        # Обновляем список групп
        self._load_mappings()
    
    def _rename_group(self):
        """Переименование выбранной группы"""
        if not self.current_group_id:
            messagebox.showwarning("Предупреждение", "Сначала выберите группу")
            return
        
        # Получаем текущее название группы
        group = self.item_mapping.get_group(self.current_group_id)
        if not group:
            return
        
        # Запрашиваем новое название группы
        new_name = tk.simpledialog.askstring(
            "Переименование группы",
            "Введите новое название группы:",
            initialvalue=group['name']
        )
        if not new_name:
            return
        
        # Переименовываем группу
        if self.item_mapping.rename_group(self.current_group_id, new_name):
            # Обновляем список групп
            self._load_mappings()
    
    def _merge_groups(self):
        """Объединение двух групп соответствий"""
        if not self.current_group_id:
            messagebox.showwarning("Предупреждение", "Сначала выберите исходную группу")
            return
        
        # Получаем список групп для выбора целевой группы
        mappings = self.item_mapping.get_all_mappings()
        target_groups = [(group_id, group['name']) for group_id, group in mappings.items() 
                        if group_id != self.current_group_id]
        
        if not target_groups:
            messagebox.showwarning("Предупреждение", "Нет других групп для объединения")
            return
        
        # Создаем диалог выбора целевой группы
        dialog = tk.Toplevel(self)
        dialog.title("Выбор целевой группы")
        dialog.geometry("300x200")
        dialog.transient(self)
        dialog.grab_set()
        
        ttk.Label(dialog, text="Выберите целевую группу:").pack(padx=10, pady=10)
        
        target_listbox = tk.Listbox(dialog)
        target_listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        for group_id, name in target_groups:
            target_listbox.insert(tk.END, name)
            target_listbox.itemconfig(tk.END, {'group_id': group_id})
        
        def on_select():
            selection = target_listbox.curselection()
            if not selection:
                return
            
            index = selection[0]
            target_group_id = target_listbox.itemcget(index, 'group_id')
            
            # Объединяем группы
            if self.item_mapping.merge_groups(self.current_group_id, target_group_id):
                # Обновляем список групп
                self._load_mappings()
            
            dialog.destroy()
        
        ttk.Button(dialog, text="Объединить", command=on_select).pack(pady=10)
        ttk.Button(dialog, text="Отмена", command=dialog.destroy).pack(pady=5)
        
        dialog.wait_window()
    
    def _delete_group(self):
        """Удаление выбранной группы"""
        if not self.current_group_id:
            messagebox.showwarning("Предупреждение", "Сначала выберите группу")
            return
        
        # Запрашиваем подтверждение удаления
        if not messagebox.askyesno("Подтверждение", "Вы уверены, что хотите удалить выбранную группу?"):
            return
        
        # Удаляем группу
        if self.current_group_id in self.item_mapping.mappings:
            del self.item_mapping.mappings[self.current_group_id]
            self.item_mapping.save_mappings()
            
            # Обновляем список групп
            self._load_mappings()
    
    def _add_item(self):
        """Добавление нового элемента в выбранную группу"""
        if not self.current_group_id:
            messagebox.showwarning("Предупреждение", "Сначала выберите группу")
            return
        
        # Получаем данные нового элемента
        name = self.new_item_name.get().strip()
        code = self.new_item_code.get().strip()
        
        if not name or not code:
            messagebox.showwarning("Предупреждение", "Заполните наименование и артикул")
            return
        
        # Добавляем элемент в группу
        if self.item_mapping.add_item_to_group(self.current_group_id, name, code):
            # Очищаем поля ввода
            self.new_item_name.delete(0, tk.END)
            self.new_item_code.delete(0, tk.END)
            
            # Обновляем таблицу элементов
            self._load_group_items(self.current_group_id)
    
    def _show_item_context_menu(self, event):
        """Отображение контекстного меню для элемента таблицы"""
        # Получаем выбранный элемент
        item = self.items_table.identify_row(event.y)
        if not item:
            return
        
        # Выделяем элемент
        self.items_table.selection_set(item)
        
        # Создаем контекстное меню
        context_menu = tk.Menu(self, tearoff=0)
        context_menu.add_command(label="Удалить", command=self._delete_selected_item)
        
        # Отображаем меню
        context_menu.post(event.x_root, event.y_root)
    
    def _close_progress_dialog(self, dialog, dialog_active_flag):
        """Закрытие диалога прогресса
        
        Args:
            dialog: Диалог прогресса
            dialog_active_flag: Флаг, указывающий на активность диалога
        """
        # Закрываем диалог
        try:
            dialog.destroy()
        except Exception:
            # Игнорируем ошибки, если диалог уже закрыт
            pass
    
    def _delete_selected_item(self):
        """Удаление выбранного элемента из группы"""
        if not self.current_group_id:
            return
        
        # Получаем выбранный элемент
        selection = self.items_table.selection()
        if not selection:
            return
        
        item_id = selection[0]
        values = self.items_table.item(item_id, 'values')
        
        name = values[0]
        code = values[1]
        
        # Удаляем элемент из группы
        if self.item_mapping.remove_item_from_group(self.current_group_id, name, code):
            # Обновляем таблицу элементов
            self._load_group_items(self.current_group_id)
    
    def _auto_find_mappings(self):
        """Автоматический поиск соответствий"""
        if not self.data_processor or self.data_processor.processed_data is None:
            messagebox.showwarning("Предупреждение", "Сначала загрузите и обработайте данные")
            return
        
        # Запрашиваем порог сходства
        threshold = simpledialog.askinteger(
            "Порог сходства",
            "Введите порог сходства (0-100):",
            initialvalue=80,
            minvalue=0,
            maxvalue=100
        )
        if threshold is None:
            return
        
        # Находим столбцы с наименованиями и артикулами
        name_col = next((col for col in self.data_processor.processed_data.columns 
                         if 'наименование' in col and 'норм' not in col), None)
        code_col = next((col for col in self.data_processor.processed_data.columns 
                         if 'артикул' in col and 'норм' not in col), None)
        
        if not name_col or not code_col:
            messagebox.showerror("Ошибка", "Не найдены столбцы с наименованиями и артикулами")
            return
        
        # Создаем диалог прогресса
        progress_dialog = ProgressDialog(
            self.parent,
            title="Поиск соответствий",
            message="Выполняется поиск похожих элементов..."
        )
        
        # Результаты поиска
        result = {"similar_items": None, "error": None}
        
        # Флаг для отслеживания состояния диалога
        dialog_active = True
        
        # Функция обновления прогресса
        def update_progress(value, status_text):
            # Проверяем, что диалог еще активен
            if dialog_active:
                # Используем after для обновления UI из другого потока
                try:
                    self.after(0, lambda: progress_dialog.update_progress(value, status_text))
                except Exception:
                    # Игнорируем ошибки, если диалог уже закрыт
                    pass
        
        # Функция для выполнения поиска в отдельном потоке
        def search_thread():
            try:
                # Выполняем поиск похожих элементов через ItemMapping
                result["similar_items"] = self.item_mapping.find_similar_items(
                    self.data_processor.processed_data,
                    name_col,
                    code_col,
                    threshold,
                    progress_callback=update_progress
                )
            except Exception as e:
                result["error"] = str(e)
            finally:
                # Закрываем диалог прогресса
                self.after(0, lambda: self._close_progress_dialog(progress_dialog, dialog_active))
        
        # Запускаем поиск в отдельном потоке
        thread = threading.Thread(target=search_thread)
        thread.daemon = True
        thread.start()
        
        # Ждем завершения диалога
        self.wait_window(progress_dialog)
        
        # Проверяем результаты
        if result["error"]:
            messagebox.showerror("Ошибка", f"Произошла ошибка при поиске соответствий: {result['error']}")
            return
        
        similar_items = result["similar_items"]
        if not similar_items:
            messagebox.showinfo("Информация", "Не найдено похожих элементов")
            return
        
        # Запрашиваем подтверждение добавления найденных групп
        groups_count = len(similar_items)
        if not messagebox.askyesno(
            "Подтверждение",
            f"Найдено {groups_count} групп похожих элементов. Добавить их в базу соответствий?"
        ):
            return
        
        # Добавляем найденные группы в базу соответствий
        added = self.item_mapping.update_from_similar_items(similar_items)
        
        # Обновляем список групп
        self._load_mappings()
        
        messagebox.showinfo("Информация", f"Добавлено {added} новых групп в базу соответствий")


class ProgressDialog(tk.Toplevel):
    """Диалог с индикатором прогресса"""
    
    def __init__(self, parent, title="Выполнение операции", message="Пожалуйста, подождите..."):
        """Инициализация диалога прогресса
        
        Args:
            parent: Родительский виджет
            title: Заголовок диалога
            message: Сообщение диалога
        """
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        
        # Делаем диалог модальным
        self.transient(parent)
        self.grab_set()
        
        # Создаем виджеты
        frame = ttk.Frame(self, padding="20 10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Сообщение
        self.message_label = ttk.Label(frame, text=message)
        self.message_label.pack(pady=(0, 10))
        
        # Индикатор прогресса
        self.progress = ttk.Progressbar(frame, orient="horizontal", length=300, mode="determinate")
        self.progress.pack(fill=tk.X, pady=5)
        
        # Текст статуса
        self.status_label = ttk.Label(frame, text="")
        self.status_label.pack(pady=5)
        
        # Центрируем диалог
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = parent.winfo_rootx() + (parent.winfo_width() - width) // 2
        y = parent.winfo_rooty() + (parent.winfo_height() - height) // 2
        self.geometry(f"{width}x{height}+{x}+{y}")
    
    def update_progress(self, value, status_text=""):
        """Обновление прогресса
        
        Args:
            value: Значение прогресса (0-100)
            status_text: Текст статуса
        """
        self.progress["value"] = value
        if status_text:
            self.status_label["text"] = status_text
        self.update_idletasks()


class MappingEditorDialog(tk.Toplevel):
    """Диалог для редактирования базы соответствий"""
    
    def __init__(self, parent, item_mapping, data_processor=None):
        """Инициализация диалога
        
        Args:
            parent: Родительский виджет
            item_mapping: Экземпляр класса ItemMapping
            data_processor: Экземпляр класса DataProcessor (опционально)
        """
        super().__init__(parent)
        self.title("Редактор базы соответствий")
        self.geometry("800x600")
        self.minsize(600, 400)
        
        # Создаем редактор соответствий
        self.editor = MappingEditor(self, item_mapping, data_processor)
        self.editor.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Кнопка закрытия
        ttk.Button(self, text="Закрыть", command=self.destroy).pack(pady=10)
        
        # Делаем диалог модальным
        self.transient(parent)
        self.grab_set()
        
        # Центрируем диалог относительно родительского окна
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = parent.winfo_rootx() + (parent.winfo_width() - width) // 2
        y = parent.winfo_rooty() + (parent.winfo_height() - height) // 2
        self.geometry(f"{width}x{height}+{x}+{y}")
