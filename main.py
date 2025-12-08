import sys
import os
import json
import pyodbc
import shutil
from datetime import datetime, timedelta
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                               QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                               QTableWidget, QTableWidgetItem, QComboBox, QMessageBox, 
                               QGroupBox, QTabWidget, QFileDialog, QCheckBox, QTimeEdit,
                               QProgressBar, QFormLayout, QRadioButton, 
                               QButtonGroup, QAbstractItemView, QHeaderView, QMenu)
from PySide6.QtCore import Qt, QThread, Signal, QTime, QTimer, QSize
from PySide6.QtGui import QIcon, QAction, QPalette, QColor, QFont, QGuiApplication
import subprocess
import platform
from config import (DEFAULT_BACKUP_PATH, SETTINGS_FILE, ODBC_DRIVER, 
                   DEFAULT_USER, SCHEDULER_CHECK_INTERVAL)

class Worker(QThread):
    progress = Signal(str)
    finished = Signal(bool, str)

    def __init__(self, connection_str, sql_commands, operation_name):
        super().__init__()
        self.conn_str = connection_str
        self.sql_commands = sql_commands
        self.operation_name = operation_name

    def run(self):
        try:
            conn = pyodbc.connect(self.conn_str, autocommit=True)
            cursor = conn.cursor()
            
            # Выполнение команд (BACKUP/RESTORE)
            for sql in self.sql_commands:
                self.progress.emit(f"Выполнение: {sql[:60]}...")
                cursor.execute(sql)
                while cursor.nextset(): 
                    pass
            
            conn.close()
            self.finished.emit(True, f"Операция '{self.operation_name}' успешно завершена!")
        except Exception as e:
            self.finished.emit(False, f"Ошибка SQL: {str(e)}")

class BackupApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SQL Server Backup Manager")
        self.resize(1200, 800)
        
        # темная тема
        self.set_dark_theme()
        
        icon = QIcon()
        icon.addFile(u"logo.png", QSize(), QIcon.Normal, QIcon.Off)
        self.setWindowIcon(icon)
    
        # Переменные состояния
        self.connection = None
        self.conn_str_cache = ""
        self.history = self.load_history()
        self.current_backup_path = DEFAULT_BACKUP_PATH
        self.last_backup_day = None
        
        self.init_ui()
        
    def set_dark_theme(self):
        app = QApplication.instance()
        
        dark_palette = QPalette()
        dark_palette.setColor(QPalette.Window, QColor(30, 30, 30))
        dark_palette.setColor(QPalette.WindowText, QColor(240, 240, 240))
        dark_palette.setColor(QPalette.Base, QColor(20, 20, 20))
        dark_palette.setColor(QPalette.AlternateBase, QColor(40, 40, 40))
        dark_palette.setColor(QPalette.ToolTipBase, QColor(50, 50, 50))
        dark_palette.setColor(QPalette.ToolTipText, QColor(240, 240, 240))
        dark_palette.setColor(QPalette.Text, QColor(240, 240, 240))
        dark_palette.setColor(QPalette.Button, QColor(45, 45, 45))
        dark_palette.setColor(QPalette.ButtonText, QColor(240, 240, 240))
        dark_palette.setColor(QPalette.BrightText, QColor(255, 80, 80))
        dark_palette.setColor(QPalette.Link, QColor(100, 150, 255))
        dark_palette.setColor(QPalette.Highlight, QColor(76, 175, 80))
        dark_palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
        
        app.setPalette(dark_palette)
        
        app.setStyleSheet("""
            QMainWindow, QWidget { 
                background-color: #1e1e1e; 
                color: #e0e0e0; 
            }
            QGroupBox { 
                font-weight: 600; 
                font-size: 11pt;
                border: 2px solid #3a3a3a; 
                border-radius: 8px; 
                margin-top: 12px; 
                padding-top: 12px;
                background-color: #252525;
            }
            QGroupBox::title { 
                subcontrol-origin: margin; 
                left: 12px; 
                padding: 0 8px; 
                color: #64b5f6; 
                font-size: 10pt;
            }
            QLineEdit, QTextEdit, QPlainTextEdit { 
                padding: 8px 12px; 
                border-radius: 6px; 
                background-color: #2a2a2a; 
                color: #e0e0e0; 
                border: 2px solid #3a3a3a; 
                selection-background-color: #4caf50;
                font-size: 10pt;
            }
            QLineEdit:focus, QTextEdit:focus {
                border: 2px solid #64b5f6;
            }
            QTableWidget { 
                background-color: #252525; 
                color: #e0e0e0; 
                gridline-color: #3a3a3a; 
                border: 1px solid #3a3a3a;
                border-radius: 6px;
                selection-background-color: #4caf50;
                selection-color: white;
                font-size: 10pt;
            }
            QHeaderView::section { 
                background-color: #2d2d2d; 
                padding: 8px; 
                border: none; 
                border-bottom: 2px solid #3a3a3a;
                color: #b0b0b0;
                font-weight: 600;
                font-size: 10pt;
            }
            QPushButton { 
                padding: 10px 20px; 
                border-radius: 6px; 
                background-color: #3a3a3a; 
                color: #e0e0e0; 
                border: none;
                font-weight: 600;
                font-size: 10pt;
                min-height: 20px;
            }
            QPushButton:hover { 
                background-color: #4a4a4a; 
                transform: translateY(-1px);
            }
            QPushButton:pressed { 
                background-color: #2a2a2a; 
            }
            QPushButton#GreenBtn { 
                background-color: #4caf50; 
                color: white; 
            }
            QPushButton#GreenBtn:hover { 
                background-color: #45a049; 
            }
            QPushButton#BlueBtn { 
                background-color: #2196f3; 
                color: white; 
            }
            QPushButton#BlueBtn:hover { 
                background-color: #1976d2; 
            }
            QPushButton#RedBtn { 
                background-color: #f44336; 
                color: white; 
            }
            QPushButton#RedBtn:hover { 
                background-color: #d32f2f; 
            }
            QPushButton#YellowBtn { 
                background-color: #ff9800; 
                color: white; 
            }
            QPushButton#YellowBtn:hover { 
                background-color: #f57c00; 
            }
            QComboBox { 
                padding: 8px 12px; 
                background-color: #2a2a2a; 
                border: 2px solid #3a3a3a; 
                border-radius: 6px;
                color: #e0e0e0;
                font-size: 10pt;
            }
            QComboBox:focus {
                border: 2px solid #64b5f6;
            }
            QComboBox::drop-down {
                border: 0px;
                width: 30px;
            }
            QComboBox::down-arrow {
                image: none;
                border-left: 6px solid transparent;
                border-right: 6px solid transparent;
                border-top: 8px solid #b0b0b0;
                margin-right: 8px;
            }
            QComboBox QAbstractItemView {
                background-color: #2a2a2a;
                color: #e0e0e0;
                selection-background-color: #4caf50;
                border: 1px solid #3a3a3a;
                border-radius: 4px;
            }
            QTabWidget::pane { 
                border: 1px solid #3a3a3a; 
                background-color: #1e1e1e;
                border-radius: 6px;
            }
            QTabBar::tab { 
                background: #2a2a2a; 
                color: #b0b0b0; 
                padding: 10px 24px; 
                margin-right: 4px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
                font-weight: 500;
                font-size: 10pt;
            }
            QTabBar::tab:selected { 
                background: #4caf50; 
                color: white; 
                font-weight: 600;
            }
            QTabBar::tab:hover:!selected {
                background: #3a3a3a;
            }
            QRadioButton { 
                spacing: 12px; 
                color: #e0e0e0;
                font-size: 10pt;
            }
            QRadioButton::indicator { 
                width: 18px; 
                height: 18px; 
                background-color: #2a2a2a; 
                border: 2px solid #3a3a3a; 
                border-radius: 9px;
            }
            QRadioButton::indicator:checked { 
                background-color: #4caf50; 
                border: 2px solid #4caf50; 
            }
            QCheckBox {
                color: #e0e0e0;
                font-size: 10pt;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                background-color: #2a2a2a;
                border: 2px solid #3a3a3a;
                border-radius: 4px;
            }
            QCheckBox::indicator:checked {
                background-color: #4caf50;
                border: 2px solid #4caf50;
            }
            QProgressBar {
                border: 1px solid #3a3a3a;
                border-radius: 6px;
                text-align: center;
                background-color: #2a2a2a;
                color: #e0e0e0;
                font-size: 10pt;
            }
            QProgressBar::chunk {
                background-color: #4caf50;
                border-radius: 5px;
            }
            QMenu {
                background-color: #2a2a2a;
                color: #e0e0e0;
                border: 1px solid #3a3a3a;
                border-radius: 6px;
                padding: 4px;
            }
            QMenu::item {
                padding: 8px 24px;
                border-radius: 4px;
            }
            QMenu::item:selected {
                background-color: #4caf50;
            }
            QLabel {
                color: #e0e0e0;
                font-size: 10pt;
            }
            QTimeEdit {
                background-color: #2a2a2a;
                color: #e0e0e0;
                border: 2px solid #3a3a3a;
                padding: 8px;
                border-radius: 6px;
                font-size: 10pt;
            }
            QTimeEdit:focus {
                border: 2px solid #64b5f6;
            }
            QScrollBar:vertical {
                background-color: #1e1e1e;
                width: 12px;
                border-radius: 6px;
                margin: 2px;
            }
            QScrollBar::handle:vertical {
                background-color: #4a4a4a;
                border-radius: 6px;
                min-height: 30px;
            }
            QScrollBar::handle:vertical:hover {
                background-color: #5a5a5a;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
        """)

    def load_history(self):
        """Загрузка истории подключений из JSON"""
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save_history(self):
        """Сохранение истории подключений в JSON"""
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.history, f, ensure_ascii=False, indent=4)

    def init_ui(self):
        """Инициализация пользовательского интерфейса"""
        # Меню
        self.create_menu()
        
        # Основной контейнер
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Создаем вкладки
        self.init_connection_tab()
        self.init_backup_tab()
        self.init_restore_tab()
        self.init_scheduler_tab()
        self.init_backup_files_tab()  

        # Статус бар
        status_container = QWidget()
        status_layout = QHBoxLayout(status_container)
        status_layout.setContentsMargins(5, 5, 5, 5)
        
        self.status_label = QLabel("Готов к работе")
        self.status_label.setStyleSheet("color: #4CAF50; font-weight: bold;")
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.progress_bar)
        main_layout.addWidget(status_container)

    def create_menu(self):
        """Создание меню"""
        menubar = self.menuBar()
        
        file_menu = menubar.addMenu("Файл")
        
        exit_action = QAction("Выход", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        help_menu = menubar.addMenu("Справка")
        
        about_action = QAction("О программе", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

    def init_connection_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)

        # История
        hist_group = QGroupBox("Сохраненные подключения")
        hist_layout = QVBoxLayout()
        
        top_hist_layout = QHBoxLayout()
        self.combo_history = QComboBox()
        self.combo_history.addItem("Новое подключение", None)
        for name, data in self.history.items():
            self.combo_history.addItem(name, data)
        self.combo_history.currentIndexChanged.connect(self.fill_connection_data)
        
        btn_delete_hist = QPushButton("Удалить выбранное")
        btn_delete_hist.clicked.connect(self.delete_history_item)
        btn_delete_hist.setObjectName("RedBtn")
        
        top_hist_layout.addWidget(QLabel("Выбрать:"))
        top_hist_layout.addWidget(self.combo_history, 1)
        top_hist_layout.addWidget(btn_delete_hist)
        
        hist_layout.addLayout(top_hist_layout)
        hist_group.setLayout(hist_layout)
        layout.addWidget(hist_group)

        # Форма ввода
        form_group = QGroupBox("Параметры сервера")
        form_layout = QFormLayout()
        form_layout.setSpacing(10)
        
        self.conn_name = QLineEdit()
        self.conn_name.setPlaceholderText("Название подключения")
        
        self.server_input = QLineEdit()
        self.server_input.setPlaceholderText("localhost\\SQLEXPRESS или 192.168.1.100")
        
        self.user_input = QLineEdit()
        self.user_input.setText(DEFAULT_USER)
        self.user_input.setPlaceholderText("Имя пользователя")
        
        self.pass_input = QLineEdit()
        self.pass_input.setEchoMode(QLineEdit.Password)
        self.pass_input.setPlaceholderText("Пароль")
        
        form_layout.addRow("Название:", self.conn_name)
        form_layout.addRow("Сервер:", self.server_input)
        form_layout.addRow("Пользователь:", self.user_input)
        form_layout.addRow("Пароль:", self.pass_input)
        form_group.setLayout(form_layout)
        layout.addWidget(form_group)

        # Кнопка подключения
        self.btn_connect = QPushButton("ПОДКЛЮЧИТЬСЯ")
        self.btn_connect.setObjectName("GreenBtn")
        self.btn_connect.setFixedHeight(50)
        self.btn_connect.clicked.connect(self.connect_to_db)
        layout.addWidget(self.btn_connect)
        
        layout.addStretch()
        self.tabs.addTab(tab, "📡 Подключение")

    def fill_connection_data(self):
        data = self.combo_history.currentData()
        if data:
            self.conn_name.setText(self.combo_history.currentText())
            self.server_input.setText(data.get('server', ''))
            self.user_input.setText(data.get('user', ''))
            self.pass_input.setText(data.get('password', ''))

    def delete_history_item(self):
        idx = self.combo_history.currentIndex()
        if idx > 0:
            name = self.combo_history.currentText()
            reply = QMessageBox.question(self, "Подтверждение", 
                                       f"Удалить подключение '{name}' из истории?",
                                       QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                del self.history[name]
                self.save_history()
                self.combo_history.removeItem(idx)
                self.conn_name.clear()
                self.server_input.clear()
                self.user_input.clear()
                self.pass_input.clear()

    def connect_to_db(self):
        server = self.server_input.text()
        user = self.user_input.text()
        password = self.pass_input.text()
        name = self.conn_name.text()

        if not server or not user:
            QMessageBox.warning(self, "Ошибка", "Заполните поля сервера и пользователя")
            return

        # Строка подключения
        self.conn_str_cache = f'DRIVER={{{ODBC_DRIVER}}};SERVER={server};UID={user};PWD={password};TrustServerCertificate=yes;'

        try:
            self.connection = pyodbc.connect(self.conn_str_cache)
            self.status_label.setText(f"✅ Успешное подключение к {server}")
            self.status_label.setStyleSheet("color: #4caf50; font-weight: bold;")
            
            # Сохраняем в историю
            if name:
                self.history[name] = {'server': server, 'user': user, 'password': password}
                self.save_history()
                if self.combo_history.findText(name) == -1:
                    self.combo_history.addItem(name, self.history[name])

            # Загружаем данные после подключения
            self.load_databases_with_sizes()
            self.load_databases_for_restore()
            self.load_databases_for_schedule()
            self.tabs.setCurrentIndex(1) # Переключаем на вкладку бэкапа
            
        except Exception as e:
            self.status_label.setText("❌ Ошибка подключения")
            self.status_label.setStyleSheet("color: #f44336; font-weight: bold;")
            QMessageBox.critical(self, "Ошибка подключения", 
                               f"Не удалось подключиться к серверу.\n\nДетали:\n{str(e)}")

    def load_databases_with_sizes(self):
        """Загрузка списка баз данных с размерами"""
        if not self.connection:
            return
            
        try:
            cursor = self.connection.cursor()
            # Получаем список баз с размерами
            sql_query = """
                SELECT 
                    d.name,
                    CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(18,2)) as SizeMB,
                    CAST(SUM(mf.size) * 8.0 / 1024 / 1024 AS DECIMAL(18,2)) as SizeGB,
                    d.state_desc,
                    d.recovery_model_desc
                FROM sys.databases d
                JOIN sys.master_files mf ON d.database_id = mf.database_id
                WHERE d.name NOT IN ('master', 'tempdb', 'model', 'msdb')
                GROUP BY d.name, d.state_desc, d.recovery_model_desc
                ORDER BY d.name
            """
            cursor.execute(sql_query)
            dbs = cursor.fetchall()
            
            self.db_table.setRowCount(0)
            self.db_table.setRowCount(len(dbs))
            
            for i, db in enumerate(dbs):
                db_name = db[0]
                size_mb = db[1]
                size_gb = db[2]
                state = db[3]
                recovery_model = db[4]
                
                # Чекбокс
                chk = QTableWidgetItem()
                if state == 'ONLINE':
                    chk.setCheckState(Qt.Unchecked)
                else:
                    chk.setCheckState(Qt.Unchecked)
                    chk.setFlags(chk.flags() & ~Qt.ItemIsEnabled)  # Делаем недоступным
                self.db_table.setItem(i, 0, chk)
                
                # Имя базы
                name_item = QTableWidgetItem(db_name)
                if state != 'ONLINE':
                    name_item.setForeground(Qt.red)
                    name_item.setToolTip(f"База не в сети: {state}")
                self.db_table.setItem(i, 1, name_item)
                
                # Размер в GB
                size_item = QTableWidgetItem(f"{size_gb:.2f} GB")
                size_item.setToolTip(f"{size_mb:.2f} MB")
                self.db_table.setItem(i, 2, size_item)
                
                # Состояние
                state_item = QTableWidgetItem(state)
                if state == 'ONLINE':
                    state_item.setForeground(QColor('#4CAF50'))
                else:
                    state_item.setForeground(Qt.red)
                self.db_table.setItem(i, 3, state_item)
                
                # Модель восстановления
                self.db_table.setItem(i, 4, QTableWidgetItem(recovery_model))
                
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Не удалось загрузить размеры баз:\n{str(e)}")
            # Загружаем без размеров
            self.load_databases_simple()

    def load_databases_simple(self):
        """Загрузка только имен баз (резервный метод)"""
        try:
            cursor = self.connection.cursor()
            sql_query = """
                SELECT name 
                FROM sys.databases 
                WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                AND state_desc = 'ONLINE'
            """
            cursor.execute(sql_query)
            dbs = cursor.fetchall()
            
            self.db_table.setRowCount(0)
            self.db_table.setRowCount(len(dbs))
            for i, db in enumerate(dbs):
                db_name = db[0]
                
                chk = QTableWidgetItem()
                chk.setCheckState(Qt.Unchecked)
                self.db_table.setItem(i, 0, chk)
                
                self.db_table.setItem(i, 1, QTableWidgetItem(db_name))
                self.db_table.setItem(i, 2, QTableWidgetItem("N/A"))
                self.db_table.setItem(i, 3, QTableWidgetItem("ONLINE"))
                self.db_table.setItem(i, 4, QTableWidgetItem("N/A"))
                
        except Exception as e:
            print(f"Ошибка при загрузке баз: {e}")

    # Вкладка  Бэкап 
    def init_backup_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        # Таблица баз данных
        self.db_table = QTableWidget()
        self.db_table.setColumnCount(5)
        self.db_table.setHorizontalHeaderLabels(["✓", "Имя Базы", "Размер", "Состояние", "Модель восстановления"])
        self.db_table.setColumnWidth(0, 40)
        self.db_table.setColumnWidth(1, 200)
        self.db_table.setColumnWidth(2, 100)
        self.db_table.setColumnWidth(3, 100)
        self.db_table.setColumnWidth(4, 150)
        self.db_table.horizontalHeader().setStretchLastSection(True)
        self.db_table.verticalHeader().setVisible(False)
        
        # Кнопки для таблицы
        table_buttons = QHBoxLayout()
        btn_select_all = QPushButton("Выбрать все")
        btn_select_all.clicked.connect(lambda: self.select_all_databases(True))
        btn_deselect_all = QPushButton("Снять выделение")
        btn_deselect_all.clicked.connect(lambda: self.select_all_databases(False))
        btn_refresh_sizes = QPushButton("🔄 Обновить размеры")
        btn_refresh_sizes.clicked.connect(self.load_databases_with_sizes)
        
        table_buttons.addWidget(btn_select_all)
        table_buttons.addWidget(btn_deselect_all)
        table_buttons.addStretch()
        table_buttons.addWidget(btn_refresh_sizes)
        
        layout.addWidget(QLabel("Выберите базы для резервного копирования:"))
        layout.addLayout(table_buttons)
        layout.addWidget(self.db_table)

        # Настройки бэкапа
        sett_group = QGroupBox("Настройки Бэкапа")
        sett_layout = QVBoxLayout()
        sett_layout.setSpacing(15)
        
        # Путь для бэкапа
        path_layout = QHBoxLayout()
        self.backup_path = QLineEdit(DEFAULT_BACKUP_PATH)
        self.backup_path.setPlaceholderText("Сетевой путь для бэкапов")
        
        btn_test_path = QPushButton("Проверить путь")
        btn_test_path.clicked.connect(self.test_backup_path)
        
        path_layout.addWidget(QLabel("Путь для бэкапов:"))
        path_layout.addWidget(self.backup_path, 1)
        path_layout.addWidget(btn_test_path)
        
        # Тип бэкапа
        type_group = QGroupBox("Тип бэкапа")
        type_layout = QHBoxLayout()
        
        self.backup_type_group = QButtonGroup(self)
        
        self.radio_full = QRadioButton("Полный бэкап")
        self.radio_full.setChecked(True)
        self.radio_full.setToolTip("Создает полную копию базы данных")
        
        self.radio_differential = QRadioButton("Дифференциальный")
        self.radio_differential.setToolTip("Создает бэкап только изменений с момента последнего полного бэкапа")
        
        self.backup_type_group.addButton(self.radio_full)
        self.backup_type_group.addButton(self.radio_differential)
        
        type_layout.addWidget(self.radio_full)
        type_layout.addWidget(self.radio_differential)
        type_group.setLayout(type_layout)
        
        # Дополнительные опции
        opt_layout = QHBoxLayout()
        self.chk_compression = QCheckBox("Сжимать бэкап (COMPRESSION)")
        self.chk_compression.setChecked(True)
        self.chk_compression.setToolTip("Значительно уменьшает размер файла")
        
        self.chk_copy_only = QCheckBox("Только копия (COPY_ONLY)")
        self.chk_copy_only.setToolTip("Делает бэкап, не нарушая цепочку логов для разностных бэкапов")
        
        self.chk_verify = QCheckBox("Проверить бэкап (VERIFY)")
        self.chk_verify.setChecked(True)
        self.chk_verify.setToolTip("Проверяет целостность бэкапа после создания")
        
        opt_layout.addWidget(self.chk_compression)
        opt_layout.addWidget(self.chk_copy_only)
        opt_layout.addWidget(self.chk_verify)
        
        sett_layout.addLayout(path_layout)
        sett_layout.addWidget(type_group)
        sett_layout.addLayout(opt_layout)
        sett_group.setLayout(sett_layout)
        layout.addWidget(sett_group)

        # Кнопка запуска
        self.btn_backup = QPushButton("🚀 ЗАПУСТИТЬ БЭКАП")
        self.btn_backup.setObjectName("BlueBtn")
        self.btn_backup.setFixedHeight(50)
        self.btn_backup.clicked.connect(self.start_backup)
        layout.addWidget(self.btn_backup)
        
        self.tabs.addTab(tab, "💾 Создание Бэкапа")

    def test_backup_path(self):
        """Проверка доступности пути для бэкапов"""
        path = self.backup_path.text()
        if not path:
            QMessageBox.warning(self, "Ошибка", "Укажите путь для проверки")
            return
            
        # Проверяем сетевой путь
        if path.startswith('\\\\'):
            try:
                # Пробуем подключиться к сетевой папке
                if os.path.exists(path):
                    files = []
                    try:
                        files = os.listdir(path)
                    except:
                        pass
                    
                    info = f"✅ Путь доступен:\n{path}"
                    if files:
                        info += f"\n\nНайдено файлов: {len(files)}"
                        bak_files = [f for f in files if f.lower().endswith('.bak')]
                        if bak_files:
                            info += f"\nФайлов .bak: {len(bak_files)}"
                    else:
                        info += "\nПапка пуста"
                    
                    QMessageBox.information(self, "Проверка пути", info)
                else:
                    # Пробуем создать папку
                    reply = QMessageBox.question(self, "Путь не существует", 
                                               f"Путь {path} не существует.\nСоздать папку?",
                                               QMessageBox.Yes | QMessageBox.No)
                    if reply == QMessageBox.Yes:
                        try:
                            os.makedirs(path, exist_ok=True)
                            QMessageBox.information(self, "Успех", f"Папка создана:\n{path}")
                        except Exception as e:
                            QMessageBox.critical(self, "Ошибка", f"Не удалось создать папку:\n{str(e)}")
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось получить доступ к пути:\n{str(e)}")
        else:
            QMessageBox.warning(self, "Предупреждение", 
                              f"Указан локальный путь:\n{path}\n\n"
                              "Рекомендуется использовать сетевой путь для централизованного хранения бэкапов.")

    def select_all_databases(self, select):
        """Выделить/снять все базы данных"""
        for i in range(self.db_table.rowCount()):
            item = self.db_table.item(i, 0)
            if item and item.flags() & Qt.ItemIsEnabled:
                item.setCheckState(Qt.Checked if select else Qt.Unchecked)

    def start_backup(self):
        if not self.connection:
            QMessageBox.warning(self, "Ошибка", "Сначала подключитесь к серверу!")
            return

        selected_dbs = []
        for i in range(self.db_table.rowCount()):
            item = self.db_table.item(i, 0)
            if item and item.checkState() == Qt.Checked:
                selected_dbs.append(self.db_table.item(i, 1).text())

        if not selected_dbs:
            QMessageBox.warning(self, "Ошибка", "Выберите хотя бы одну базу данных из списка.")
            return

        target_path = self.backup_path.text()
        if not target_path:
            QMessageBox.warning(self, "Ошибка", "Укажите путь для сохранения бэкапов.")
            return

        # Проверяем и корректируем путь
        if not target_path.endswith("\\") and not target_path.endswith("/"): 
            target_path += "\\"
            
        # Для сетевых путей могут потребоваться дополнительные проверки
        if target_path.startswith('\\\\'):
            # Проверяем доступность сетевого пути
            if not os.path.exists(target_path):
                reply = QMessageBox.question(self, "Путь не существует", 
                                           f"Сетевой путь {target_path} не существует или недоступен.\n"
                                           "Продолжить? (Может возникнуть ошибка)",
                                           QMessageBox.Yes | QMessageBox.No)
                if reply == QMessageBox.No:
                    return

        sql_commands = []
        backup_type = "дифференциальный" if self.radio_differential.isChecked() else "полный"
        
        for db in selected_dbs:
            # Имя сервера
            server_address = self.server_input.text()
            server_name = server_address.split('\\')[0].split('.')[0]
            
            # Форматируем дату и время
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Формируем имя файла
            filename = f"{target_path}{server_name}_{db}_{timestamp}.bak"
            
            # Формируем SQL команду
            cmd = f"BACKUP DATABASE [{db}] TO DISK = '{filename}' WITH INIT"
            
            if self.radio_differential.isChecked():
                cmd += ", DIFFERENTIAL"
            
            if self.chk_compression.isChecked():
                cmd += ", COMPRESSION"
            
            if self.chk_copy_only.isChecked():
                cmd += ", COPY_ONLY"
            
            if self.chk_verify.isChecked():
                cmd += ", CHECKSUM"
            
            sql_commands.append(cmd)

        operation_name = f"Массовый {backup_type} бэкап ({len(selected_dbs)} баз)"
        self.run_worker(sql_commands, operation_name)

    # Вкладка  Восстановление 
    def init_restore_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)

        # Предупреждение
        warn_label = QLabel("⚠️ ВНИМАНИЕ: Восстановление полностью перезапишет текущую базу!")
        warn_label.setStyleSheet("color: #ff9800; font-weight: bold; font-size: 14px; padding: 10px; background-color: #333; border-radius: 5px;")
        warn_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(warn_label)

        # Форма восстановления
        form = QFormLayout()
        form.setSpacing(10)
        
        self.db_combo_restore = QComboBox()
        
        self.file_path_restore = QLineEdit()
        self.file_path_restore.setPlaceholderText("Выберите .bak файл или введите путь")
        
        file_layout = QHBoxLayout()
        btn_browse = QPushButton("Обзор файлов")
        btn_browse.clicked.connect(self.browse_backup_file_local)
        btn_clear = QPushButton("Очистить")
        btn_clear.clicked.connect(lambda: self.file_path_restore.clear())
        
        file_layout.addWidget(self.file_path_restore)
        file_layout.addWidget(btn_browse)
        file_layout.addWidget(btn_clear)

        form.addRow("База данных:", self.db_combo_restore)
        form.addRow("Файл бэкапа:", file_layout)
        
        layout.addLayout(form)
        
        # Опции восстановления
        options_group = QGroupBox("Опции восстановления")
        options_layout = QVBoxLayout()
        
        self.chk_close_conns = QCheckBox("Принудительно отключить пользователей (SINGLE_USER)")
        self.chk_close_conns.setChecked(True)
        self.chk_close_conns.setToolTip("Необходимо, если в базе кто-то работает")
        
        self.chk_overwrite = QCheckBox("Перезаписать существующую базу (REPLACE)")
        self.chk_overwrite.setChecked(True)
        self.chk_overwrite.setToolTip("Перезаписывает существующую базу данных")
        
        self.chk_recovery = QCheckBox("Восстановить базу в рабочее состояние (RECOVERY)")
        self.chk_recovery.setChecked(True)
        self.chk_recovery.setToolTip("Восстанавливает базу данных и делает её доступной")
        
        options_layout.addWidget(self.chk_close_conns)
        options_layout.addWidget(self.chk_overwrite)
        options_layout.addWidget(self.chk_recovery)
        options_group.setLayout(options_layout)
        layout.addWidget(options_group)

        # Кнопка восстановления
        btn_restore = QPushButton("🔄 ВОССТАНОВИТЬ БАЗУ")
        btn_restore.setObjectName("RedBtn")
        btn_restore.setFixedHeight(50)
        btn_restore.clicked.connect(self.start_restore)
        layout.addWidget(btn_restore)
        
        layout.addStretch()
        self.tabs.addTab(tab, "↩️ Восстановление")

    def browse_backup_file_local(self):
        """Просмотр файлов бэкапов в сетевой папке"""
        # Используем сетевой путь по умолчанию
        initial_path = DEFAULT_BACKUP_PATH if os.path.exists(DEFAULT_BACKUP_PATH) else ""
        
        fname, _ = QFileDialog.getOpenFileName(self, "Выберите файл бэкапа", 
                                              initial_path, "Backup Files (*.bak);;All Files (*)")
        if fname:
            self.file_path_restore.setText(fname)

    def load_databases_for_restore(self):
        """Загрузка списка баз для восстановления"""
        if not self.connection:
            return
            
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model')")
        dbs = cursor.fetchall()
        
        self.db_combo_restore.clear()
        for db in dbs:
            self.db_combo_restore.addItem(db[0])

    def start_restore(self):
        if not self.connection: 
            return
        
        db_name = self.db_combo_restore.currentText()
        file_path = self.file_path_restore.text()
        
        if not db_name or not file_path:
            QMessageBox.warning(self, "Ошибка", "Выберите базу данных и файл бэкапа")
            return

        if not os.path.exists(file_path):
            QMessageBox.critical(self, "Ошибка", f"Файл не найден:\n{file_path}")
            return

        reply = QMessageBox.question(self, "Подтверждение", 
                                   f"Вы ТОЧНО хотите восстановить базу '{db_name}' из файла '{os.path.basename(file_path)}'?\n\n"
                                   "⚠️ ВСЕ ТЕКУЩИЕ ДАННЫЕ БУДУТ УДАЛЕНЫ!",
                                   QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        
        if reply == QMessageBox.No: 
            return

        cmds = []
        # Переводим в Single User (выкидываем всех)
        if self.chk_close_conns.isChecked():
            cmds.append(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        
        # Само восстановление
        restore_cmd = f"RESTORE DATABASE [{db_name}] FROM DISK = '{file_path}'"
        if self.chk_overwrite.isChecked():
            restore_cmd += " WITH REPLACE"
        if self.chk_recovery.isChecked():
            restore_cmd += ", RECOVERY"
        
        cmds.append(restore_cmd)
        
        # Возвращаем Multi User
        if self.chk_close_conns.isChecked():
            cmds.append(f"ALTER DATABASE [{db_name}] SET MULTI_USER")

        self.run_worker(cmds, f"Восстановление '{db_name}'")

    # Вкладка Планировщик
    def init_scheduler_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)
        
        group = QGroupBox("Автоматический бэкап")
        gl = QFormLayout()
        gl.setSpacing(10)
        
        self.db_combo_schedule = QComboBox()
        
        self.time_edit = QTimeEdit()
        self.time_edit.setTime(QTime.currentTime())
        self.time_edit.setDisplayFormat("HH:mm")
        
        # Расписание дней
        days_group = QGroupBox("Дни недели")
        days_layout = QHBoxLayout()
        
        self.days_checkboxes = []
        days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        for i, day in enumerate(days):
            chk = QCheckBox(day)
            if i < 5:  # Пн-Пт по умолчанию отмечены
                chk.setChecked(True)
            self.days_checkboxes.append(chk)
            days_layout.addWidget(chk)
        days_layout.addStretch()
        days_group.setLayout(days_layout)
        
        # Кнопки управления таймером
        timer_layout = QHBoxLayout()
        self.btn_schedule = QPushButton("⏰ Активировать планировщик")
        self.btn_schedule.setCheckable(True)
        self.btn_schedule.clicked.connect(self.toggle_schedule)
        self.btn_schedule.setFixedHeight(40)
        
        self.btn_test_backup = QPushButton("Тестовый бэкап")
        self.btn_test_backup.clicked.connect(self.test_scheduled_backup)
        self.btn_test_backup.setObjectName("YellowBtn")
        
        timer_layout.addWidget(self.btn_schedule)
        timer_layout.addWidget(self.btn_test_backup)
        
        gl.addRow("База данных:", self.db_combo_schedule)
        gl.addRow("Время запуска:", self.time_edit)
        group.setLayout(gl)
        
        layout.addWidget(group)
        layout.addWidget(days_group)
        layout.addLayout(timer_layout)
        
        # Статус планировщика
        status_group = QGroupBox("Статус планировщика")
        status_layout = QVBoxLayout()
        
        self.lbl_timer_status = QLabel("Планировщик отключен")
        self.lbl_timer_status.setAlignment(Qt.AlignCenter)
        self.lbl_timer_status.setStyleSheet("font-size: 14px; color: grey; padding: 10px;")
        
        self.lbl_next_backup = QLabel("Следующий бэкап: -")
        self.lbl_next_backup.setAlignment(Qt.AlignCenter)
        self.lbl_next_backup.setStyleSheet("font-size: 12px; color: #aaa;")
        
        status_layout.addWidget(self.lbl_timer_status)
        status_layout.addWidget(self.lbl_next_backup)
        status_group.setLayout(status_layout)
        layout.addWidget(status_group)
        
        layout.addStretch()
        self.tabs.addTab(tab, "⏰ Планировщик")
        
        # Таймер для проверки времени
        self.timer = QTimer()
        self.timer.timeout.connect(self.check_schedule)
        self.timer.start(SCHEDULER_CHECK_INTERVAL)

    def load_databases_for_schedule(self):
        """Загрузка списка баз для планировщика"""
        if not self.connection:
            return
            
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
        dbs = cursor.fetchall()
        
        self.db_combo_schedule.clear()
        for db in dbs:
            self.db_combo_schedule.addItem(db[0])

    def toggle_schedule(self, checked):
        if checked:
            if not self.db_combo_schedule.currentText():
                QMessageBox.warning(self, "Ошибка", "Выберите базу данных для планирования")
                self.btn_schedule.setChecked(False)
                return
                
            self.btn_schedule.setText("⏹ Остановить планировщик")
            self.btn_schedule.setStyleSheet("background-color: #f44336; color: white;")
            self.lbl_timer_status.setText(f"Планировщик активен для базы '{self.db_combo_schedule.currentText()}'")
            self.lbl_timer_status.setStyleSheet("color: #4CAF50; font-weight: bold;")
            self.update_next_backup_time()
        else:
            self.btn_schedule.setText("⏰ Активировать планировщик")
            self.btn_schedule.setStyleSheet("")
            self.lbl_timer_status.setText("Планировщик отключен")
            self.lbl_timer_status.setStyleSheet("color: grey;")
            self.lbl_next_backup.setText("Следующий бэкап: -")

    def update_next_backup_time(self):
        """Обновление времени следующего бэкапа"""
        if not self.btn_schedule.isChecked():
            return
            
        now = datetime.now()
        target_time = self.time_edit.time()
        
        # Проверяем сегодняшний день
        if self.days_checkboxes[now.weekday()].isChecked():
            scheduled_time = datetime(now.year, now.month, now.day, 
                                     target_time.hour(), target_time.minute())
            
            if now < scheduled_time:
                self.lbl_next_backup.setText(f"Следующий бэкап: сегодня в {target_time.toString('HH:mm')}")
                return
        
        # Ищем следующий подходящий день
        for days_ahead in range(1, 8):
            next_day = now + timedelta(days=days_ahead)
            if self.days_checkboxes[next_day.weekday()].isChecked():
                next_date = datetime(next_day.year, next_day.month, next_day.day,
                                    target_time.hour(), target_time.minute())
                self.lbl_next_backup.setText(f"Следующий бэкап: {next_date.strftime('%d.%m.%Y в %H:%M')}")
                return

    def check_schedule(self):
        """Проверка времени для запуска запланированного бэкапа"""
        if not self.btn_schedule.isChecked():
            return
            
        now = datetime.now()
        current_day = now.day
        target_time = self.time_edit.time()
        
        if self.last_backup_day == current_day:
            return
            
        if not self.days_checkboxes[now.weekday()].isChecked():
            return
            
        if now.hour == target_time.hour() and now.minute == target_time.minute():
            self.perform_scheduled_backup()
            self.last_backup_day = current_day
            self.update_next_backup_time()

    def test_scheduled_backup(self):
        """Тестовый запуск запланированного бэкапа"""
        if not self.connection:
            QMessageBox.warning(self, "Ошибка", "Сначала подключитесь к серверу!")
            return
            
        db = self.db_combo_schedule.currentText()
        if not db:
            QMessageBox.warning(self, "Ошибка", "Выберите базу данных")
            return
            
        reply = QMessageBox.question(self, "Тестовый бэкап", 
                                   f"Выполнить тестовый бэкап базы '{db}'?",
                                   QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.perform_scheduled_backup()

    def perform_scheduled_backup(self):
        db = self.db_combo_schedule.currentText()
        if not db or not self.connection: 
            return
        
        path = self.backup_path.text()
        if not path:
            QMessageBox.warning(self, "Ошибка", "Укажите путь для бэкапов в настройках")
            return
            
        if not path.endswith("\\") and not path.endswith("/"): 
            path += "\\"
            
        server_address = self.server_input.text()
        server_name = server_address.split('\\')[0].split('.')[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{path}{server_name}_{db}_SCHEDULED_{timestamp}.bak"
        
        sql = f"BACKUP DATABASE [{db}] TO DISK='{filename}' WITH COMPRESSION, INIT, CHECKSUM"
        self.run_worker([sql], f"Авто-бэкап '{db}'")
        
        # Обновляем список файлов
        self.refresh_backup_files()

    # Вкладка: Файлы бэкапов
    def init_backup_files_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        # Панель управления
        control_panel = QGroupBox("Управление файлами бэкапов")
        control_layout = QVBoxLayout()
        
        # Путь для файлов
        path_layout = QHBoxLayout()
        self.files_path_edit = QLineEdit(DEFAULT_BACKUP_PATH)
        self.files_path_edit.setPlaceholderText("Сетевой путь к папке с бэкапами")
        
        btn_browse = QPushButton("Обзор")
        btn_browse.clicked.connect(self.browse_backup_folder)
        btn_refresh = QPushButton("🔄 Обновить")
        btn_refresh.clicked.connect(self.refresh_backup_files)
        btn_test_path = QPushButton("Проверить")
        btn_test_path.clicked.connect(self.test_files_path)
        
        path_layout.addWidget(QLabel("Папка с бэкапами:"))
        path_layout.addWidget(self.files_path_edit, 1)
        path_layout.addWidget(btn_browse)
        path_layout.addWidget(btn_refresh)
        path_layout.addWidget(btn_test_path)
        
        # Фильтры
        filter_layout = QHBoxLayout()
        self.filter_server = QLineEdit()
        self.filter_server.setPlaceholderText("Фильтр по серверу")
        self.filter_db = QLineEdit()
        self.filter_db.setPlaceholderText("Фильтр по базе данных")
        self.filter_date = QLineEdit()
        self.filter_date.setPlaceholderText("Фильтр по дате (ГГГГ-ММ-ДД)")
        
        btn_clear_filters = QPushButton("Очистить фильтры")
        btn_clear_filters.clicked.connect(self.clear_filters)
        
        filter_layout.addWidget(QLabel("Фильтры:"))
        filter_layout.addWidget(self.filter_server)
        filter_layout.addWidget(self.filter_db)
        filter_layout.addWidget(self.filter_date)
        filter_layout.addWidget(btn_clear_filters)
        
        # События изменения фильтров
        self.filter_server.textChanged.connect(self.apply_filters)
        self.filter_db.textChanged.connect(self.apply_filters)
        self.filter_date.textChanged.connect(self.apply_filters)
        
        control_layout.addLayout(path_layout)
        control_layout.addLayout(filter_layout)
        control_panel.setLayout(control_layout)
        layout.addWidget(control_panel)

        # Таблица файлов
        self.files_table = QTableWidget()
        self.files_table.setColumnCount(7)
        self.files_table.setHorizontalHeaderLabels(["Имя файла", "Сервер", "База", "Размер", "Дата создания", "Тип", "Полный путь"])
        self.files_table.horizontalHeader().setStretchLastSection(True)
        self.files_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.files_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.files_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.files_table.customContextMenuRequested.connect(self.show_files_context_menu)
        self.files_table.itemSelectionChanged.connect(self.update_selected_count)
        
        # Настройка ширины колонок
        self.files_table.setColumnWidth(0, 250)  # Имя файла
        self.files_table.setColumnWidth(1, 120)  # Сервер
        self.files_table.setColumnWidth(2, 120)  # База
        self.files_table.setColumnWidth(3, 100)  # Размер
        self.files_table.setColumnWidth(4, 150)  # Дата
        self.files_table.setColumnWidth(5, 80)   # Тип
        self.files_table.setColumnWidth(6, 300)  # Путь
        
        layout.addWidget(self.files_table)

        # Панель действий
        action_panel = QHBoxLayout()
        
        btn_open_folder = QPushButton("📂 Открыть папку")
        btn_open_folder.clicked.connect(self.open_backup_folder)
        btn_open_folder.setObjectName("BlueBtn")
        
        btn_download = QPushButton("⬇️ Скачать выбранное")
        btn_download.clicked.connect(self.download_selected_files)
        btn_download.setObjectName("GreenBtn")
        
        btn_delete = QPushButton("🗑️ Удалить выбранное")
        btn_delete.clicked.connect(self.delete_selected_files)
        btn_delete.setObjectName("RedBtn")
        
        btn_use_for_restore = QPushButton("↩️ Использовать для восстановления")
        btn_use_for_restore.clicked.connect(self.use_file_for_restore)
        btn_use_for_restore.setObjectName("YellowBtn")
        
        action_panel.addWidget(btn_open_folder)
        action_panel.addWidget(btn_download)
        action_panel.addWidget(btn_delete)
        action_panel.addWidget(btn_use_for_restore)
        action_panel.addStretch()
        
        layout.addLayout(action_panel)
        
        # Статистика
        stats_layout = QHBoxLayout()
        self.lbl_total_files = QLabel("Всего файлов: 0")
        self.lbl_total_size = QLabel("Общий размер: 0 GB")
        self.lbl_selected_count = QLabel("Выбрано: 0")
        
        stats_layout.addWidget(self.lbl_total_files)
        stats_layout.addWidget(self.lbl_total_size)
        stats_layout.addWidget(self.lbl_selected_count)
        stats_layout.addStretch()
        
        layout.addLayout(stats_layout)
        
        self.tabs.addTab(tab, "📁 Файлы Бэкапов")

    def browse_backup_folder(self):
        """Выбор папки с бэкапами"""
        folder = QFileDialog.getExistingDirectory(self, "Выберите папку с бэкапами", 
                                                 DEFAULT_BACKUP_PATH)
        if folder:
            self.files_path_edit.setText(folder)
            self.refresh_backup_files()

    def test_files_path(self):
        """Проверка пути к файлам"""
        path = self.files_path_edit.text()
        if not path:
            QMessageBox.warning(self, "Ошибка", "Укажите путь для проверки")
            return
            
        if os.path.exists(path):
            try:
                files = os.listdir(path)
                bak_files = [f for f in files if f.lower().endswith('.bak')]
                
                info = f"✅ Путь доступен:\n{path}\n\n"
                info += f"Всего файлов в папке: {len(files)}\n"
                info += f"Файлов .bak: {len(bak_files)}"
                
                if bak_files:
                    # Показываем несколько примеров
                    info += "\n\nПримеры файлов:\n"
                    for i, f in enumerate(bak_files[:5]):
                        info += f"  • {f}\n"
                    if len(bak_files) > 5:
                        info += f"  ... и еще {len(bak_files) - 5} файлов"
                
                QMessageBox.information(self, "Проверка пути", info)
                
                # Автоматически обновляем список файлов
                self.refresh_backup_files()
                
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось прочитать папку:\n{str(e)}")
        else:
            QMessageBox.warning(self, "Ошибка", f"Путь не существует:\n{path}")

    def refresh_backup_files(self):
        """Обновление списка файлов бэкапов из указанной папки"""
        path = self.files_path_edit.text()
        if not path or not os.path.exists(path):
            return
            
        try:
            files = []
            for filename in os.listdir(path):
                if filename.lower().endswith('.bak'):
                    filepath = os.path.join(path, filename)
                    stat = os.stat(filepath)
                    
                    # Парсим имя файла для извлечения информации
                    # Формат: Сервер_База_Дата_Время.bak
                    name_parts = filename[:-4].split('_')
                    
                    server_name = name_parts[0] if len(name_parts) > 0 else "Неизвестно"
                    db_name = name_parts[1] if len(name_parts) > 1 else "Неизвестно"
                    
                    # Определяем тип бэкапа по имени
                    backup_type = "Полный"
                    if 'DIFF' in filename.upper() or 'DIFFERENTIAL' in filename.upper():
                        backup_type = "Диф"
                    elif 'LOG' in filename.upper():
                        backup_type = "Лог"
                    elif 'SCHEDULED' in filename.upper():
                        backup_type = "План"
                    
                    # Пытаемся извлечь дату из имени файла
                    file_date = ""
                    if len(name_parts) >= 3:
                        date_str = name_parts[2]
                        try:
                            # Пробуем разные форматы дат
                            if len(date_str) >= 8:
                                # YYYYMMDD или YYYYMMDD_HHMMSS
                                year = date_str[0:4]
                                month = date_str[4:6]
                                day = date_str[6:8]
                                file_date = f"{day}.{month}.{year}"
                        except:
                            pass
                    
                    # Если не удалось из имени, берем дату модификации
                    if not file_date:
                        file_date = datetime.fromtimestamp(stat.st_mtime).strftime("%d.%m.%Y %H:%M")
                    
                    files.append({
                        'name': filename,
                        'server': server_name,
                        'database': db_name,
                        'size': stat.st_size,
                        'date': file_date,
                        'path': filepath,
                        'type': backup_type,
                        'mtime': stat.st_mtime
                    })
            
            # Сортируем по дате (новые сверху)
            files.sort(key=lambda x: x['mtime'], reverse=True)
            
            # Отображаем в таблице
            self.files_table.setRowCount(0)
            
            total_size = 0
            for i, file_info in enumerate(files):
                self.files_table.insertRow(i)
                
                # Имя файла
                self.files_table.setItem(i, 0, QTableWidgetItem(file_info['name']))
                
                # Сервер
                self.files_table.setItem(i, 1, QTableWidgetItem(file_info['server']))
                
                # База данных
                self.files_table.setItem(i, 2, QTableWidgetItem(file_info['database']))
                
                # Размер
                size = file_info['size']
                total_size += size
                
                if size >= 1024**3:  # GB
                    size_str = f"{size/(1024**3):.2f} GB"
                elif size >= 1024**2:  # MB
                    size_str = f"{size/(1024**2):.2f} MB"
                elif size >= 1024:  # KB
                    size_str = f"{size/1024:.2f} KB"
                else:
                    size_str = f"{size} B"
                
                size_item = QTableWidgetItem(size_str)
                size_item.setData(Qt.UserRole, size)  # Сохраняем оригинальный размер для сортировки
                self.files_table.setItem(i, 3, size_item)
                
                # Дата
                self.files_table.setItem(i, 4, QTableWidgetItem(file_info['date']))
                
                # Тип
                type_item = QTableWidgetItem(file_info['type'])
                if file_info['type'] == "Полный":
                    type_item.setForeground(QColor('#4CAF50'))
                elif file_info['type'] == "Диф":
                    type_item.setForeground(QColor('#2196F3'))
                elif file_info['type'] == "Лог":
                    type_item.setForeground(QColor('#FF9800'))
                self.files_table.setItem(i, 5, type_item)
                
                # Полный путь
                path_item = QTableWidgetItem(file_info['path'])
                path_item.setToolTip(file_info['path'])
                self.files_table.setItem(i, 6, path_item)
            
            # Обновляем статистику
            total_size_gb = total_size / (1024**3)
            self.lbl_total_files.setText(f"Всего файлов: {len(files)}")
            self.lbl_total_size.setText(f"Общий размер: {total_size_gb:.2f} GB")
            self.update_selected_count()
            
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить файлы:\n{str(e)}")

    def clear_filters(self):
        """Очистка всех фильтров"""
        self.filter_server.clear()
        self.filter_db.clear()
        self.filter_date.clear()
        
        # Показываем все строки
        for row in range(self.files_table.rowCount()):
            self.files_table.setRowHidden(row, False)

    def apply_filters(self):
        """Применение фильтров к таблице файлов"""
        server_filter = self.filter_server.text().lower()
        db_filter = self.filter_db.text().lower()
        date_filter = self.filter_date.text().lower()
        
        visible_count = 0
        
        for row in range(self.files_table.rowCount()):
            hide = False
            
            server_item = self.files_table.item(row, 1)
            db_item = self.files_table.item(row, 2)
            date_item = self.files_table.item(row, 4)
            
            server = server_item.text().lower() if server_item else ""
            db = db_item.text().lower() if db_item else ""
            date = date_item.text().lower() if date_item else ""
            
            if server_filter and server_filter not in server:
                hide = True
            if db_filter and db_filter not in db:
                hide = True
            if date_filter and date_filter not in date:
                hide = True
                
            self.files_table.setRowHidden(row, hide)
            
            if not hide:
                visible_count += 1
        
        self.lbl_total_files.setText(f"Отфильтровано: {visible_count}")

    def update_selected_count(self):
        """Обновление счетчика выбранных файлов"""
        selected_count = len(self.files_table.selectedItems()) // self.files_table.columnCount()
        self.lbl_selected_count.setText(f"Выбрано: {selected_count}")

    def show_files_context_menu(self, position):
        menu = QMenu()
        
        open_folder_action = menu.addAction("📂 Открыть папку с файлом")
        download_action = menu.addAction("⬇️ Скачать файл")
        delete_action = menu.addAction("🗑️ Удалить файл")
        restore_action = menu.addAction("↩️ Использовать для восстановления")
        menu.addSeparator()
        copy_path_action = menu.addAction("📋 Копировать путь")
        show_info_action = menu.addAction("ℹ️ Информация о файле")
        
        action = menu.exec_(self.files_table.mapToGlobal(position))
        
        if action == open_folder_action:
            self.open_selected_file_folder()
        elif action == download_action:
            self.download_selected_files()
        elif action == delete_action:
            self.delete_selected_files()
        elif action == restore_action:
            self.use_file_for_restore()
        elif action == copy_path_action:
            self.copy_selected_file_path()
        elif action == show_info_action:
            self.show_file_info()

    def get_selected_files(self):
        """Получение списка выбранных файлов"""
        selected_files = []
        selected_rows = set()
        
        for item in self.files_table.selectedItems():
            row = item.row()
            if row not in selected_rows:
                selected_rows.add(row)
                
                name_item = self.files_table.item(row, 0)
                path_item = self.files_table.item(row, 6)
                db_item = self.files_table.item(row, 2)
                
                if path_item and name_item:
                    selected_files.append({
                        'path': path_item.text(),
                        'name': name_item.text(),
                        'database': db_item.text() if db_item else "Неизвестно"
                    })
        
        return selected_files

    def open_backup_folder(self):
        """Открытие папки с бэкапами в проводнике"""
        path = self.files_path_edit.text()
        if os.path.exists(path):
            if platform.system() == "Windows":
                os.startfile(path)
            elif platform.system() == "Darwin":  # macOS
                subprocess.Popen(["open", path])
            else:  # Linux
                subprocess.Popen(["xdg-open", path])
        else:
            QMessageBox.warning(self, "Ошибка", f"Папка не существует:\n{path}")

    def open_selected_file_folder(self):
        """Открытие папки с выбранным файлом"""
        files = self.get_selected_files()
        if files:
            path = os.path.dirname(files[0]['path'])
            if os.path.exists(path):
                if platform.system() == "Windows":
                    os.startfile(path)
                elif platform.system() == "Darwin":
                    subprocess.Popen(["open", path])
                else:
                    subprocess.Popen(["xdg-open", path])

    def download_selected_files(self):
        """Скачивание выбранных файлов"""
        files = self.get_selected_files()
        if not files:
            QMessageBox.warning(self, "Ошибка", "Выберите файлы для скачивания")
            return
            
        dest_folder = QFileDialog.getExistingDirectory(self, "Выберите папку для сохранения")
        if not dest_folder:
            return
            
        try:
            for file_info in files:
                src = file_info['path']
                filename = file_info['name']
                dst = os.path.join(dest_folder, filename)
                
                # Копируем файл
                shutil.copy2(src, dst)
                
            QMessageBox.information(self, "Успех", f"Скачано {len(files)} файлов в папку:\n{dest_folder}")
            
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при копировании файлов:\n{str(e)}")

    def delete_selected_files(self):
        """Удаление выбранных файлов"""
        files = self.get_selected_files()
        if not files:
            QMessageBox.warning(self, "Ошибка", "Выберите файлы для удаления")
            return
            
        file_list = "\n".join([f['name'] for f in files])
        reply = QMessageBox.question(self, "Подтверждение удаления",
                                   f"Вы точно хотите удалить {len(files)} файлов?\n\n{file_list}\n\n"
                                   "⚠️ Эта операция необратима!",
                                   QMessageBox.Yes | QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            try:
                for file_info in files:
                    os.remove(file_info['path'])
                    
                QMessageBox.information(self, "Успех", f"Удалено {len(files)} файлов")
                self.refresh_backup_files()  # Обновляем список
                
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Ошибка при удалении файлов:\n{str(e)}")

    def use_file_for_restore(self):
        """Использование выбранного файла для восстановления"""
        files = self.get_selected_files()
        if not files:
            QMessageBox.warning(self, "Ошибка", "Выберите файл для восстановления")
            return
            
        if len(files) > 1:
            QMessageBox.warning(self, "Предупреждение", "Выберите только один файл для восстановления")
            return
            
        file_info = files[0]
        
        # Устанавливаем путь к файлу во вкладке восстановления
        self.file_path_restore.setText(file_info['path'])
        
        # Пытаемся определить базу данных из имени файла
        db_name = file_info['database']
        if db_name != "Неизвестно":
            index = self.db_combo_restore.findText(db_name, Qt.MatchFixedString)
            if index >= 0:
                self.db_combo_restore.setCurrentIndex(index)
        
        # Переключаемся на вкладку восстановления
        self.tabs.setCurrentIndex(2)
        
        QMessageBox.information(self, "Файл выбран", 
                              f"Файл '{file_info['name']}' выбран для восстановления базы '{db_name}'.\n"
                              "Проверьте настройки восстановления и нажмите кнопку 'Восстановить'.")

    def copy_selected_file_path(self):
        """Копирование пути к выбранному файлу в буфер обмена"""
        files = self.get_selected_files()
        if files:
            clipboard = QGuiApplication.clipboard()
            clipboard.setText(files[0]['path'])
            self.status_label.setText("Путь скопирован в буфер обмена")
            self.status_label.setStyleSheet("color: #4CAF50;")

    def show_file_info(self):
        """Показать подробную информацию о файле"""
        files = self.get_selected_files()
        if not files:
            return
            
        file_info = files[0]
        
        # Получаем полную информацию о файле
        try:
            stat = os.stat(file_info['path'])
            size_mb = stat.st_size / (1024 * 1024)
            size_gb = size_mb / 1024
            created_date = datetime.fromtimestamp(stat.st_ctime).strftime("%d.%m.%Y %H:%M:%S")
            modified_date = datetime.fromtimestamp(stat.st_mtime).strftime("%d.%m.%Y %H:%M:%S")
            
            info_text = f"""
            <h3>Информация о файле бэкапа</h3>
            <table>
            <tr><td><b>Имя файла:</b></td><td>{file_info['name']}</td></tr>
            <tr><td><b>База данных:</b></td><td>{file_info['database']}</td></tr>
            <tr><td><b>Путь:</b></td><td>{file_info['path']}</td></tr>
            <tr><td><b>Размер:</b></td><td>{size_mb:.2f} MB ({size_gb:.2f} GB)</td></tr>
            <tr><td><b>Дата создания:</b></td><td>{created_date}</td></tr>
            <tr><td><b>Дата изменения:</b></td><td>{modified_date}</td></tr>
            </table>
            """
            
            QMessageBox.information(self, "Информация о файле", info_text)
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Не удалось получить информацию о файле:\n{str(e)}")

    # Общие методы
    def run_worker(self, cmds, name):
        self.lock_ui(True)
        self.worker = Worker(self.conn_str_cache, cmds, name)
        self.worker.progress.connect(self.update_status)
        self.worker.finished.connect(self.on_worker_finished)
        self.worker.start()

    def update_status(self, msg):
        self.status_label.setText(msg)

    def on_worker_finished(self, success, msg):
        self.lock_ui(False)
        if success:
            QMessageBox.information(self, "✅ Успешно", msg)
            self.status_label.setText("Операция завершена успешно")
            self.status_label.setStyleSheet("color: #4caf50; font-weight: bold;")
            
            # Обновляем данные после успешной операции
            if self.connection:
                self.load_databases_with_sizes()
            self.refresh_backup_files()
        else:
            QMessageBox.critical(self, "❌ Ошибка", msg)
            self.status_label.setText("Произошла ошибка")
            self.status_label.setStyleSheet("color: #f44336; font-weight: bold;")

    def lock_ui(self, lock):
        self.tabs.setEnabled(not lock)
        self.progress_bar.setVisible(lock)
        if lock: 
            self.progress_bar.setRange(0, 0)
        else: 
            self.progress_bar.setRange(0, 100)

    def show_about(self):
        text = f"""
        <h3>SQL Server Backup Manager v2.2</h3>
        <p>Профессиональный инструмент для управления резервными копиями MS SQL Server.</p>
        
        <h4>Основные функции:</h4>
        <ul>
            <li>Массовое создание бэкапов баз данных с отображением размеров</li>
            <li>Восстановление баз из бэкапов</li>
            <li>Планировщик автоматических бэкапов</li>
            <li>Просмотр файлов бэкапов из сетевой папки</li>
            <li>Управление файлами бэкапов (скачивание, удаление)</li>
        </ul>
        
        <p><b>Разработчик:</b> Tigran</p>
        <p><b>GitHub:</b> <a href='https://github.com/TigranSo/SQL_Server_Backup_Manager'>TigranSo/SQL_Server_Backup_Manager</a></p>
        <p><b>Telegram:</b> @tigran_so</p>
        
        <hr>
        <p style='color: #888; font-size: 10px;'>
        Используется: PySide6, pyodbc, python-dotenv<br>
        Лицензия: MIT
        </p>
        """
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("О программе")
        msg_box.setTextFormat(Qt.RichText)
        msg_box.setText(text)
        msg_box.setIconPixmap(QIcon("logo.png").pixmap(64, 64))
        msg_box.exec()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    # шрифт по умолчанию
    font = QFont("Segoe UI", 10)
    app.setFont(font)
    
    window = BackupApp()
    window.show()
    sys.exit(app.exec())
