import sys
import os
import json
import pyodbc
from datetime import datetime
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                               QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                               QTableWidget, QTableWidgetItem, QComboBox, QMessageBox, 
                               QGroupBox, QTabWidget, QFileDialog, QCheckBox, QTimeEdit,
                               QProgressBar, QDialog, QFormLayout)
from PySide6.QtCore import Qt, QThread, Signal, QSettings, QTime, QTimer
from PySide6.QtGui import QIcon, QAction
from PySide6.QtCore import QSize
from PySide6.QtGui import QIcon

# Файл для хранения истории
SETTINGS_FILE = "connection_history.json"

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
            # autocommit=True обязателен для команд BACKUP/RESTORE
            conn = pyodbc.connect(self.conn_str, autocommit=True)
            cursor = conn.cursor()
            
            for sql in self.sql_commands:
                self.progress.emit(f"Выполнение: {sql[:60]}...")
                cursor.execute(sql)
                # Ожидание завершения операции SQL сервером
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
        self.resize(950, 700)
        icon = QIcon() # Привязка иконки
        icon.addFile(u"logo.png", QSize(), QIcon.Normal, QIcon.Off)
        self.setWindowIcon(icon)
    
        # Темная тема и стилизация (CSS)
        self.setStyleSheet("""
            QMainWindow { background-color: #2b2b2b; color: #ffffff; }
            QWidget { color: #ffffff; font-size: 14px; }
            QGroupBox { 
                font-weight: bold; 
                border: 1px solid #555; 
                border-radius: 6px; 
                margin-top: 10px; 
                padding-top: 10px;
            }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 3px; color: #4CAF50; }
            QLineEdit { 
                padding: 5px; 
                border-radius: 4px; 
                background-color: #3d3d3d; 
                color: white; 
                border: 1px solid #555; 
            }
            QTableWidget { 
                background-color: #3d3d3d; 
                color: white; 
                gridline-color: #555; 
                border: 1px solid #555;
            }
            QHeaderView::section { background-color: #444; padding: 4px; border: 1px solid #555; }
            QPushButton { 
                padding: 8px 16px; 
                border-radius: 4px; 
                background-color: #444; 
                color: white; 
                border: none;
            }
            QPushButton:hover { background-color: #555; }
            QPushButton#GreenBtn { background-color: #4CAF50; color: white; font-weight: bold; }
            QPushButton#GreenBtn:hover { background-color: #45a049; }
            QPushButton#BlueBtn { background-color: #2196F3; color: white; font-weight: bold; }
            QPushButton#BlueBtn:hover { background-color: #0b7dda; }
            QPushButton#RedBtn { background-color: #f44336; color: white; font-weight: bold; }
            QPushButton#RedBtn:hover { background-color: #d32f2f; }
            QComboBox { padding: 5px; background-color: #3d3d3d; border: 1px solid #555; }
            QTabWidget::pane { border: 1px solid #444; }
            QTabBar::tab { background: #333; color: #aaa; padding: 8px 20px; }
            QTabBar::tab:selected { background: #4CAF50; color: white; }
        """)

        # Меню
        menubar = self.menuBar()
        help_menu = menubar.addMenu("Справка")
        about_action = QAction("О программе", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

        # Основной контейнер
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Переменные состояния
        self.connection = None
        self.conn_str_cache = ""
        self.history = self.load_history()

        # Инициализация вкладок
        self.init_connection_tab()
        self.init_backup_tab()
        self.init_restore_tab()
        self.init_scheduler_tab()

        # Статус бар
        status_container = QWidget()
        status_layout = QHBoxLayout(status_container)
        status_layout.setContentsMargins(5, 5, 5, 5)
        
        self.status_label = QLabel("Готов к работе")
        self.status_label.setStyleSheet("color: #aaa;")
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("QProgressBar { border: 1px solid grey; border-radius: 5px; text-align: center; } QProgressBar::chunk { background-color: #4CAF50; }")
        
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.progress_bar)
        main_layout.addWidget(status_container)

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

    # Вкладка  Подключение 
    def init_connection_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # История
        hist_group = QGroupBox("Сохраненные подключения")
        hist_layout = QHBoxLayout()
        self.combo_history = QComboBox()
        self.combo_history.addItem("Новое подключение", None)
        for name, data in self.history.items():
            self.combo_history.addItem(name, data)
        self.combo_history.currentIndexChanged.connect(self.fill_connection_data)
        
        btn_delete_hist = QPushButton("Удалить из истории")
        btn_delete_hist.clicked.connect(self.delete_history_item)
        
        hist_layout.addWidget(QLabel("Выбрать:"))
        hist_layout.addWidget(self.combo_history, 1)
        hist_layout.addWidget(btn_delete_hist)
        hist_group.setLayout(hist_layout)
        layout.addWidget(hist_group)

        # Форма ввода
        form_group = QGroupBox("Параметры сервера")
        form_layout = QFormLayout()
        
        self.conn_name = QLineEdit()
        self.conn_name.setPlaceholderText("Название (например: Сервер 1С)")
        
        self.server_input = QLineEdit()
        self.server_input.setPlaceholderText("Адрес (IP или Hostname)")
        
        self.user_input = QLineEdit()
        self.user_input.setText("sa")
        
        self.pass_input = QLineEdit()
        self.pass_input.setEchoMode(QLineEdit.Password)
        
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
        self.tabs.addTab(tab, "🔌 Подключение")

    def fill_connection_data(self):
        data = self.combo_history.currentData()
        if data:
            self.conn_name.setText(self.combo_history.currentText())
            self.server_input.setText(data.get('server', ''))
            self.user_input.setText(data.get('user', ''))
            self.pass_input.setText(data.get('password', ''))
        else:
            self.conn_name.clear()
            self.server_input.clear()
            self.pass_input.clear()

    def delete_history_item(self):
        idx = self.combo_history.currentIndex()
        if idx > 0:
            name = self.combo_history.currentText()
            del self.history[name]
            self.save_history()
            self.combo_history.removeItem(idx)

    def connect_to_db(self):
        server = self.server_input.text()
        user = self.user_input.text()
        password = self.pass_input.text()
        name = self.conn_name.text()

        if not server or not user:
            QMessageBox.warning(self, "Ошибка", "Заполните поля сервера и пользователя")
            return

        # Строка подключения
        self.conn_str_cache = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={user};PWD={password};TrustServerCertificate=yes;'

        try:
            self.connection = pyodbc.connect(self.conn_str_cache)
            self.status_label.setText(f"Успешное подключение к {server}")
            self.status_label.setStyleSheet("color: #4CAF50; font-weight: bold;")
            
            # Сохраняем в историю
            if name:
                self.history[name] = {'server': server, 'user': user, 'password': password}
                self.save_history()
                if self.combo_history.findText(name) == -1:
                    self.combo_history.addItem(name, self.history[name])

            self.load_databases()
            self.tabs.setCurrentIndex(1) # Переключаем на вкладку бэкапа
            
        except Exception as e:
            self.status_label.setText("Ошибка подключения")
            self.status_label.setStyleSheet("color: #f44336;")
            QMessageBox.critical(self, "Ошибка подключения", f"Детали:\n{str(e)}\n\nПроверьте, включен ли SQL Server и доступен ли он по сети.")

    def load_databases(self):
        """Фильтр только активных баз"""
        cursor = self.connection.cursor()
        # Добавлено условие AND state_desc = 'ONLINE'
        # Это скрывает базы, которые в Offline, Suspect или Recovery, так как их нельзя бэкапить
        sql_query = """
            SELECT name 
            FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') 
            AND state_desc = 'ONLINE'
        """
        cursor.execute(sql_query)
        dbs = cursor.fetchall()
        
        self.db_table.setRowCount(0)
        self.db_combo_restore.clear()
        self.db_combo_schedule.clear()

        self.db_table.setRowCount(len(dbs))
        for i, db in enumerate(dbs):
            db_name = db[0]
            
            # Чекбокс в таблице
            chk = QTableWidgetItem()
            chk.setCheckState(Qt.Unchecked)
            self.db_table.setItem(i, 0, chk)
            
            # Имя базы
            self.db_table.setItem(i, 1, QTableWidgetItem(db_name))
            
            # Добавление в выпадающие списки других вкладок
            self.db_combo_restore.addItem(db_name)
            self.db_combo_schedule.addItem(db_name)

    # Вкладка  Бэкап 
    def init_backup_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        self.db_table = QTableWidget()
        self.db_table.setColumnCount(2)
        self.db_table.setHorizontalHeaderLabels(["", "Имя Базы Данных"])
        self.db_table.setColumnWidth(0, 40)
        self.db_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(QLabel("Выберите базы для резервного копирования:"))
        layout.addWidget(self.db_table)

        sett_group = QGroupBox("Настройки Бэкапа")
        sett_layout = QVBoxLayout()
        
        path_layout = QHBoxLayout()
        self.backup_path = QLineEdit("G:\\backup\\")
        self.backup_path.setPlaceholderText("Путь на сервере (например C:\\Backups\\)")
        path_layout.addWidget(QLabel("Папка:"))
        path_layout.addWidget(self.backup_path)
        
        opt_layout = QHBoxLayout()
        self.chk_compression = QCheckBox("Сжимать бэкап (COMPRESSION)")
        self.chk_compression.setChecked(True)
        self.chk_compression.setToolTip("Значительно уменьшает размер файла")
        
        self.chk_copy_only = QCheckBox("Только копия (COPY_ONLY)")
        self.chk_copy_only.setToolTip("Делает бэкап, не нарушая цепочку логов для разностных бэкапов")
        
        opt_layout.addWidget(self.chk_compression)
        opt_layout.addWidget(self.chk_copy_only)
        
        sett_layout.addLayout(path_layout)
        sett_layout.addLayout(opt_layout)
        sett_group.setLayout(sett_layout)
        layout.addWidget(sett_group)

        self.btn_backup = QPushButton("ЗАПУСТИТЬ БЭКАП")
        self.btn_backup.setObjectName("BlueBtn")
        self.btn_backup.setFixedHeight(45)
        self.btn_backup.clicked.connect(self.start_backup)
        layout.addWidget(self.btn_backup)
        
        self.tabs.addTab(tab, "Создание Бэкапа")

    def start_backup(self):
        if not self.connection:
            QMessageBox.warning(self, "Ошибка", "Сначала подключитесь к серверу!")
            return

        selected_dbs = []
        for i in range(self.db_table.rowCount()):
            if self.db_table.item(i, 0).checkState() == Qt.Checked:
                selected_dbs.append(self.db_table.item(i, 1).text())

        if not selected_dbs:
            QMessageBox.warning(self, "Ошибка", "Выберите хотя бы одну базу данных из списка.")
            return

        target_path = self.backup_path.text()
        if not target_path.endswith("\\") and not target_path.endswith("/"): 
            target_path += "\\"

        sql_commands = []
        for db in selected_dbs:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{target_path}{db}_{timestamp}.bak"
            
            # Формируем SQL команду
            cmd = f"BACKUP DATABASE [{db}] TO DISK = '{filename}' WITH INIT"
            if self.chk_compression.isChecked():
                cmd += ", COMPRESSION"
            if self.chk_copy_only.isChecked():
                cmd += ", COPY_ONLY"
            
            sql_commands.append(cmd)

        self.run_worker(sql_commands, "Массовый бэкап")

    # Вкладка  Восстановление 
    def init_restore_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        warn_label = QLabel("ВНИМАНИЕ: Восстановление полностью перезапишет текущую базу!")
        warn_label.setStyleSheet("color: #f44336; font-weight: bold; font-size: 16px;")
        layout.addWidget(warn_label)

        form = QFormLayout()
        
        self.db_combo_restore = QComboBox()
        self.file_path_restore = QLineEdit()
        self.file_path_restore.setPlaceholderText("Выберите .bak файл")
        btn_browse = QPushButton("...")
        btn_browse.setFixedWidth(40)
        btn_browse.clicked.connect(self.browse_bak_file)
        
        file_layout = QHBoxLayout()
        file_layout.addWidget(self.file_path_restore)
        file_layout.addWidget(btn_browse)

        form.addRow("База данных:", self.db_combo_restore)
        form.addRow("Файл бэкапа:", file_layout)
        
        layout.addLayout(form)
        
        self.chk_close_conns = QCheckBox("Принудительно отключить пользователей (SINGLE_USER)")
        self.chk_close_conns.setChecked(True)
        self.chk_close_conns.setToolTip("Необходимо, если в базе кто-то работает")
        self.chk_close_conns.setStyleSheet("color: #ffa726;")
        layout.addWidget(self.chk_close_conns)

        btn_restore = QPushButton("ВОССТАНОВИТЬ БАЗУ")
        btn_restore.setObjectName("RedBtn")
        btn_restore.setFixedHeight(45)
        btn_restore.clicked.connect(self.start_restore)
        layout.addWidget(btn_restore)
        
        layout.addStretch()
        self.tabs.addTab(tab, "Восстановление")

    def browse_bak_file(self):
        fname, _ = QFileDialog.getOpenFileName(self, "Выберите файл бэкапа", "", "Backup Files (*.bak)")
        if fname:
            self.file_path_restore.setText(fname)

    def start_restore(self):
        if not self.connection: return
        
        db_name = self.db_combo_restore.currentText()
        file_path = self.file_path_restore.text()
        
        if not file_path:
            QMessageBox.warning(self, "Ошибка", "Выберите файл бэкапа (.bak)")
            return

        reply = QMessageBox.question(self, "Подтверждение", 
                                     f"Вы ТОЧНО хотите восстановить базу {db_name}?\nВСЕ ТЕКУЩИЕ ДАННЫЕ БУДУТ УДАЛЕНЫ!",
                                     QMessageBox.Yes | QMessageBox.No)
        
        if reply == QMessageBox.No: return

        cmds = []
        # Переводим в Single User (выкидываем всех)
        if self.chk_close_conns.isChecked():
            cmds.append(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        
        # Само восстановление
        cmds.append(f"RESTORE DATABASE [{db_name}] FROM DISK = '{file_path}' WITH REPLACE")
        
        # Возвращаем Multi User
        if self.chk_close_conns.isChecked():
            cmds.append(f"ALTER DATABASE [{db_name}] SET MULTI_USER")

        self.run_worker(cmds, f"Восстановление {db_name}")

    # Вкладка Планировщик
    def init_scheduler_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        group = QGroupBox("Автоматический бэкап")
        gl = QFormLayout()
        
        self.db_combo_schedule = QComboBox()
        self.time_edit = QTimeEdit()
        self.time_edit.setTime(QTime.currentTime().addSecs(3600)) 
        
        self.btn_schedule = QPushButton("Активировать таймер")
        self.btn_schedule.setCheckable(True)
        self.btn_schedule.clicked.connect(self.toggle_schedule)
        self.btn_schedule.setFixedHeight(40)
        
        self.lbl_timer_status = QLabel("Таймер отключен")
        self.lbl_timer_status.setAlignment(Qt.AlignCenter)
        self.lbl_timer_status.setStyleSheet("font-size: 14px; color: grey;")

        gl.addRow("Какую базу:", self.db_combo_schedule)
        gl.addRow("Время запуска:", self.time_edit)
        group.setLayout(gl)
        
        layout.addWidget(group)
        layout.addWidget(self.btn_schedule)
        layout.addWidget(self.lbl_timer_status)
        layout.addStretch()
        
        self.tabs.addTab(tab, "Планировщик")
        
        self.timer = QTimer()
        self.timer.timeout.connect(self.check_time)
        self.timer.start(1000)

    def toggle_schedule(self, checked):
        if checked:
            self.btn_schedule.setText("Остановить таймер")
            self.btn_schedule.setStyleSheet("background-color: #f44336; color: white;")
            self.lbl_timer_status.setText(f"Ожидание запуска в {self.time_edit.time().toString()}")
            self.lbl_timer_status.setStyleSheet("color: #4CAF50;")
            self.scheduled_done = False
        else:
            self.btn_schedule.setText("Активировать таймер")
            self.btn_schedule.setStyleSheet("")
            self.lbl_timer_status.setText("Таймер отключен")
            self.lbl_timer_status.setStyleSheet("color: grey;")

    def check_time(self):
        if self.btn_schedule.isChecked() and not self.scheduled_done:
            current = QTime.currentTime()
            target = self.time_edit.time()
            if current.hour() == target.hour() and current.minute() == target.minute():
                self.scheduled_done = True
                self.perform_scheduled_backup()

    def perform_scheduled_backup(self):
        db = self.db_combo_schedule.currentText()
        if not db or not self.connection: return
        
        path = self.backup_path.text()
        if not path.endswith("\\") and not path.endswith("/"): path += "\\"
        filename = f"{path}{db}_AUTO_{datetime.now().strftime('%H%M')}.bak"
        
        sql = f"BACKUP DATABASE [{db}] TO DISK='{filename}' WITH COMPRESSION, INIT"
        self.run_worker([sql], f"Авто-бэкап {db}")
        self.btn_schedule.click()

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
            QMessageBox.information(self, "Успешно", msg)
            self.status_label.setText("Готово")
        else:
            QMessageBox.critical(self, "Ошибка", msg)
            self.status_label.setText("Произошла ошибка")

    def lock_ui(self, lock):
        self.tabs.setEnabled(not lock)
        self.progress_bar.setVisible(lock)
        if lock: self.progress_bar.setRange(0, 0)
        else: self.progress_bar.setRange(0, 100)

    def show_about(self):
        author_name = "Tigran"
        github_link = "https://github.com/TigranSo"
        telegram_link = "@tigran_so"
        
        text = f"""
        <h3>SQL Server Backup Manager</h3>
        <p>Версия 1.0</p>
        <p>Профессиональный инструмент для управления резервными копиями MS SQL Server.</p>
        <hr>
        <p><b>Разработчик:</b> {author_name}</p>
        <p><b>GitHub:</b> <a href='{github_link}'>{github_link}</a></p>
        <p><b>Telegram:</b> {telegram_link}</p>
        """
        QMessageBox.about(self, "О программе", text)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion") 
    window = BackupApp()
    window.show()
    sys.exit(app.exec())