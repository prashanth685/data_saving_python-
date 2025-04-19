from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QLabel, QHBoxLayout, QComboBox, QTextEdit, 
                            QScrollArea, QPushButton, QMessageBox, QDialog, QTableWidget, QTableWidgetItem)
from PyQt5.QtCore import Qt, QTimer
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from collections import deque
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class DataTableDialog(QDialog):
    def __init__(self, db, project_name, filename, parent=None, on_delete_callback=None):
        super().__init__(parent)
        self.db = db
        self.project_name = project_name
        self.filename = filename
        self.on_delete_callback = on_delete_callback
        self.setWindowTitle(f"Data Details for {filename}")
        self.setStyleSheet("background-color: #2c3e50; color: white;")
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        self.setLayout(layout)

        self.table = QTableWidget()
        self.table.setStyleSheet("background-color: #34495e; color: white; border: 1px solid #1a73e8;")
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Frame Index", "No. of Channels", "Sampling Rate", "Sampling Size", "Updated Time"])
        self.table.horizontalHeader().setStyleSheet("color: black; font-weight: bold;")
        self.table.setSelectionMode(QTableWidget.NoSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        
        self.populate_table()
        layout.addWidget(self.table)

        button_layout = QHBoxLayout()
        delete_button = QPushButton("Delete")
        delete_button.setStyleSheet("background-color: #e63946; color: white; padding: 5px; border-radius: 3px;")
        delete_button.clicked.connect(self.delete_data)
        button_layout.addWidget(delete_button)

        close_button = QPushButton("Close")
        close_button.setStyleSheet("background-color: #e63946; color: white; padding: 5px; border-radius: 3px;")
        close_button.clicked.connect(self.close)
        button_layout.addWidget(close_button)

        layout.addLayout(button_layout)
        self.setMinimumSize(800, 500)

    def populate_table(self):
        query = {"filename": self.filename, "project_name": self.project_name}
        data = list(self.db.timeview_collection.find(query).sort("frameIndex", 1))
        
        self.table.setRowCount(len(data))
        for row, item in enumerate(data):
            self.table.setItem(row, 0, QTableWidgetItem(str(item.get("frameIndex", ""))))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.get("numberOfChannels", ""))))
            sampling_rate = item.get("samplingRate", "")
            self.table.setItem(row, 2, QTableWidgetItem(f"{sampling_rate:.2f}" if sampling_rate else ""))
            self.table.setItem(row, 3, QTableWidgetItem(str(item.get("samplingSize", ""))))
            created_at = item.get("createdAt", "")
            self.table.setItem(row, 4, QTableWidgetItem(created_at if created_at else ""))
        
        self.table.resizeColumnsToContents()

    def delete_data(self):
        reply = QMessageBox.question(self, "Confirm Deletion", 
                                     f"Are you sure you want to delete all data for {self.filename}?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No:
            return

        result = self.db.timeview_collection.delete_many({"filename": self.filename, "project_name": self.project_name})
        if result.deleted_count > 0:
            logging.info(f"Deleted {result.deleted_count} records for filename {self.filename}")
            QMessageBox.information(self, "Success", f"Deleted data for {self.filename}")
            if self.on_delete_callback:
                self.on_delete_callback(self.filename)
            self.close()
        else:
            logging.error(f"Failed to delete data for {self.filename}")
            QMessageBox.warning(self, "Error", f"Failed to delete data for {self.filename}")

class TimeViewFeature:
    def __init__(self, parent, db, project_name):
        self.parent = parent
        self.db = db
        self.project_name = project_name
        self.widget = QWidget()
        self.mqtt_tag = None
        self.initial_buffer_size = 4096
        self.time_view_buffers = [deque(maxlen=self.initial_buffer_size) for _ in range(4)]
        self.time_view_timestamps = deque(maxlen=self.initial_buffer_size * 4)
        self.timer = QTimer(self.widget)
        self.timer.timeout.connect(self.update_time_view_plot)
        self.figure = plt.Figure(figsize=(10, 8))
        self.canvas = FigureCanvas(self.figure)
        self.dragging = False
        self.press_x = None
        self.last_data_time = None
        self.data_rate = 1.0
        self.annotations = []
        self.is_saving = False
        self.frame_index = 0
        self.filename_counter = 1
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        self.widget.setLayout(layout)

        header = QLabel(f"TIME VIEW FOR {self.project_name.upper()}")
        header.setStyleSheet("color: white; font-size: 26px; font-weight: bold; padding: 8px;")
        layout.addWidget(header, alignment=Qt.AlignCenter)

        self.time_widget = QWidget()
        self.time_layout = QVBoxLayout()
        self.time_widget.setLayout(self.time_layout)
        self.time_widget.setStyleSheet("background-color: #2c3e50; border-radius: 5px; padding: 10px;")

        tag_layout = QHBoxLayout()
        tag_label = QLabel("Select Tag:")
        tag_label.setStyleSheet("color: white; font-size: 16px;")
        self.tag_combo = QComboBox()
        tags_data = list(self.db.tags_collection.find({"project_name": self.project_name}))
        if not tags_data:
            self.tag_combo.addItem("No Tags Available")
        else:
            for tag in tags_data:
                self.tag_combo.addItem(tag["tag_name"])
        self.tag_combo.setStyleSheet("background-color: #34495e; color: white; border: 1px solid #1a73e8; padding: 15px")
        self.tag_combo.currentTextChanged.connect(self.setup_time_view_plot)

        tag_layout.addWidget(tag_label)
        tag_layout.addWidget(self.tag_combo)
        tag_layout.addStretch()
        self.time_layout.addLayout(tag_layout)

        save_layout = QHBoxLayout()
        filename_label = QLabel("Saved Files:")
        filename_label.setStyleSheet("color: white; font-size: 16px;")
        self.filename_combo = QComboBox()
        self.filename_combo.setStyleSheet("background-color: #34495e; color: white; border: 1px solid #1a73e8; padding: 15px;")
        self.refresh_filenames()
        self.filename_combo.currentTextChanged.connect(self.open_data_table)

        self.start_save_button = QPushButton("Start Saving")
        self.start_save_button.setStyleSheet("background-color: #1a73e8; color: white; padding: 5px; border-radius: 3px;")
        self.start_save_button.clicked.connect(self.start_saving)
        
        self.stop_save_button = QPushButton("Stop Saving")
        self.stop_save_button.setStyleSheet("background-color: #e63946; color: white; padding: 5px; border-radius: 3px;")
        self.stop_save_button.clicked.connect(self.stop_saving)
        self.stop_save_button.setEnabled(False)

        save_layout.addWidget(filename_label)
        save_layout.addWidget(self.filename_combo)
        save_layout.addWidget(self.start_save_button)
        save_layout.addWidget(self.stop_save_button)
        save_layout.addStretch()
        self.time_layout.addLayout(save_layout)

        self.time_layout.addWidget(self.canvas)

        self.time_result = QTextEdit()
        self.time_result.setReadOnly(True)
        self.time_result.setStyleSheet("background-color: #34495e; color: white; border-radius: 5px; padding: 10px;")
        self.time_result.setMinimumHeight(50)
        self.time_result.setText(
            f"Time View for {self.project_name}: Select a tag to start real-time plotting.\n"
            "Click 'Start Saving' to save data to database with auto-generated filename."
        )
        self.time_layout.addWidget(self.time_result)
        self.time_layout.addStretch()

        scroll_area = QScrollArea()
        scroll_area.setWidget(self.time_widget)
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet("background-color: #2c3e50; border: none;")
        scroll_area.setMinimumHeight(400)
        layout.addWidget(scroll_area)

        if tags_data:
            self.tag_combo.setCurrentIndex(0)
            self.setup_time_view_plot(self.tag_combo.currentText())

    def refresh_filenames(self):
        filenames = self.db.timeview_collection.distinct("filename", {"project_name": self.project_name})
        self.filename_combo.clear()
        for filename in sorted(filenames):
            self.filename_combo.addItem(filename)
        self.filename_combo.addItem(f"data{self.filename_counter} (Next)")
        self.filename_combo.setCurrentText(f"data{self.filename_counter} (Next)")

    def open_data_table(self, selected_filename):
        if "(Next)" in selected_filename:
            return
        
        if not self.db.timeview_collection.find_one({"filename": selected_filename, "project_name": self.project_name}):
            return
        
        dialog = DataTableDialog(self.db, self.project_name, selected_filename, self.widget, self.on_delete)
        dialog.exec_()

    def on_delete(self, deleted_filename):
        self.refresh_filenames()
        self.time_result.setText(f"Deleted data for {deleted_filename}")

    def start_saving(self):
        if not self.mqtt_tag or self.mqtt_tag == "No Tags Available":
            self.time_result.setText("Error: Please select a valid tag!")
            QMessageBox.warning(self.widget, "Error", "Please select a valid tag!")
            return
        
        filename = f"data{self.filename_counter}"
        self.is_saving = True
        self.frame_index = 0
        self.start_save_button.setEnabled(False)
        self.stop_save_button.setEnabled(True)
        self.time_result.setText(f"Started saving data for {self.mqtt_tag} to {filename}")
        logging.info(f"Started saving data for {self.mqtt_tag} with filename {filename}")
        QMessageBox.information(self.widget, "Success", 
                               f"Started saving data for {self.mqtt_tag} to {filename}")

    def stop_saving(self):
        self.is_saving = False
        self.start_save_button.setEnabled(True)
        self.stop_save_button.setEnabled(False)
        filename = f"data{self.filename_counter}"
        self.filename_counter += 1
        self.refresh_filenames()
        self.time_result.setText(f"Stopped saving data for {self.mqtt_tag}")
        logging.info(f"Stopped saving data for {self.mqtt_tag}")
        QMessageBox.information(self.widget, "Success", 
                               f"Stopped saving data for {self.mqtt_tag}")

    def setup_time_view_plot(self, tag_name):
        if not self.project_name or not tag_name or tag_name == "No Tags Available":
            logging.warning("No project or valid tag selected for Time View!")
            self.time_result.setText("No project or valid tag selected for Time View.")
            return

        self.mqtt_tag = tag_name
        self.timer.stop()
        self.timer.setInterval(100)
        for buf in self.time_view_buffers:
            buf.clear()
        self.time_view_timestamps.clear()
        self.last_data_time = None
        self.data_rate = 1.0
        self.is_saving = False
        self.start_save_button.setEnabled(True)
        self.stop_save_button.setEnabled(False)
        self.frame_index = 0

        self.figure.clear()
        self.axes = [self.figure.add_subplot(4, 1, i+1) for i in range(4)]
        self.lines = []
        for i, ax in enumerate(self.axes):
            line, = ax.plot([], [], f'C{i}-', linewidth=1.5)
            self.lines.append(line)
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.set_ylabel(f"Slot {i+1}", rotation=90, labelpad=10)
            ax.yaxis.set_label_position("right")
            ax.yaxis.tick_right()
            ax.set_xlabel("Time (HH:MM:SS.mmm)")
            ax.set_xlim(0, 1)  # 1-second window
            ax.set_xticks(np.linspace(0, 1, 11))  # 100ms intervals

        self.figure.subplots_adjust(left=0.05, right=0.85, top=0.95, bottom=0.15, hspace=0.4)
        self.canvas.setMinimumSize(1000, 800)
        self.time_widget.setMinimumSize(1000, 850)
        self.canvas.draw()
        self.timer.start()
        self.time_result.setText(f"Plotting real-time data for {self.mqtt_tag}. Click 'Start Saving' to save to database.")

    def split_and_store_values(self, values, timestamp, save=False):
        try:
            values = [float(v) for v in values]
            for i in range(0, min(len(values), 4096 * 4), 4):
                if i + 3 < len(values):
                    self.time_view_buffers[0].append(values[i])
                    self.time_view_buffers[1].append(values[i + 1])
                    self.time_view_buffers[2].append(values[i + 2])
                    self.time_view_buffers[3].append(values[i + 3])
                    self.time_view_timestamps.extend([timestamp] * 4)
        except (ValueError, TypeError) as e:
            logging.error(f"Error converting values to float: {e}")
            self.time_result.setText(f"Error processing data: {e}")
            return

        # Save to database only if explicitly saving
        if self.is_saving:
            filename = f"data{self.filename_counter}"
            message_data = {
                "project_name": self.project_name,
                "topic": self.mqtt_tag,
                "filename": filename,
                "frameIndex": self.frame_index,
                "numberOfChannels": 4,
                "samplingRate": self.data_rate if self.data_rate > 0 else None,
                "samplingSize": 16,
                "messageFrequency": self.data_rate / 4 if self.data_rate > 0 else None,
                "slot1": "channel1",
                "slot2": "channel2",
                "slot3": "channel3",
                "slot4": "channel4",
                "message": values[:min(len(values), 4096 * 4)],
                "createdAt": timestamp
            }
            success, msg = self.db.save_timeview_message(self.project_name, message_data)
            if success:
                self.frame_index += 1
                logging.debug(f"Saved timeview message for {self.mqtt_tag}, frame {self.frame_index - 1}")
                self.time_result.append(f"Saved frame {self.frame_index - 1} to {filename}")
            else:
                self.time_result.setText(f"Error saving data: {msg}")
                logging.error(f"Failed to save data: {msg}")

        logging.debug(f"Processed {len(values)} values for plotting")

    def adjust_buffer_size(self):
        window_size = 1.0  # Fixed 1-second window
        if self.data_rate > 0:
            new_buffer_size = max(int(self.data_rate * window_size * 2), 100)
            if new_buffer_size != self.time_view_buffers[0].maxlen:
                for i in range(4):
                    self.time_view_buffers[i] = deque(self.time_view_buffers[i], maxlen=new_buffer_size)
                self.time_view_timestamps = deque(self.time_view_timestamps, maxlen=new_buffer_size * 4)
                logging.debug(f"Adjusted buffer size to {new_buffer_size} based on data rate {self.data_rate:.2f} samples/s")

    def generate_y_ticks(self, values):
        if not values or not all(np.isfinite(v) for v in values):
            return np.arange(0, 100, 10)
        y_max = max(values)
        y_min = min(values)
        padding = (y_max - y_min) * 0.1 if y_max != y_min else 1
        y_max += padding
        y_min -= padding
        range_val = y_max - y_min
        step = max(range_val / 10, 1)
        step = np.ceil(step / 0.5) * 0.5
        ticks = []
        current = np.floor(y_min / step) * step
        while current <= y_max:
            ticks.append(current)
            current += step
        return ticks

    def update_time_view_plot(self):
        if not self.project_name or not self.mqtt_tag:
            self.time_result.setText("No project or tag selected for Time View.")
            return

        current_buffer_size = len(self.time_view_buffers[0])
        if current_buffer_size < 2:
            self.time_result.setText(
                f"Waiting for data for {self.mqtt_tag} (Buffer: {current_buffer_size}/{self.time_view_buffers[0].maxlen})."
            )
            return

        self.adjust_buffer_size()

        for i, (ax, line) in enumerate(zip(self.axes, self.lines)):
            window_size = 1.0  # Fixed 1-second window
            self.samples_per_window = min(current_buffer_size, int(self.data_rate * window_size))
            if self.samples_per_window < 2:
                self.samples_per_window = 2

            window_values = list(self.time_view_buffers[i])[-self.samples_per_window:]
            window_timestamps = list(self.time_view_timestamps)[-self.samples_per_window * 4:][::4]

            if not window_values or not all(np.isfinite(v) for v in window_values):
                self.time_result.setText(f"Invalid data received for {self.mqtt_tag}. Buffer: {current_buffer_size}")
                ax.set_ylim(0, 100)
                ax.set_yticks(self.generate_y_ticks([]))
                line.set_data([], [])
                continue

            time_points = np.linspace(0, 1, self.samples_per_window)  # 1-second window
            line.set_data(time_points, window_values)
            y_max = max(window_values)
            y_min = min(window_values)
            padding = (y_max - y_min) * 0.1 if y_max != y_min else 1
            ax.set_ylim(y_min - padding, y_max + padding)
            ax.set_yticks(self.generate_y_ticks(window_values))

            if window_timestamps:
                try:
                    latest_dt = datetime.fromisoformat(window_timestamps[-1])
                    time_labels = []
                    tick_positions = np.linspace(0, 1, 11)  # 100ms intervals
                    for tick in tick_positions:
                        delta_seconds = tick - 1  # Shift to align with latest timestamp
                        tick_dt = latest_dt + timedelta(seconds=delta_seconds)
                        milliseconds = tick_dt.microsecond // 1000
                        time_labels.append(f"{tick_dt.strftime('%H:%M:%S')}.{milliseconds:03d}")
                    ax.set_xticks(tick_positions)
                    ax.set_xticklabels(time_labels, rotation=0)
                except ValueError as e:
                    logging.error(f"Error parsing timestamp: {e}")
                    self.time_result.setText(f"Timestamp error: {e}")

        self.canvas.draw_idle()
        latest_values = [list(buf)[-1] if buf else None for buf in self.time_view_buffers]
        save_status = "Saving" if self.is_saving else "Not saving"
        self.time_result.setText(
            f"Plotting {self.mqtt_tag} | Latest values: {latest_values} | Status: {save_status}"
        )

    def on_data_received(self, tag_name, values):
        if tag_name != self.mqtt_tag:
            return

        current_time = datetime.now()
        if self.last_data_time:
            time_delta = (current_time - self.last_data_time).total_seconds()
            if time_delta > 0:
                self.data_rate = len(values) / time_delta / 4
        self.last_data_time = current_time

        # Process data for plotting, save only if is_saving is True
        self.split_and_store_values(values, current_time.isoformat())
        logging.debug(f"Received {len(values)} values for {tag_name}, Data rate: {self.data_rate:.2f} samples/s")

    def get_widget(self):
        return self.widget