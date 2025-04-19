import math
import struct
import time
import paho.mqtt.publish as publish
from PyQt5.QtCore import QTimer, QObject
from PyQt5.QtWidgets import QApplication
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class MQTTPublisher(QObject):
    def __init__(self, broker, topics):
        super().__init__()
        self.broker = broker
        self.topics = topics if isinstance(topics, list) else [topics]
        self.count = 0

        self.frequency = 10
        self.amplitude = (46537 - 16390) / 2
        self.offset = (46537 + 16390) / 2

        self.sample_rate = 16384
        self.time_per_message = 1.0
        self.current_time = 0.0

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.publish_message)
        self.timer.start(1000)  # Publish every 1 second

    def publish_message(self):
        if self.count < 200:  # Limiting to 1 message for testing
            values = []
            for i in range(self.sample_rate):
                t = self.current_time + (i / self.sample_rate)
                value = self.offset + self.amplitude * math.sin(2 * math.pi * self.frequency * t)
                values.append(int(round(value)))  # Convert to integer

            self.current_time += 1
            # Pack values as uint16_t (2 bytes per value)
            binary_message = struct.pack(f"{len(values)}H", *values)
            logging.debug(f"Generated {len(values)} values, binary size: {len(binary_message)} bytes")

            for topic in self.topics:
                try:
                    publish.single(topic, binary_message, hostname=self.broker, qos=1)
                    logging.info(f"[{self.count}] Published to {topic}: {len(values)} uint16_t values")
                except Exception as e:
                    logging.error(f"Failed to publish to {topic}: {str(e)}")

            self.count += 1
        else:
            self.timer.stop()
            logging.info("Publishing stopped after 200 message.")

if __name__ == "__main__":
    app = QApplication([])
    broker = "192.168.1.173"
    topics = ["sarayu/tag2/topic2|m/s"]
    mqtt_publisher = MQTTPublisher(broker, topics)
    app.exec_()


# import math
# import struct
# import time
# import paho.mqtt.publish as publish
# from PyQt5.QtCore import QTimer, QObject
# from PyQt5.QtWidgets import QApplication
# import logging

# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# class MQTTPublisher(QObject):
#     def __init__(self, broker, topics):
#         super().__init__()
#         self.broker = broker
#         self.topics = topics if isinstance(topics, list) else [topics]
#         self.count = 0

#         self.frequency = 10
#         self.amplitude = (46537 - 16390) / 2
#         self.offset = (46537 + 16390) / 2

#         self.sample_rate = 4096
#         self.time_per_message = 1.0
#         self.current_time = 0.0

#         self.timer = QTimer(self)
#         self.timer.timeout.connect(self.publish_message)
#         self.timer.start(1000)  # Publish every 1 second

#     def publish_message(self):
#         if self.count < 200:  # Limiting to 200 messages
#             values = []
#             for i in range(self.sample_rate):
#                 t = self.current_time + (i / self.sample_rate)
#                 value = self.offset + self.amplitude * math.sin(2 * math.pi * self.frequency * t)
#                 values.append(int(round(value)))  # Convert to integer

#             self.current_time += 1

#             # looping values into groups of 4 channels (i, i+1, i+2, i+3)
#             for i in range(0, len(values), 4):
#                 group = values[i:i+4]  # Get up to 4 values starting at index i
#                 if len(group) == 4:  # 4 channels
#                     # Pack group as uint16_t (2 bytes per value)
#                     binary_message = struct.pack("4H", *group)
#                     logging.debug(f"Generated group at index {i} with {len(group)} values, binary size: {len(binary_message)} bytes")

#                     for topic in self.topics:
#                         try:
#                             publish.single(topic, binary_message, hostname=self.broker, qos=1)
#                             logging.info(f"[{self.count}] Published to {topic}: group at index {i} with {len(group)} uint16_t values")
#                         except Exception as e:
#                             logging.error(f"Failed to publish to {topic}: {str(e)}")

#             self.count += 1
#         else:
#             self.timer.stop()
#             logging.info("Publishing stopped after 200 messages.")

# if __name__ == "__main__":
#     app = QApplication([])
#     broker = "192.168.1.173"
#     topics = ["sarayu/tag2/topic2|m/s"]
#     mqtt_publisher = MQTTPublisher(broker, topics)
#     app.exec_()