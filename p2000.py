#!/usr/bin/env python3
import aiohttp
import asyncio
import configparser
import json
import logging
import os
import re
import signal
import sys
import time
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime
from asyncio import Event, Queue
from typing import Optional, Dict, List, Any, Tuple, Union
import asyncio_mqtt as aiomqtt

VERSION = "1.0"
CFGFILE = "config.ini"

# Simple pattern matching pipe-separated fields
FLEX_PATTERN = re.compile(
    r"^FLEX\|([^|]*)\|([^|]*)\|([^|]*)\|([^|]*)\|ALN\|([^|]*)"
)

class BaseHandler(ABC):
    """Base class for message handlers with common functionality."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connected = False
        self.reconnect_delay = 1
        self.max_reconnect_delay = 30
        self.connection_attempts = 0
        self.last_reconnect_attempt = None
        self.pending_messages = deque(maxlen=config.get("buffer_size", 1000))
        
    @abstractmethod
    async def setup_connection(self):
        """Initialize connection - to be implemented by subclasses."""
        pass
        
    @abstractmethod
    async def send_single_message(self, *args) -> bool:
        """Send a single message - to be implemented by subclasses."""
        pass
        
    async def handle_connection_failure(self):
        """Common connection failure handling with exponential backoff."""
        if self.connection_attempts == 0:
            logging.info(f"Initial {self.__class__.__name__} connection attempt delayed - will retry automatically")
        else:
            logging.warning(f"{self.__class__.__name__} connection lost after {self.connection_attempts} successful connections")
        
        self.connected = False
        self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
        
    async def process_pending_messages(self):
        """Process queued messages with unified handling."""
        if not self.connected or not self.pending_messages:
            return
            
        initial_pending = len(self.pending_messages)
        logging.info(f"{self.__class__.__name__} attempting to send {initial_pending} pending messages")
        
        successful_sends = 0
        failed_sends = 0
        
        while self.pending_messages and self.connected:
            message = self.pending_messages.popleft()
            try:
                if await self.send_single_message(*message):
                    successful_sends += 1
                    
                    if successful_sends % 10 == 0:
                        remaining = len(self.pending_messages)
                        logging.info(f"{self.__class__.__name__} buffer progress: {successful_sends}/{initial_pending} sent, {remaining} remaining")
                else:
                    self.pending_messages.append(message)
                    break
                    
            except Exception as e:
                logging.error(f"Failed to send pending message: {str(e)}")
                self.pending_messages.append(message)
                failed_sends += 1
                if failed_sends >= 3:
                    remaining = len(self.pending_messages)
                    logging.warning(f"{self.__class__.__name__} buffer processing stopped after {successful_sends} messages due to errors. {remaining} messages still pending.")
                    break
                    
        if successful_sends == initial_pending:
            logging.info(f"{self.__class__.__name__} buffer successfully emptied - all {successful_sends} pending messages delivered")
        else:
            logging.info(f"{self.__class__.__name__} delivered {successful_sends} messages, {len(self.pending_messages)} remaining in buffer")
            
    async def stop(self):
        """Base implementation of stop method."""
        pending_count = len(self.pending_messages)
        if pending_count > 0:
            logging.warning(f"{self.__class__.__name__} shutting down with {pending_count} pending messages")
        self.connected = False

class AsyncHTTPHandler(BaseHandler):
    """HTTP handler implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            "Authorization": f"Bearer {self.config['token']}",
            "Content-Type": "application/json"
        }
        
    async def setup_connection(self):
        """Implementation of HTTP connection setup."""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                
            connector = aiohttp.TCPConnector(
                limit=20, # Meer parallelle verbindingen
                force_close=False,
                enable_cleanup_closed=True,
                keepalive_timeout=60.0  # Verbindingen 60 seconden in pool houden als ze niet gebruikt worden
            )
            
            timeout = aiohttp.ClientTimeout(
                total=10,
                connect=3,
                sock_read=5
            )
            
            self.session = aiohttp.ClientSession(
                headers=self.headers,
                connector=connector,
                timeout=timeout,
                raise_for_status=True
            )
            
            await self.check_connection()
            logging.info("HTTP connection setup complete")
            
        except Exception as e:
            logging.error(f"HTTP error setting up session: {str(e)}")
            self.connected = False
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
                
    async def check_connection(self):
        """Test connection to Home Assistant."""
        try:
            async with self.session.get(f"{self.config['baseurl']}/api/") as response:
                await response.read()
                self.connected = True
                self.connection_attempts += 1
                self.reconnect_delay = 1
                self.last_reconnect_attempt = None
                
                if self.pending_messages:
                    await self.process_pending_messages()
                    
        except aiohttp.ClientError as e:
            self.connected = False
            logging.error(f"HTTP connection check failed: {str(e)}")
            await self.handle_connection_failure()
                
    async def send_single_message(self, endpoint: str, payload: str) -> bool:
        """Implementation of HTTP message sending with improved connection handling."""
        if not isinstance(payload, str):
            logging.error(f"Invalid payload type: {type(payload)}. Expected string.")
            return False
            
        try:
            if not self.connected or not self.session or self.session.closed:
                current_time = datetime.now()
                if (self.last_reconnect_attempt is None or
                    (current_time - self.last_reconnect_attempt).total_seconds() >= self.reconnect_delay):
                    
                    logging.info("HTTP attempting to reconnect before sending message...")
                    await self.check_connection()
                    self.last_reconnect_attempt = current_time
                
                if not self.connected:
                    self.pending_messages.append((endpoint, payload))
                    pending_count = len(self.pending_messages)
                    logging.warning(f"HTTP not connected, message queued (pending: {pending_count})")
                    return False
                
            url = f"{self.config['baseurl']}{endpoint}"
            if not url.startswith(('http://', 'https://')):
                logging.error(f"Invalid URL: {url}")
                return False
                
            async with self.session.post(url, json=json.loads(payload)) as response:
                await response.read()
                return True
                
        except Exception as e:
            logging.error(f"HTTP send error: {str(e)}")
            self.connected = False
            self.pending_messages.append((endpoint, payload))
            await self.handle_connection_failure()
            return False

    async def stop(self):
        """Enhanced HTTP handler cleanup."""
        await super().stop()
        try:
            if hasattr(self, 'session') and self.session is not None:
                if not self.session.closed:
                    try:
                        await self.session.close()
                    except Exception as e:
                        logging.error(f"Error closing HTTP session: {e}")
                
                # Ensure connector is properly closed if it exists
                if hasattr(self.session, '_connector') and self.session._connector is not None:
                    try:
                        await self.session._connector.close()
                    except Exception as e:
                        logging.error(f"Error closing HTTP connector: {e}")
                
            logging.info("HTTP client shutdown complete")
        except Exception as e:
            logging.error(f"HTTP Error during shutdown: {e}")
        finally:
            self.session = None

class AsyncMQTTHandler(BaseHandler):
    """MQTT handler implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client: Optional[aiomqtt.Client] = None
        self._connection_task = None
        self._shutdown = False
        
    async def setup_connection(self):
        """Implementation of MQTT connection setup."""
        try:
            if self._connection_task and not self._connection_task.done():
                self._connection_task.cancel()
                
            client_kwargs = {
                "hostname": self.config["server"],
                "port": self.config["port"],
                "client_id": f"p2000_receiver_{os.getpid()}",
                "clean_session": False,
                "keepalive": 15
            }
            
            if self.config["user"] and self.config["password"]:
                client_kwargs.update({
                    "username": self.config["user"],
                    "password": self.config["password"]
                })
                
            self.client = aiomqtt.Client(**client_kwargs)
            self._connection_task = asyncio.create_task(self._manage_connection())
            
        except Exception as e:
            logging.error(f"MQTT setup failed: {str(e)}")
            await asyncio.sleep(2)
            if not self._shutdown:
                asyncio.create_task(self.setup_connection())
                
    async def _manage_connection(self):
        """Manage MQTT connection with improved reconnection handling."""
        while not self._shutdown:
            try:
                if not self.connected:
                    current_time = datetime.now()
                    if (self.last_reconnect_attempt is None or
                        (current_time - self.last_reconnect_attempt).total_seconds() >= self.reconnect_delay):
                        
                        logging.info("MQTT attempting to connect...")
                        self.last_reconnect_attempt = current_time
                        
                        try:
                            if self.client:
                                try:
                                    await self.client.disconnect()
                                except:
                                    pass
                                self.client = None
                                
                            # Create new client instance for each connection attempt
                            client_kwargs = {
                                "hostname": self.config["server"],
                                "port": self.config["port"],
                                "client_id": f"p2000_receiver_{os.getpid()}_{int(time.time())}",
                                "clean_session": True,
                                "keepalive": 15
                            }
                            
                            if self.config["user"] and self.config["password"]:
                                client_kwargs.update({
                                    "username": self.config["user"],
                                    "password": self.config["password"]
                                })
                                
                            self.client = aiomqtt.Client(**client_kwargs)
                            await self.client.connect()
                            self.connected = True
                            self.connection_attempts += 1
                            self.reconnect_delay = 1
                            logging.info(f"MQTT connected successfully after {self.connection_attempts} attempts")
                            
                            # Process pending messages after successful connection
                            if self.pending_messages:
                                await self.process_pending_messages()
                                
                        except Exception as e:
                            self.connected = False
                            logging.error(f"MQTT connection attempt failed: {str(e)}")
                            self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                            await asyncio.sleep(self.reconnect_delay)
                            
                await asyncio.sleep(1)
                    
            except Exception as e:
                logging.error(f"Error in MQTT connection management: {str(e)}")
                self.connected = False
                await asyncio.sleep(self.reconnect_delay)
                
            if self._shutdown:
                break
                
    async def send_single_message(self, topic: str, payload: str, qos: int = 1) -> bool:
        """Implementation of MQTT message sending with improved connection handling."""
        try:
            if not self.connected or not self.client:
                self.pending_messages.append((topic, payload, qos))
                pending_count = len(self.pending_messages)
                logging.warning(f"MQTT not connected, message queued (pending: {pending_count})")

                # Ensure connection management is running
                if self._connection_task is None or self._connection_task.done():
                    self._connection_task = asyncio.create_task(self._manage_connection())
                return False

            await self.client.publish(topic, payload, qos=qos)
            return True

        except Exception as e:
            logging.error(f"MQTT send error: {str(e)}")
            self.connected = False
            self.pending_messages.append((topic, payload, qos))
            
            # Trigger reconnection
            if self._connection_task is None or self._connection_task.done():
                self._connection_task = asyncio.create_task(self._manage_connection())
            return False
            
    async def stop(self):
        """Clean shutdown of MQTT connection."""
        await super().stop()
        try:
            self._shutdown = True
            
            if self._connection_task and not self._connection_task.done():
                self._connection_task.cancel()
                try:
                    await self._connection_task
                except asyncio.CancelledError:
                    pass
                    
            if self.client and self.connected:
                try:
                    await asyncio.wait_for(self.client.disconnect(), timeout=2.0)
                except asyncio.TimeoutError:
                    logging.error("MQTT disconnect timed out")
                    
            logging.info("MQTT client shutdown complete")
            
        except Exception as e:
            logging.error(f"Error during MQTT shutdown: {e}")
        finally:
            self._connection_task = None
            self.client = None

class MessageItem:
    """Message container with slots for efficiency."""
    
    __slots__ = ("timestamp", "message_raw", "body", "capcodes", "receivers")
    
    def __init__(self, message_raw: str, timestamp: str, body: str, capcodes: List[str]):
        self.timestamp = timestamp
        self.message_raw = message_raw
        self.body = body
        self.capcodes = capcodes
        self.receivers = " ".join(capcodes)

class AsyncP2000Receiver:
    """Main P2000 message receiver and processor."""
    
    def __init__(self):
        print(f"Initializing Async P2000 Receiver v{VERSION}...")
        self._cleanup_lock = asyncio.Lock()  # Add cleanup lock
        self._cleanup_done = False  # Add cleanup state tracking
        
        self.config = self._load_config()
        self.shutdown_event = Event()
        self.buffer_size = self.config.getint("main", "buffer_size", fallback=1000)
        self.messages: Queue[MessageItem] = Queue(maxsize=self.buffer_size)
        self.rtl_process = None
        self._tasks = []
        self.mqtt_handler = None
        self.http_handler = None
        self.device_ready = Event()
        
        # Configure logging
        self._setup_logging()
        
        # Load RTL-SDR command
        self.rtlfm_cmd = self.config.get("rtl-sdr", "cmd")
        logging.info(f"RTL-SDR command: {self.rtlfm_cmd}")
        
        # Initialize handlers based on config
        self.use_hass = self.config.getboolean("home-assistant", "enabled")
        self.use_mqtt = self.config.getboolean("mqtt", "enabled")
        
    def _load_config(self) -> configparser.ConfigParser:
        """Load or create configuration file."""
        config = configparser.ConfigParser()
        if config.read(CFGFILE):
            return config
            
        # Create default config
        config["main"] = {
            "debug": "true",
            "buffer_size": "1000",
            "group_prefix": "0020295"
        }
        
        config["rtl-sdr"] = {
            "cmd": "rtl_fm -f 169.65M -M fm -s 22050 | multimon-ng -a FLEX -t raw -"
        }
        
        config["home-assistant"] = {
            "enabled": "true",
            "baseurl": "http://homeassistant.local:8123",
            "token": "YOUR_TOKEN_HERE",
        }
        
        config["mqtt"] = {
            "enabled": "true",
            "mqtt_server": "192.168.1.100",
            "mqtt_port": "1883",
            "mqtt_user": "mqttuser",
            "mqtt_password": "password",
            "mqtt_topic": "p2000",
            "mqtt_qos": "1",
        }
        
        with open(CFGFILE, "w") as f:
            config.write(f)
            logging.info(f"Created new config file: {CFGFILE}")
            
        return config
        
    def _setup_logging(self):
        """Configure logging based on debug setting."""
        self.debug = self.config.getboolean("main", "debug")
        log_level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        
    def is_usb_error(self, error_text: str) -> bool:
        """Check if error indicates USB/hardware issues."""
        usb_errors = [
            "cb transfer status",
            "canceling",
            "device not found",
            "no compatible devices found",
            "USB error",
            "failed to open rtlsdr device",
            "unable to claim usb interface",
        ]
        return any(err in error_text.lower() for err in usb_errors)

    def is_startup_message(self, error_text: str) -> bool:
        """Identify normal startup messages."""
        startup_msgs = [
            "multimon-ng",
            "available demodulators:",
            "found",
            "using device",
            "tuner",
            "tuned to",
            "sampling at",
            "sample rate",
            "buffer size",
            "oversampling",
            "allocating",
            "copyright",
            "output at",
            "(c)",
            "realtek",
            "rtl2838",
            "sn:",
            "by tom sailer",
            "by elias oenal",
        ]
        return any(msg in error_text.lower() for msg in startup_msgs)

    def is_device_ready(self, error_text: str) -> bool:
        """Check if device is ready to receive messages."""
        ready_indicators = ["tuned to", "sampling at", "output at"]
        return any(indicator in error_text.lower() for indicator in ready_indicators)

    async def setup_home_assistant(self):
        """Initialize Home Assistant connection."""
        try:
            logging.info("Setting up Home Assistant connection...")
            await asyncio.sleep(0.5)  # Small delay for network readiness
            
            self.ha_config = {
                "baseurl": self.config.get("home-assistant", "baseurl"),
                "token": self.config.get("home-assistant", "token"),
                "buffer_size": self.buffer_size,
            }
            self.http_handler = AsyncHTTPHandler(self.ha_config)
            await self.http_handler.setup_connection()
            logging.info("Home Assistant connection setup complete")
            
        except Exception as e:
            logging.error(f"Error setting up Home Assistant: {e}")
            self.use_hass = False

    async def setup_mqtt(self):
        """Initialize MQTT connection."""
        try:
            logging.info("Setting up MQTT connection...")
            self.mqtt_config = {
                "server": self.config.get("mqtt", "mqtt_server"),
                "port": self.config.getint("mqtt", "mqtt_port"),
                "user": self.config.get("mqtt", "mqtt_user"),
                "password": self.config.get("mqtt", "mqtt_password"),
                "topic": self.config.get("mqtt", "mqtt_topic"),
                "qos": self.config.getint("mqtt", "mqtt_qos", fallback=1),
                "buffer_size": self.buffer_size,
            }
            self.mqtt_handler = AsyncMQTTHandler(self.mqtt_config)
            await self.mqtt_handler.setup_connection()
            logging.info("MQTT connection setup complete")
            
        except Exception as e:
            logging.error(f"Error setting up MQTT: {e}")
            self.use_mqtt = False

    async def post_message(self, msg: MessageItem):
        """Post message to configured endpoints."""
        try:
            mqtt_data = {
                "payload": {"message": msg.body, "address": msg.receivers}
            }

            ha_data = {
                "state": msg.body,
                "attributes": {
                    "address": msg.receivers,
                },
            }

            # Post to Home Assistant
            if self.use_hass and self.http_handler:
                try:
                    await self.http_handler.send_single_message(
                        "/api/states/sensor.p2000", 
                        json.dumps(ha_data)
                    )
                except Exception as e:
                    logging.error(f"HA Error: {e}")

            # Post to MQTT
            if self.use_mqtt and self.mqtt_handler:
                try:
                    topic = f"{self.mqtt_config['topic']}/sensor/p2000"
                    await self.mqtt_handler.send_single_message(
                        topic,
                        json.dumps(mqtt_data),
                        qos=self.mqtt_config["qos"],
                    )
                except Exception as e:
                    logging.error(f"MQTT Error: {e}")

        except Exception as e:
            logging.error(f"Error in post_message: {e}")

    async def process_messages(self):
        """Process messages from the queue in batches."""
        while not self.shutdown_event.is_set():
            try:
                # Get at least one message
                msg = await self.messages.get()
                batch = [msg]
                
                # Collect more messages if available (up to 9 more)
                for _ in range(9):
                    try:
                        msg = self.messages.get_nowait()
                        batch.append(msg)
                    except asyncio.QueueEmpty:
                        break
                
                # Process the batch
                await asyncio.gather(*[self.post_message(msg) for msg in batch])
                for _ in range(len(batch)):
                    self.messages.task_done()
                    
            except Exception as e:
                logging.error(f"Error processing message batch: {e}")
                await asyncio.sleep(1)

    async def _handle_stdout(self, stdout: asyncio.StreamReader):
        """Handle stdout from RTL-SDR process."""
        while True:
            try:
                line = await stdout.readline()
                if not line:
                    break

                line_str = line.decode().strip()
                if "FLEX" in line_str and "ALN" in line_str:
                    match = FLEX_PATTERN.search(line_str)
                    if match:
                        if not self.device_ready.is_set():
                            self.device_ready.set()
                            logging.info("First message received, device is ready")
                        await self.handle_message(line_str, match)
                        
            except Exception as e:
                logging.error(f"Error processing stdout: {e}")

    async def _handle_stderr(self, stderr: asyncio.StreamReader):
        """Handle stderr from RTL-SDR process."""
        while True:
            try:
                line = await stderr.readline()
                if not line:
                    break

                error_text = line.decode().strip()
                if not error_text:
                    continue

                if not self.device_ready.is_set() and self.is_device_ready(error_text):
                    self.device_ready.set()
                    logging.info("Device ready to receive messages")

                if self.is_usb_error(error_text):
                    logging.error(f"USB Error detected: {error_text}")
                    return
                elif self.is_startup_message(error_text):
                    logging.info(f"RTL-SDR: {error_text}")
                else:
                    logging.warning(f"RTL-SDR Error: {error_text}")
                    
            except Exception as e:
                logging.error(f"Error processing stderr: {e}")

    async def read_rtl_sdr(self):
        """Read and process RTL-SDR data with improved process handling."""
        try:
            while not self.shutdown_event.is_set():
                try:
                    self.rtl_process = await asyncio.create_subprocess_shell(
                        self.rtlfm_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )

                    # Create tasks for stdout and stderr
                    stdout_task = asyncio.create_task(self._handle_stdout(self.rtl_process.stdout))
                    stderr_task = asyncio.create_task(self._handle_stderr(self.rtl_process.stderr))

                    # Wait for process to complete or shutdown
                    done, pending = await asyncio.wait(
                        [stdout_task, stderr_task],
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    # Cancel pending tasks
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                    if self.shutdown_event.is_set():
                        break

                except Exception as e:
                    logging.error(f"Error in RTL-SDR process: {e}")
                    if not self.shutdown_event.is_set():
                        await asyncio.sleep(6)

        except asyncio.CancelledError:
            logging.info("RTL-SDR reader task cancelled")
        finally:
            if self.rtl_process and self.rtl_process.returncode is None:
                try:
                    self.rtl_process.terminate()
                    await asyncio.sleep(0.1)
                    if self.rtl_process.returncode is None:
                        self.rtl_process.kill()
                except Exception as e:
                    logging.error(f"Error cleaning up RTL-SDR process: {e}")

    async def handle_message(self, line: str, match: re.Match):
        """Process and queue a new message."""
        try:
            capcodes = match.group(4).strip().split()
            message = match.group(5).strip()

            if message and capcodes:
                msg = MessageItem(
                    message_raw=line.strip(),
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    body=message,
                    capcodes=capcodes,
                )
                await self.messages.put(msg)

                is_group = any(cap.startswith(self.config.get("main", "group_prefix")) 
                             for cap in capcodes)
                console_message = f"{message} - GROUP" if is_group else message
                print(f"[{msg.timestamp}] P2000: {console_message}")
                
        except Exception as e:
            logging.error(f"Error handling message: {e}")

    async def run(self):
        """Start the receiver with improved shutdown handling."""
        logging.info(f"P2000 Receiver v{VERSION} starting...")

        try:
            # Initialize handlers
            if self.use_hass:
                await self.setup_home_assistant()
            if self.use_mqtt:
                await self.setup_mqtt()

            # Create tasks
            self._tasks = [
                asyncio.create_task(self.process_messages()),
                asyncio.create_task(self.read_rtl_sdr())
            ]

            # Wait for shutdown signal
            await self.shutdown_event.wait()

        except Exception as e:
            logging.error(f"Error in run: {e}")
            raise
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Enhanced cleanup with proper resource handling and duplicate prevention."""
        async with self._cleanup_lock:
            if self._cleanup_done:
                return
            
            logging.info("Starting cleanup...")
            
            try:
                # Set shutdown event first to prevent new operations
                self.shutdown_event.set()
                
                # Cancel all running tasks
                for task in self._tasks:
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        
                # Clean up RTL-SDR process
                if self.rtl_process:
                    try:
                        if self.rtl_process.returncode is None:
                            self.rtl_process.terminate()
                            try:
                                await asyncio.wait_for(self.rtl_process.wait(), timeout=2.0)
                            except asyncio.TimeoutError:
                                self.rtl_process.kill()
                                await self.rtl_process.wait()
                    except Exception as e:
                        logging.error(f"Error during RTL-SDR cleanup: {e}")
                        
                # Clean up MQTT
                if self.mqtt_handler:
                    try:
                        await asyncio.wait_for(self.mqtt_handler.stop(), timeout=3.0)
                    except asyncio.TimeoutError:
                        logging.error("MQTT shutdown timed out")
                    except Exception as e:
                        logging.error(f"Error during MQTT cleanup: {e}")
                        
                # Clean up HTTP
                if self.http_handler:
                    try:
                        await asyncio.wait_for(self.http_handler.stop(), timeout=3.0)
                    except asyncio.TimeoutError:
                        logging.error("HTTP shutdown timed out")
                    except Exception as e:
                        logging.error(f"Error during HTTP cleanup: {e}")
                        
                # Process remaining messages in queue
                remaining_messages = self.messages.qsize()
                if remaining_messages > 0:
                    logging.warning(f"Shutdown with {remaining_messages} messages still in queue")
                    try:
                        while not self.messages.empty():
                            try:
                                self.messages.get_nowait()
                                self.messages.task_done()
                            except asyncio.QueueEmpty:
                                break
                    except Exception as e:
                        logging.error(f"Error clearing message queue: {e}")
                        
            except Exception as e:
                logging.error(f"Error during cleanup: {e}")
            finally:
                self._cleanup_done = True
                logging.info("Cleanup completed")


async def main():
    """Main entry point with improved error handling."""
    receiver = None
    try:
        receiver = AsyncP2000Receiver()
        
        def shutdown_handler():
            """Handle shutdown signals."""
            logging.info("Shutdown signal received")
            if not receiver.shutdown_event.is_set():
                receiver.shutdown_event.set()  # Just set the event, don't create new cleanup task

        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)

        await receiver.run()

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received")
    except Exception as e:
        logging.exception("Fatal error occurred")
        sys.exit(1)
    finally:
        if receiver:
            await receiver.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
