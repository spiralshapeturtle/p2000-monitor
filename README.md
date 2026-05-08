# P2000 Monitor

An asynchronous Python-based P2000 message receiver and processor. This tool captures P2000 (FLEX) messages used by Dutch emergency services using an RTL-SDR dongle and forwards them to Home Assistant and/or MQTT brokers.

## Functionality

This script serves as a bridge between the radio frequency (169.65 MHz) and your smart home system. It utilizes `rtl_fm` to tune the radio and `multimon-ng` to decode the digital FLEX signal. The decoded messages are then parsed, reassembled if they are fragmented, and pushed to configured endpoints.

### Key Features
- **Asynchronous Architecture:** Built with `asyncio` for high-performance, non-blocking message processing.
- **Message Defragmentation:** Automatically joins multi-part messages into a single coherent notification.
- **Home Assistant Integration:** Sends messages directly to Home Assistant as sensor states via the REST API.
- **Multiple MQTT Support:** Can push messages to up to two different MQTT brokers simultaneously.
- **Resilient Connections:** Includes automatic reconnection logic with exponential backoff for both HTTP and MQTT.
- **Buffered Output:** Maintains an internal queue to ensure no messages are lost during temporary network outages.

## Requirements

### Hardware
- An RTL-SDR compatible USB dongle.
- An antenna tuned for 169-170 MHz (standard DVB-T antennas often work well enough).

### Software
- **Linux Environment:** (Tested on Raspberry Pi/Debian).
- **RTL-SDR Tools:** `sudo apt install rtl-sdr`
- **multimon-ng:** `sudo apt install multimon-ng`
- **Python 3.7+**

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/spiralshapeturtle/p2000-monitor.git
   cd p2000-monitor
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the application:**
   Copy the example configuration and edit it with your settings:
   ```bash
   cp config.example config.ini
   nano config.ini
   ```

## Configuration

The `config.ini` file is divided into several sections:

- **[main]**: General settings like debug mode and buffer sizes.
- **[rtl-sdr]**: The command used to run the decoder. You might need to adjust `-d 0` if you have multiple RTL-SDR devices.
- **[home-assistant]**: Your HA URL and a Long-Lived Access Token.
- **[mqtt]**: Primary MQTT broker settings.
- **[mqtt2]**: Optional secondary MQTT broker (disabled by default).

## Usage

You can start the script directly:
```bash
python3 p2000.py
```

### Running in the background (PM2)
To keep the script running permanently, it is recommended to use a process manager like [PM2](https://pm2.keymetrics.io/):
```bash
pm2 start p2000.py --interpreter python3 --name p2000-monitor
pm2 save
```

## Disclaimer
This project is for educational and personal use only. Ensure you comply with local regulations regarding the reception of radio signals.
