# MZF-CAN2MQTT

Reads CAN frames from a Waveshare 2-CH-CAN-TO-ETH adapter over TCP, decodes
configurable bit fields, and publishes changes to MQTT and/or InfluxDB.

## Features

- Declarative field mapping via `config.toml` — no code changes needed for new CAN IDs
- Supports bit-field extraction (single-byte and multi-byte little-endian), signed
  32-bit integers with scaling, and 32-bit IEEE 754 floats
- Named label maps translate raw values to human-readable strings (e.g. `0x2` → `green`)
- Hysteresis per field to suppress noise on analog values
- MQTT and InfluxDB are both optional — missing libraries or omitted config sections are
  handled gracefully

---

## Requirements

- Python 3.11+ **or** Docker
- A Waveshare 2-CH-CAN-TO-ETH adapter reachable over TCP

---

## Installation

### Docker (recommended)

```bash
docker compose up -d
```

The container is built from the local `Dockerfile`. `config.toml` is mounted read-only
from the project directory — edit it and restart the container to apply changes.

```bash
docker compose restart
docker compose logs -f
```

### Manual

```bash
pip install -r requirements.txt
python processor.py            # uses config.toml in the current directory
python processor.py /path/to/config.toml
```

---

## Configuration

All settings live in `config.toml`.

### Connection

```toml
[connection]
host = "10.2.101.11"   # Waveshare adapter IP
port = 20001
```

### MQTT

```toml
[mqtt]
host         = "10.2.1.11"
port         = 1883
username     = "user"
password     = "secret"
topic_prefix = "mzf"   # topics become  <prefix>/<message_name>/<field_name>
# client_id  = "can2mqtt"   # optional, auto-generated if omitted
```

Omit the entire `[mqtt]` section to disable MQTT.

### InfluxDB

```toml
[influxdb]
url    = "http://10.2.1.11:8086"
token  = "my-token"
org    = "my-org"
bucket = "can_data"
```

Omit the entire `[influxdb]` section to disable InfluxDB.  
Each changed field is written as one InfluxDB point with the message name as the
measurement and the CAN frame timestamp (nanoseconds).

### Label maps

Named lookup tables that translate raw integer values to strings.

```toml
[label_maps.button_color]
"0x0" = "off"
"0x1" = "red"
"0x2" = "green"
# …

[label_maps.switch_state]
"0x0" = "off"
"0x1" = "on"
```

### Messages and fields

MQTT and InfluxDB publishing are **opt-in**: nothing is published unless explicitly
enabled. Set `mqtt` and `influxdb` at the message level to enable all fields at once,
then override individual fields as needed.

```toml
[[messages]]
can_id   = 0xA006
name     = "front_panel_color"
mqtt     = true     # publish all fields in this message to MQTT
influxdb = true     # write all fields in this message to InfluxDB

  [[messages.fields]]
  name       = "blaulicht"
  bit_offset = 0      # absolute LSB position in the 8-byte LE payload
  length     = 3
  label_map  = "button_color"

  [[messages.fields]]
  name       = "debug_field"
  bit_offset = 60
  length     = 3
  mqtt       = false   # override: exclude this field even though the message has mqtt = true
```

**Precedence:** field-level `mqtt`/`influxdb` overrides the message-level value.
Omitting a key at the field level inherits from the message. Omitting it at the message
level defaults to `false`.

#### Field types

| `type`      | Required keys                                              | Description                                                     |
|-------------|------------------------------------------------------------|-----------------------------------------------------------------|
| *(omitted)* | `bit_offset` + `length`  **or**  `byte` + `bit` + `length` | Bit extraction from the LE payload                              |
| `int32`     | `byte`, optional `scale`                                   | Signed 32-bit LE integer, multiplied by `scale` (default `1.0`) |
| `float32`   | `byte`                                                     | IEEE 754 single-precision LE float                              |

#### Bit extraction variants

- **`bit_offset` + `length`** — absolute bit position counting from the LSB of the
  64-bit little-endian integer. Use this for fields that cross byte boundaries.
- **`byte` + `bit` + `length`** — bit position within a single byte.

#### Hysteresis

```toml
  [[messages.fields]]
  name       = "strom"
  byte       = 4
  type       = "int32"
  scale      = 0.01
  hysteresis = 0.05   # only publish when value changes by or more than 0.05
```

Only meaningful for `int32` and `float32` fields. The last *published* value is used as
the reference, so slow drift never accumulates through sub-threshold steps.

---

## Output

Changes are printed to stdout with a timestamp:

```
[19:12:18.014] 0x0000A006  front_panel_color
  blaulicht: init -> green  [0x2 (0b010)]
  blaulicht_heck: green  [0x2 (0b010)] -> off  [0x0 (0b000)]
```

Fields with a matching `label_map` entry show the label followed by the raw hex/binary
value in brackets. Numeric (`int32`/`float32`) fields show three decimal places.
