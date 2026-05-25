import signal
import struct
import sys
import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from can_waveshare import WaveShareBus

sys.stdout.reconfigure(line_buffering=True)

try:
    import paho.mqtt.client as mqtt
    _MQTT_AVAILABLE = True
except ImportError:
    _MQTT_AVAILABLE = False

try:
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS
    _INFLUXDB_AVAILABLE = True
except ImportError:
    _INFLUXDB_AVAILABLE = False


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class FieldDef:
    name: str
    field_type: str           # "bits" | "int32" | "float32"
    bit_offset: int | None    # absolute bit position (little-endian)
    byte: int | None          # byte index for single-byte bits / int32 / float32
    bit: int | None           # LSB within byte (single-byte bits only)
    length: int | None        # bit count (bits only)
    scale: float              # multiplier applied after int32 extraction
    label: dict[int, str]     # value → display string
    hysteresis: float | None
    mqtt_topic: str | None
    influxdb: bool


@dataclass
class MessageDef:
    can_id: int
    name: str
    fields: list[FieldDef]


# ── Config parsing ────────────────────────────────────────────────────────────

def load_config(path: str) -> dict[str, Any]:
    with open(path, "rb") as f:
        return tomllib.load(f)


def _parse_label_maps(cfg: dict) -> dict[str, dict[int, str]]:
    return {
        name: {int(k, 0): v for k, v in entries.items()}
        for name, entries in cfg.get("label_maps", {}).items()
    }


def _parse_field(
    raw: dict,
    label_maps: dict[str, dict[int, str]],
    mqtt_prefix: str,
    msg_name: str,
    msg_mqtt: bool,
    msg_influxdb: bool,
) -> FieldDef:
    map_name = raw.get("label_map")
    if raw.get("mqtt", msg_mqtt):
        parts = [p for p in [mqtt_prefix, msg_name, raw["name"]] if p]
        mqtt_topic: str | None = "/".join(parts)
    else:
        mqtt_topic = None
    return FieldDef(
        name=raw["name"],
        field_type=raw.get("type", "bits"),
        bit_offset=raw.get("bit_offset"),
        byte=raw.get("byte"),
        bit=raw.get("bit"),
        length=raw.get("length"),
        scale=raw.get("scale", 1.0),
        label=label_maps.get(map_name, {}) if map_name else {},
        hysteresis=raw.get("hysteresis"),
        mqtt_topic=mqtt_topic,
        influxdb=raw.get("influxdb", msg_influxdb),
    )


def parse_config(cfg: dict) -> dict[int, MessageDef]:
    label_maps = _parse_label_maps(cfg)
    mqtt_prefix = cfg.get("mqtt", {}).get("topic_prefix", "")
    result: dict[int, MessageDef] = {}
    for raw_msg in cfg.get("messages", []):
        can_id = int(raw_msg["can_id"])
        name = raw_msg["name"]
        msg_mqtt = raw_msg.get("mqtt", False)
        msg_influxdb = raw_msg.get("influxdb", False)
        fields = [
            _parse_field(f, label_maps, mqtt_prefix, name, msg_mqtt, msg_influxdb)
            for f in raw_msg.get("fields", [])
        ]
        result[can_id] = MessageDef(can_id=can_id, name=name, fields=fields)
    return result


# ── Extraction ────────────────────────────────────────────────────────────────

def _extract_bits(data: bytes, f: FieldDef) -> int | None:
    if f.bit_offset is not None:
        if (f.bit_offset + f.length - 1) // 8 >= len(data):
            return None
        return (int.from_bytes(data, "little") >> f.bit_offset) & ((1 << f.length) - 1)
    if f.byte >= len(data):
        return None
    return (data[f.byte] >> f.bit) & ((1 << f.length) - 1)


def _extract_int32(data: bytes, f: FieldDef) -> float | None:
    if f.byte + 4 > len(data):
        return None
    return float(struct.unpack_from("<i", data, f.byte)[0]) * f.scale


def _extract_float32(data: bytes, f: FieldDef) -> float | None:
    if f.byte + 4 > len(data):
        return None
    return struct.unpack_from("<f", data, f.byte)[0]


def extract_value(data: bytes, f: FieldDef) -> int | float | None:
    if f.field_type == "int32":
        return _extract_int32(data, f)
    if f.field_type == "float32":
        return _extract_float32(data, f)
    return _extract_bits(data, f)


# ── Change detection ──────────────────────────────────────────────────────────

def has_changed(value: int | float, prev: int | float | None, hysteresis: float | None) -> bool:
    if prev is None:
        return True
    if hysteresis is not None:
        return abs(value - prev) >= hysteresis
    return value != prev


# ── Formatting ────────────────────────────────────────────────────────────────

def format_display(value: int | float, f: FieldDef) -> str:
    if isinstance(value, float):
        return f"{value:.3f}"
    length = f.length or 0
    raw = f"0x{value:0{max(1, (length + 3) // 4)}X} (0b{value:0{length}b})"
    text = f.label.get(value)
    return f"{text}  [{raw}]" if text else raw


def _mqtt_payload(value: int | float, label: dict[int, str]) -> str:
    if isinstance(value, float):
        return f"{value:.3f}"
    return label.get(value) or f"0x{value:X}"


def _influxdb_value(value: int | float, label: dict[int, str]) -> str | int | float:
    if isinstance(value, float):
        return value
    text = label.get(value)
    return text if text is not None else value


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_mqtt(cfg: dict) -> Any | None:
    mqtt_cfg = cfg.get("mqtt")
    if not mqtt_cfg:
        return None
    if not _MQTT_AVAILABLE:
        print("paho-mqtt not installed — continuing without MQTT", file=sys.stderr)
        return None
    try:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=mqtt_cfg.get("client_id", "can2mqtt"),
        )
        if "username" in mqtt_cfg:
            client.username_pw_set(mqtt_cfg["username"], mqtt_cfg.get("password", ""))
        client.connect(mqtt_cfg.get("host", "localhost"), int(mqtt_cfg.get("port", 1883)))
        client.loop_start()
        print(f"MQTT connected to {mqtt_cfg.get('host', 'localhost')}:{mqtt_cfg.get('port', 1883)}")
        return client
    except Exception as e:
        print(f"MQTT connection failed: {e} — continuing without MQTT", file=sys.stderr)
        return None


def setup_influxdb(cfg: dict) -> tuple[Any, str] | tuple[None, None]:
    influx_cfg = cfg.get("influxdb")
    if not influx_cfg:
        return None, None
    if not _INFLUXDB_AVAILABLE:
        print("influxdb-client not installed — continuing without InfluxDB", file=sys.stderr)
        return None, None
    try:
        client = InfluxDBClient(
            url=influx_cfg.get("url", "http://localhost:8086"),
            token=influx_cfg.get("token", ""),
            org=influx_cfg.get("org", ""),
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        bucket = influx_cfg.get("bucket", "can")
        print(f"InfluxDB connected to {influx_cfg.get('url', 'http://localhost:8086')}")
        return write_api, bucket
    except Exception as e:
        print(f"InfluxDB setup failed: {e} — continuing without InfluxDB", file=sys.stderr)
        return None, None


# ── Message processing ────────────────────────────────────────────────────────

def process_message(
    msg: Any,
    msg_def: MessageDef,
    state: dict[str, int | float],
    mqtt_client: Any | None,
    influx_write_api: Any | None,
    influx_bucket: str | None,
) -> None:
    changed: list[tuple[FieldDef, int | float | None, int | float]] = []

    for f in msg_def.fields:
        value = extract_value(msg.data, f)
        if value is None:
            continue
        prev = state.get(f.name)
        if has_changed(value, prev, f.hysteresis):
            changed.append((f, prev, value))
            state[f.name] = value

    if not changed:
        return

    ts = datetime.fromtimestamp(msg.timestamp).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] 0x{msg.arbitration_id:08X}  {msg_def.name}")

    influx_fields: list[tuple[str, str | int | float]] = []
    for f, prev, value in changed:
        prev_str = "init" if prev is None else format_display(prev, f)
        print(f"  {f.name}: {prev_str} -> {format_display(value, f)}")

        if f.mqtt_topic and mqtt_client:
            mqtt_client.publish(f.mqtt_topic, _mqtt_payload(value, f.label))

        if influx_write_api and f.influxdb:
            influx_fields.append((f.name, _influxdb_value(value, f.label)))

    if influx_fields:
        try:
            p = Point(msg_def.name).time(int(msg.timestamp * 1e9))
            for fname, fval in influx_fields:
                p = p.field(fname, fval)
            influx_write_api.write(bucket=influx_bucket, record=p)
        except Exception as e:
            print(f"InfluxDB write failed: {e}", file=sys.stderr)

    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.toml"
    if not Path(config_path).exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    cfg = load_config(config_path)
    messages = parse_config(cfg)
    if not messages:
        print("No messages configured.", file=sys.stderr)
        sys.exit(1)

    mqtt_client = setup_mqtt(cfg)
    influx_write_api, influx_bucket = setup_influxdb(cfg)

    state: dict[int, dict[str, int | float]] = {cid: {} for cid in messages}

    can_server = cfg.get("can-server", {})
    host = can_server.get("host", "192.168.1.1")
    port = int(can_server.get("port", 20001))
    print(f"Connecting to {host}:{port} ...")
    bus = WaveShareBus(host=host, port=port)
    print(f"Connected. Monitoring {len(messages)} message(s).\n")

    def _shutdown(_sig: Any, _frame: Any) -> None:
        print("\nShutting down.")
        bus.shutdown()
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        if influx_write_api:
            influx_write_api.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)

    for msg in bus:
        msg_def = messages.get(msg.arbitration_id)
        if msg_def is not None:
            process_message(msg, msg_def, state[msg.arbitration_id], mqtt_client, influx_write_api, influx_bucket)


if __name__ == "__main__":
    main()
