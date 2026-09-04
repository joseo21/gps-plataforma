#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Servidor TCP Teltonika para GpsSinergy.

El parser Codec 8/8E, Codec 12 y BLE es el original, sin cambios de logica.
Lo que cambio es todo lo que rodea al parser:

1. DURABILIDAD DEL ACK
   Antes: se encolaba en memoria y se confirmaba al equipo. Si el proceso
   moria con la cola llena, esos lotes ya habian sido confirmados y el
   equipo los habia borrado -> perdida silenciosa.
   Ahora: XADD a un Redis Stream con AOF, y recien despues el ACK. La API
   consume del stream; puede estar caida o redesplegandose y los equipos
   ni se enteran.

2. CONTRAPRESION
   Si el stream crece por encima de MAX_BACKLOG (API caida mucho rato),
   se responde ACK 0. El equipo guarda en su memoria interna y reenvia
   despues. Los equipos son el buffer.

3. WHITELIST DE IMEI
   Antes cualquiera que abriera un socket al 5027 y mandara 15 digitos
   recibia 0x01 y podia inyectar telemetria.

4. LIMITES DE TRAMA
   data_len venia sin techo: una trama corrupta con 0xFFFFFFFF pedia 4 GB.

5. TIMEOUT QUE DESINCRONIZABA
   Antes un TimeoutError en medio de una trama hacia 'continue', y se
   volvia a buscar preambulo dentro de un payload a medio leer. Ahora
   un timeout en el borde de trama es normal; en el medio, se cierra.

6. ESTADO EN REDIS, NO EN MEMORIA
   El registro de conexiones vive en Redis y los comandos Codec 12 se
   rutean por pub/sub. Con esto se puede correr mas de un ingestor
   detras de un balanceador sin reescribir nada.
"""

import asyncio
import json
import os
import logging
import random
import struct
import uuid
from typing import Any, Dict, List, Optional

import httpx
import redis.asyncio as aioredis

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("teltonika-tcp")

# ── Configuracion ───────────────────────────────────────────────────────────

API_BASE = os.getenv("API_BASE", "http://api:8000").rstrip("/")
INGEST_URL = f"{API_BASE}/ingest/teltonika/ingest"
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20.0"))

TCP_HOST = os.getenv("TCP_HOST", "0.0.0.0")
TCP_PORT = int(os.getenv("TCP_PORT", "5027"))
CMD_HOST = os.getenv("CMD_HOST", "0.0.0.0")
CMD_PORT = int(os.getenv("CMD_PORT", "5028"))
CMD_TIMEOUT = float(os.getenv("CMD_TIMEOUT", "10.0"))

# Timeout en el borde de trama: un equipo detenido puede tardar en hablar.
IDLE_TIMEOUT = float(os.getenv("IDLE_TIMEOUT", "600"))
# Timeout dentro de una trama ya empezada: si se pasa, algo anda mal.
FRAME_TIMEOUT = float(os.getenv("FRAME_TIMEOUT", "30"))

MAX_DATA_LEN = int(os.getenv("MAX_DATA_LEN", "65536"))
MAX_RESYNC_BYTES = int(os.getenv("MAX_RESYNC_BYTES", "8192"))
MAX_IMEI_LEN = 20

STREAM = os.getenv("STREAM", "gps:stream:telemetry")
GROUP = os.getenv("STREAM_GROUP", "ingest")
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "2000000"))
MAX_BACKLOG = int(os.getenv("MAX_BACKLOG", "500000"))
INGEST_WORKERS = int(os.getenv("INGEST_WORKERS", "4"))
BATCH = int(os.getenv("STREAM_BATCH", "20"))

KEY_ALLOWED = "gps:allowed_imei"
KEY_CONN = "gps:conn:{imei}"
CH_CMD = "gps:cmd:{inst}"
CH_RESP = "gps:cmdresp:{rid}"

ENFORCE_WHITELIST = os.getenv("ENFORCE_WHITELIST", "1").lower() in ("1", "true", "yes", "on")
# Porcentaje de tramas cuyo hex crudo se guarda. Util en el piloto para
# depurar el mapeo de IO; bajalo a 0 en produccion.
RAW_SAMPLE_PCT = float(os.getenv("RAW_SAMPLE_PCT", "100"))
HEX_DUMP_DEBUG = os.getenv("HEX_DUMP_DEBUG", "0").lower() in ("1", "true", "yes", "on")

INSTANCE_ID = os.getenv("INSTANCE_ID") or f"ing-{uuid.uuid4().hex[:8]}"

# Equipos que hablan JSON plano en vez de Codec 8. Formato del env:
#   IP_IMEI_MAP={"181.161.55.170":"861585041880343"}
try:
    IP_IMEI_MAP: Dict[str, str] = json.loads(os.getenv("IP_IMEI_MAP", "{}"))
except Exception:
    IP_IMEI_MAP = {}
    log.warning("IP_IMEI_MAP invalido, se ignora")

MAX_BEACONS = int(os.getenv("MAX_BEACONS", "60"))

rds: Optional[aioredis.Redis] = None
http: Optional[httpx.AsyncClient] = None
_ACTIVE_CONNECTIONS: Dict[str, asyncio.StreamWriter] = {}
_PENDING_RESPONSES: Dict[str, "asyncio.Future[str]"] = {}
_backlog_cache = {"n": 0, "ts": 0.0}


# ── Mapas de IO (sin cambios) ───────────────────────────────────────────────

IO_NAME_MAP: Dict[int, str] = {
    239: "Ignition", 240: "Movement", 21: "GSM Signal", 200: "Sleep Mode",
    71: "GNSS Status", 182: "GNSS HDOP", 181: "GNSS PDOP", 66: "External Voltage",
    24: "Speed", 205: "GSM Cell ID", 206: "GSM Area Code", 67: "Battery Voltage",
    68: "Battery Current", 199: "Trip Odometer", 216: "Total Odometer",
    16: "Total Odometer",
    1: "Digital Input 1", 2: "Digital Input 2", 3: "Digital Input 3", 4: "Digital Input 4",
    179: "Digital Output 1", 180: "Digital Output 2", 50: "Digital Output 3", 51: "Digital Output 4",
    9: "Analog Input 1", 10: "Analog Input 2", 11: "Analog Input 3", 245: "Analog Input 4",
    70: "PCB Temperature", 72: "Dallas Temperature 1", 73: "Dallas Temperature 2",
    74: "Dallas Temperature 3", 75: "Dallas Temperature 4",
    62: "Dallas Temperature ID 1", 63: "Dallas Temperature ID 2",
    64: "Dallas Temperature ID 3", 65: "Dallas Temperature ID 4",
    78: "iButton", 76: "Fuel Counter", 87: "Fuel Level", 88: "Engine Speed",
    135: "Fuel Rate", 1148: "Connectivity Quality", 1161: "IMEI",
    241: "GSM Operator Code", 385: "Beacon record",
    548: "Advanced BLE Advertisement data", 10500: "BLE Advertisement data",
    219: "CCID Part1", 220: "CCID Part2", 221: "CCID Part3",
    10640: "Impulse counter frequency 1", 10641: "Impulse counter RPM 1",
    483: "Impulse Counter 2", 10642: "Impulse counter frequency 2",
    10643: "Impulse counter RPM 2", 10911: "Impulse counter value 1",
    10912: "Impulse counter value 3", 10913: "Impulse counter frequency 3",
    10914: "Impulse counter RPM 3", 10915: "Impulse counter value 4",
    10916: "Impulse counter frequency 4", 10917: "Impulse counter RPM 4",
    701: "BLE Temperature 1", 702: "BLE Temperature 2",
    705: "BLE Battery 1", 706: "BLE Battery 2",
    709: "BLE Humidity 1", 710: "BLE Humidity 2",
}

IO_META: Dict[int, Dict[str, Any]] = {
    66: {"scale": 0.001, "unit": "V"}, 67: {"scale": 0.001, "unit": "V"},
    9: {"scale": 0.001, "unit": "V"}, 10: {"scale": 0.001, "unit": "V"},
    11: {"scale": 0.001, "unit": "V"}, 245: {"scale": 0.001, "unit": "V"},
    181: {"scale": 0.1, "unit": ""}, 182: {"scale": 0.1, "unit": ""},
    70: {"signed": True, "bits": 16, "unit": "C", "scale": 0.1},
    72: {"signed": True, "bits": 16, "unit": "C", "scale": 0.1},
    73: {"signed": True, "bits": 16, "unit": "C", "scale": 0.1},
    74: {"signed": True, "bits": 16, "unit": "C", "scale": 0.1},
    75: {"signed": True, "bits": 16, "unit": "C", "scale": 0.1},
}

IO_BOOL_IDS = {239, 240, 1, 2, 3, 4, 179, 180, 50, 51}


# ── Utilidades de parseo (sin cambios) ──────────────────────────────────────

def normalize_imei(s: str) -> str:
    return (s or "").strip().strip("\x00").strip()

def _need(buf, pos, need, what):
    if pos + need > len(buf):
        raise ValueError(f"Payload truncado: {what} (pos={pos}, need={need}, len={len(buf)})")

def _u8(buf, pos):  _need(buf, pos, 1, "u8");  return buf[pos], pos + 1
def _u16(buf, pos): _need(buf, pos, 2, "u16"); return struct.unpack_from(">H", buf, pos)[0], pos + 2
def _u32(buf, pos): _need(buf, pos, 4, "u32"); return struct.unpack_from(">I", buf, pos)[0], pos + 4
def _i32(buf, pos): _need(buf, pos, 4, "i32"); return struct.unpack_from(">i", buf, pos)[0], pos + 4
def _u64(buf, pos): _need(buf, pos, 8, "u64"); return struct.unpack_from(">Q", buf, pos)[0], pos + 8

def _to_hex_be(value, byte_len): return value.to_bytes(byte_len, "big", signed=False).hex()
def _reverse_hex_bytes(h): return "".join([h[i:i+2] for i in range(0, len(h), 2)][::-1])

def _ascii_from_hex(h):
    try:
        return bytes.fromhex(h).decode("ascii", errors="ignore")
    except Exception:
        return ""

def _set_named(io_out, io_id, value):
    name = IO_NAME_MAP.get(io_id)
    if name:
        io_out[name] = value
    else:
        io_out[str(io_id)] = value

def _signed_from_unsigned(v, bits):
    mask = (1 << bits) - 1
    v &= mask
    return v - (1 << bits) if (v & (1 << (bits - 1))) else v

def _postprocess_value(io_id, raw, bits_hint=None):
    if raw is None or isinstance(raw, str):
        return raw
    try:
        v = int(raw)
    except Exception:
        return raw
    if io_id in IO_BOOL_IDS and v in (0, 1):
        return bool(v)
    meta = IO_META.get(io_id) or {}
    if meta.get("signed"):
        v = _signed_from_unsigned(v, int(meta.get("bits") or bits_hint or 16))
    scale = meta.get("scale")
    if scale is not None:
        try:
            return float(v) * float(scale)
        except Exception:
            return v
    return v

def crc16_ibm(data: bytes) -> int:
    crc = 0x0000
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if (crc & 1) else crc >> 1
    return crc & 0xFFFF


# ── Lectura de tramas: con limites y sin desincronizar ──────────────────────

class FrameDesync(Exception):
    """Timeout o inconsistencia en medio de una trama: hay que cerrar."""


async def _read_exact(reader: asyncio.StreamReader, n: int, timeout: float,
                      borde: bool = False) -> bytes:
    """borde=True: es el primer byte de una trama, un timeout aca es normal
    (equipo detenido) y se propaga como TimeoutError. En cualquier otra
    posicion, un timeout deja el stream a medio leer -> FrameDesync."""
    try:
        return await asyncio.wait_for(reader.readexactly(n), timeout=timeout)
    except asyncio.TimeoutError:
        if borde:
            raise
        raise FrameDesync(f"timeout leyendo {n} bytes dentro de una trama")


async def read_frame(reader: asyncio.StreamReader):
    preamble = await _read_exact(reader, 4, IDLE_TIMEOUT, borde=True)

    if preamble != b"\x00\x00\x00\x00":
        buf = bytearray(preamble)
        escaneados = 0
        while True:
            b = await _read_exact(reader, 1, FRAME_TIMEOUT)
            buf.pop(0)
            buf.append(b[0])
            escaneados += 1
            if bytes(buf) == b"\x00\x00\x00\x00":
                break
            if escaneados > MAX_RESYNC_BYTES:
                raise FrameDesync(f"resincronizacion excedida ({escaneados} bytes)")

    len_bytes = await _read_exact(reader, 4, FRAME_TIMEOUT)
    data_len = struct.unpack(">I", len_bytes)[0]

    if data_len == 0:
        return 0, b""
    if data_len > MAX_DATA_LEN:
        # Sin este techo, una trama corrupta con 0xFFFFFFFF pide 4 GB.
        raise FrameDesync(f"data_len {data_len} excede el maximo {MAX_DATA_LEN}")

    payload = await _read_exact(reader, data_len, FRAME_TIMEOUT)
    crc_recv = struct.unpack(">I", await _read_exact(reader, 4, FRAME_TIMEOUT))[0]
    crc_ok = ((crc_recv & 0xFFFF) == (crc16_ibm(payload) & 0xFFFF))
    return (data_len if crc_ok else -1), payload


# ── Codec 12 (sin cambios) ──────────────────────────────────────────────────

def encode_codec12_command(command: str) -> bytes:
    cmd_bytes = command.encode("ascii")
    cmd_len = len(cmd_bytes)
    data_len = 1 + 1 + 1 + 4 + cmd_len + 1
    packet = bytearray(4 + 4 + data_len + 4)
    off = 0
    struct.pack_into(">I", packet, off, 0);        off += 4
    struct.pack_into(">I", packet, off, data_len); off += 4
    packet[off] = 0x0C; off += 1
    packet[off] = 0x01; off += 1
    packet[off] = 0x05; off += 1
    struct.pack_into(">I", packet, off, cmd_len);  off += 4
    packet[off:off+cmd_len] = cmd_bytes;           off += cmd_len
    packet[off] = 0x01;                            off += 1
    struct.pack_into(">I", packet, off, crc16_ibm(bytes(packet[8:8+data_len])))
    return bytes(packet)


def decode_codec12_response(payload: bytes) -> Optional[str]:
    try:
        if len(payload) < 7 or payload[0] != 0x0C or payload[2] != 0x06:
            return None
        resp_len = struct.unpack_from(">I", payload, 3)[0]
        return payload[7:7+resp_len].decode("ascii", errors="replace")
    except Exception:
        return None


# ── BLE (sin cambios) ───────────────────────────────────────────────────────

def parse_ble_adv_bytes(raw: bytes, max_beacons=60):
    out: Dict[str, Any] = {"beacons": []}
    if not raw:
        return out
    part = raw[0]
    out["part_index"] = (part >> 4) & 0x0F
    out["part_total"] = part & 0x0F
    pos = 1
    beacons = []

    def read(n):
        nonlocal pos
        if pos + n > len(raw):
            raise ValueError("truncado")
        b = raw[pos:pos+n]
        pos += n
        return b

    def ri8():  return struct.unpack("b", read(1))[0]
    def ru16(): return struct.unpack(">H", read(2))[0]

    edd = {0x01: (False, False), 0x03: (True, False), 0x07: (True, True)}
    ibe = {0x21: (False, False), 0x23: (True, False), 0x27: (True, True)}

    while pos < len(raw) and len(beacons) < max_beacons:
        flag = raw[pos]; pos += 1
        try:
            if flag in edd:
                hb, ht = edd[flag]
                ns = read(10).hex().upper(); ins = read(6).hex().upper(); rssi = ri8()
                b = {"type": "Eddystone", "id": ns+ins, "namespace": ns,
                     "instance": ins, "rssi": rssi}
                if hb: b["battery_voltage"] = ru16()/1000.0
                if ht: b["temperature_c"] = _signed_from_unsigned(ru16(), 16)/10.0
                beacons.append(b); continue
            if flag in ibe:
                hb, ht = ibe[flag]
                uid = read(16).hex().upper(); major = ru16(); minor = ru16(); rssi = ri8()
                b = {"type": "iBeacon", "id": uid, "uuid": uid,
                     "major": major, "minor": minor, "rssi": rssi}
                if hb: b["battery_voltage"] = ru16()/1000.0
                if ht: b["temperature_c"] = _signed_from_unsigned(ru16(), 16)/10.0
                beacons.append(b); continue
            out["unknown_flag"] = flag; break
        except Exception:
            out["parse_error"] = True; break

    out["beacons"] = beacons
    if pos < len(raw):
        out["unparsed_tail_hex"] = raw[pos:].hex()
    return out


def enrich_io_with_ble(io, xbytes_map):
    for ble_id in (548, 10500):
        if ble_id not in xbytes_map:
            continue
        try:
            raw = bytes.fromhex(xbytes_map[ble_id])
        except Exception:
            continue
        parsed = parse_ble_adv_bytes(raw, max_beacons=MAX_BEACONS)
        kb = IO_NAME_MAP.get(ble_id, str(ble_id))
        io[f"{kb}_raw"] = "0x" + xbytes_map[ble_id]
        beacons = parsed.get("beacons") or []
        io["Beacon_Count"] = len(beacons)
        io["Beacon_Part"] = {"index": parsed.get("part_index"), "total": parsed.get("part_total")}
        io["Beacon_List"] = beacons
        for idx, b in enumerate(beacons, 1):
            io[f"Beacon{idx}_ID"] = b.get("id")
            io[f"Beacon{idx}_Type"] = b.get("type")
            io[f"Beacon{idx}_SignalStrength"] = b.get("rssi")
        if parsed.get("unparsed_tail_hex"):
            io["Beacon_UnparsedTail"] = "0x" + parsed["unparsed_tail_hex"]


def postprocess_record(gps, io, raw_numeric, xbytes_map):
    for io_id, raw_val in list(raw_numeric.items()):
        name = IO_NAME_MAP.get(io_id)
        if not name:
            continue
        if io_id in IO_META or io_id in IO_BOOL_IDS:
            io[f"{name}_raw"] = raw_val
        io[name] = _postprocess_value(io_id, raw_val)
    if "GNSS HDOP" in io and isinstance(io["GNSS HDOP"], (int, float)):
        gps["hdop"] = io["GNSS HDOP"]
    if "GNSS PDOP" in io and isinstance(io["GNSS PDOP"], (int, float)):
        gps["pdop"] = io["GNSS PDOP"]
    op = io.get("GSM Operator Code")
    try:
        if not isinstance(op, bool) and op is not None:
            s = str(int(op))
            if len(s) in (5, 6):
                io["MCC"] = int(s[:3]); io["MNC"] = int(s[3:])
    except Exception:
        pass
    enrich_io_with_ble(io, xbytes_map)


def decode_avl_payload(payload: bytes):
    pos = 0
    codec, pos = _u8(payload, pos)
    if codec not in (0x08, 0x8E):
        raise ValueError(f"Codec no soportado: 0x{codec:02X}")
    n1, pos = _u8(payload, pos)
    records = []

    for _ in range(n1):
        ts_ms, pos = _u64(payload, pos); prio, pos = _u8(payload, pos)
        lon, pos = _i32(payload, pos);   lat, pos = _i32(payload, pos)
        alt, pos = _u16(payload, pos);   angle, pos = _u16(payload, pos)
        sats, pos = _u8(payload, pos);   speed, pos = _u16(payload, pos)

        if codec == 0x08:
            event_id, pos = _u8(payload, pos);  total_io, pos = _u8(payload, pos)
        else:
            event_id, pos = _u16(payload, pos); total_io, pos = _u16(payload, pos)

        io_by_size = {1: {}, 2: {}, 4: {}, 8: {}}
        xbytes_map = {}
        for size in (1, 2, 4, 8):
            cnt, pos = (_u8 if codec == 0x08 else _u16)(payload, pos)
            for _ in range(cnt):
                io_id, pos = (_u8 if codec == 0x08 else _u16)(payload, pos)
                val, pos = {1: _u8, 2: _u16, 4: _u32, 8: _u64}[size](payload, pos)
                io_by_size[size][io_id] = val

        if codec == 0x8E:
            cx, pos = _u16(payload, pos)
            for _ in range(cx):
                io_id, pos = _u16(payload, pos); vlen, pos = _u16(payload, pos)
                _need(payload, pos, vlen, f"xbytes({io_id})")
                xbytes_map[io_id] = payload[pos:pos+vlen].hex(); pos += vlen

        gps = {"lat": lat/1e7, "lon": lon/1e7, "alt": alt, "angle": angle,
               "sat": sats, "hdop": None, "speed": float(speed)}

        # Los IO salen por ID crudo, sin nombrar ni escalar. El nombre, la
        # escala, el signo y la columna destino los resuelve la API contra
        # io_definitions segun el modelo del equipo.
        io_raw: Dict[str, Any] = {}
        for sg in (1, 2, 4, 8):
            for io_id, val in io_by_size[sg].items():
                io_raw[str(io_id)] = val
        for io_id, hv in xbytes_map.items():
            io_raw[str(io_id)] = "0x" + hv

        records.append({"ts": ts_ms/1000.0, "event_id": event_id, "priority": prio,
                        "gps": gps, "io_raw": io_raw, "_meta": {"total_io": total_io}})

    n2, pos = _u8(payload, pos)
    if n2 != n1:
        log.debug("Aviso: n1=%d n2=%d", n1, n2)
    return codec, n1, records, True


# ── Redis: whitelist, registro de conexiones, stream ────────────────────────

async def imei_permitido(imei: str) -> bool:
    if not ENFORCE_WHITELIST:
        return True
    if not (15 <= len(imei) <= MAX_IMEI_LEN) or not imei.isdigit():
        return False
    try:
        # Si la whitelist aun no fue poblada por la API, no bloquear.
        if await rds.scard(KEY_ALLOWED) == 0:
            return True
        return bool(await rds.sismember(KEY_ALLOWED, imei))
    except Exception as e:
        # Redis caido no puede dejar la flota afuera.
        log.warning("Whitelist no disponible (%s), se acepta %s", e, imei)
        return True


async def registrar_conexion(imei: str):
    try:
        await rds.set(KEY_CONN.format(imei=imei), INSTANCE_ID, ex=90)
    except Exception as e:
        log.debug("registrar_conexion %s: %s", imei, e)


async def desregistrar_conexion(imei: str):
    try:
        actual = await rds.get(KEY_CONN.format(imei=imei))
        if actual == INSTANCE_ID:
            await rds.delete(KEY_CONN.format(imei=imei))
    except Exception as e:
        log.debug("desregistrar_conexion %s: %s", imei, e)


async def _loop_refrescar_presencia():
    """Renueva el TTL de las conexiones vivas de esta instancia."""
    while True:
        await asyncio.sleep(30)
        try:
            if _ACTIVE_CONNECTIONS:
                pipe = rds.pipeline()
                for imei in list(_ACTIVE_CONNECTIONS.keys()):
                    pipe.set(KEY_CONN.format(imei=imei), INSTANCE_ID, ex=90)
                await pipe.execute()
        except Exception as e:
            log.debug("refrescar presencia: %s", e)


async def _backlog_alto() -> bool:
    """Contrapresion: si el stream se acumula, mejor que el dato quede en
    la memoria del equipo que perderlo aca."""
    ahora = asyncio.get_event_loop().time()
    if ahora - _backlog_cache["ts"] > 5:
        try:
            _backlog_cache["n"] = await rds.xlen(STREAM)
        except Exception:
            _backlog_cache["n"] = 0
        _backlog_cache["ts"] = ahora
    return _backlog_cache["n"] > MAX_BACKLOG


async def publicar(imei: str, codec: int, records: List[Dict[str, Any]],
                   payload_hex: Optional[str]) -> bool:
    """Escritura durable ANTES del ACK. Devuelve False si no se pudo:
    en ese caso el equipo no recibe confirmacion y reenvia."""
    if await _backlog_alto():
        log.error("Backlog alto (%d), rechazando IMEI=%s", _backlog_cache["n"], imei)
        return False
    campos = {"imei": imei, "codec": str(codec), "records": json.dumps(records)}
    if payload_hex:
        campos["payload_hex"] = payload_hex
    try:
        await rds.xadd(STREAM, campos, maxlen=STREAM_MAXLEN, approximate=True)
        return True
    except Exception as e:
        log.error("XADD fallo IMEI=%s: %s", imei, e)
        # Ultimo recurso: intentar la API directo antes de rendirse.
        return await postear_api({"imei": imei, "codec": codec, "records": records})


async def postear_api(payload: Dict[str, Any]) -> bool:
    try:
        r = await http.post(INGEST_URL, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code // 100 == 2:
            return True
        log.error("API ingest HTTP %s: %s", r.status_code, (r.text or "")[:200])
        # 4xx no se reintenta: el dato no va a mejorar solo.
        return 400 <= r.status_code < 500
    except Exception as e:
        log.error("API ingest error: %s", e)
        return False


# ── Consumidores del stream ─────────────────────────────────────────────────

async def _crear_grupo():
    try:
        await rds.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        log.info("Grupo de consumidores '%s' creado", GROUP)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.error("xgroup_create: %s", e)


async def _procesar(msg_id: str, campos: Dict[str, str]) -> bool:
    try:
        payload = {
            "imei": campos["imei"],
            "codec": int(campos.get("codec") or 8),
            "records": json.loads(campos["records"]),
        }
        if campos.get("payload_hex"):
            payload["payload_hex"] = campos["payload_hex"]
    except Exception as e:
        log.error("Mensaje %s ilegible, se descarta: %s", msg_id, e)
        return True
    return await postear_api(payload)


async def ingest_worker(n: int):
    consumidor = f"{INSTANCE_ID}-{n}"
    log.info("Worker %s iniciado", consumidor)
    while True:
        try:
            resp = await rds.xreadgroup(GROUP, consumidor, {STREAM: ">"},
                                        count=BATCH, block=5000)
            if not resp:
                continue
            for _stream, mensajes in resp:
                for msg_id, campos in mensajes:
                    if await _procesar(msg_id, campos):
                        await rds.xack(STREAM, GROUP, msg_id)
                    else:
                        # Sin XACK queda pendiente y otro worker lo reclama.
                        await asyncio.sleep(1)
        except Exception as e:
            log.error("Worker %s: %s", consumidor, e)
            await asyncio.sleep(2)


async def _loop_reclamar():
    """Recupera mensajes que quedaron colgados de un worker que murio."""
    while True:
        await asyncio.sleep(60)
        try:
            await rds.xautoclaim(STREAM, GROUP, f"{INSTANCE_ID}-claim",
                                 min_idle_time=120000, count=100)
        except Exception as e:
            log.debug("xautoclaim: %s", e)


# ── Comandos Codec 12, ruteados por Redis ───────────────────────────────────

async def _enviar_local(imei: str, command: str, timeout: float) -> Dict[str, Any]:
    writer = _ACTIVE_CONNECTIONS.get(imei)
    if writer is None or writer.is_closing():
        return {"ok": False, "error": f"IMEI {imei} no esta conectado a esta instancia"}
    if imei in _PENDING_RESPONSES:
        return {"ok": False, "error": f"Comando pendiente ya existe para IMEI {imei}"}
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    _PENDING_RESPONSES[imei] = fut
    try:
        writer.write(encode_codec12_command(command))
        await writer.drain()
        log.info("[CMD] -> IMEI=%s '%s'", imei, command)
        texto = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        return {"ok": True, "imei": imei, "command": command, "response": texto}
    except asyncio.TimeoutError:
        return {"ok": False, "imei": imei, "command": command, "error": f"Timeout ({timeout}s)"}
    except Exception as e:
        return {"ok": False, "imei": imei, "command": command, "error": str(e)}
    finally:
        _PENDING_RESPONSES.pop(imei, None)


async def send_command_to_gps(imei: str, command: str,
                              timeout: float = CMD_TIMEOUT) -> Dict[str, Any]:
    if imei in _ACTIVE_CONNECTIONS:
        return await _enviar_local(imei, command, timeout)

    # El equipo esta conectado a otra instancia: rutear por pub/sub.
    try:
        dueno = await rds.get(KEY_CONN.format(imei=imei))
    except Exception as e:
        return {"ok": False, "error": f"Redis no disponible: {e}"}
    if not dueno:
        return {"ok": False, "error": f"IMEI {imei} no esta conectado"}

    rid = uuid.uuid4().hex
    pubsub = rds.pubsub()
    await pubsub.subscribe(CH_RESP.format(rid=rid))
    try:
        await rds.publish(CH_CMD.format(inst=dueno), json.dumps(
            {"imei": imei, "command": command, "timeout": timeout, "rid": rid}))
        fin = asyncio.get_event_loop().time() + timeout + 3
        while asyncio.get_event_loop().time() < fin:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg:
                return json.loads(msg["data"])
        return {"ok": False, "imei": imei, "error": f"Timeout ruteando a {dueno}"}
    finally:
        await pubsub.unsubscribe(CH_RESP.format(rid=rid))
        await pubsub.aclose()


async def _loop_escuchar_comandos():
    """Atiende comandos que otra instancia ruteo hacia esta."""
    pubsub = rds.pubsub()
    await pubsub.subscribe(CH_CMD.format(inst=INSTANCE_ID))
    log.info("Escuchando comandos en %s", CH_CMD.format(inst=INSTANCE_ID))
    async for msg in pubsub.listen():
        if msg.get("type") != "message":
            continue
        try:
            d = json.loads(msg["data"])
            res = await _enviar_local(d["imei"], d["command"], float(d.get("timeout") or CMD_TIMEOUT))
            await rds.publish(CH_RESP.format(rid=d["rid"]), json.dumps(res))
        except Exception as e:
            log.error("Comando remoto: %s", e)


# ── Modo JSON plano ─────────────────────────────────────────────────────────

def parse_json_record(data: Dict[str, Any]) -> Dict[str, Any]:
    io = {}
    for k, v in data.items():
        if k == "imei":
            continue
        if isinstance(v, dict):
            for campo, val in v.items():
                io[f"{k}_{campo}"] = val
        else:
            io[k] = v
    import time as _t
    return {"ts": _t.time(), "event_id": 0, "priority": 0,
            "gps": None, "io": io, "_meta": {"source": "json_tcp"}}


async def handle_json_client(reader, writer, imei, first_bytes, addr):
    log.info("Modo JSON IMEI=%s desde %s", imei, addr)
    buffer = bytearray(first_bytes)
    try:
        while True:
            try:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=IDLE_TIMEOUT)
                if not chunk:
                    break
                buffer.extend(chunk)
            except asyncio.TimeoutError:
                break
            if len(buffer) > 1_000_000:
                log.warning("Buffer JSON excedido para %s, cerrando", imei)
                break
            texto = buffer.decode("utf-8", errors="ignore")
            pos = 0
            while pos < len(texto):
                ini = texto.find("{", pos)
                if ini == -1:
                    break
                prof = 0; fin = -1
                for i in range(ini, len(texto)):
                    if texto[i] == "{":
                        prof += 1
                    elif texto[i] == "}":
                        prof -= 1
                        if prof == 0:
                            fin = i + 1
                            break
                if fin == -1:
                    break
                try:
                    data = json.loads(texto[ini:fin])
                    await publicar(imei, 0, [parse_json_record(data)], None)
                except Exception as e:
                    log.debug("JSON error: %s", e)
                pos = fin
            buffer = bytearray(texto[pos:].encode()) if pos > 0 else buffer
    except Exception as e:
        log.debug("Cierre JSON %s: %s", addr, e)
    finally:
        try:
            writer.close(); await writer.wait_closed()
        except Exception:
            pass


# ── Servidor HTTP interno de comandos (puerto 5028, no expuesto) ────────────

async def _handle_cmd_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    try:
        raw = b""
        while b"\r\n\r\n" not in raw:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            if not chunk:
                return
            raw += chunk
            if len(raw) > 65536:
                return
        cabecera, _, cuerpo = raw.partition(b"\r\n\r\n")
        largo = 0
        for linea in cabecera.decode("utf-8", errors="ignore").split("\r\n"):
            if linea.lower().startswith("content-length:"):
                try:
                    largo = int(linea.split(":", 1)[1].strip())
                except Exception:
                    pass
        while len(cuerpo) < largo:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            if not chunk:
                break
            cuerpo += chunk

        lineas = cabecera.decode("utf-8", errors="ignore").split("\r\n")
        partes = (lineas[0] if lineas else "").split(" ")
        metodo = partes[0] if partes else ""
        ruta = partes[1].split("?")[0] if len(partes) > 1 else ""

        def _resp(status, body):
            b = json.dumps(body, ensure_ascii=False).encode()
            writer.write((f"HTTP/1.0 {status}\r\nContent-Type: application/json\r\n"
                          f"Content-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b)

        if metodo == "GET" and ruta in ("/status", "/status/"):
            locales = list(_ACTIVE_CONNECTIONS.keys())
            try:
                backlog = await rds.xlen(STREAM)
                pendientes = await rds.xpending(STREAM, GROUP)
                pend = pendientes.get("pending", 0) if isinstance(pendientes, dict) else 0
            except Exception:
                backlog, pend = -1, -1
            _resp("200 OK", {"instancia": INSTANCE_ID, "conectados_local": locales,
                             "total_local": len(locales), "backlog_stream": backlog,
                             "pendientes": pend})
            await writer.drain(); return

        if metodo == "POST" and ruta in ("/command", "/command/"):
            try:
                d = json.loads(cuerpo.decode("utf-8", errors="ignore"))
            except Exception:
                _resp("400 Bad Request", {"error": "JSON invalido"}); await writer.drain(); return
            imei = str(d.get("imei") or "").strip()
            cmd = str(d.get("command") or "").strip()
            to = float(d.get("timeout") or CMD_TIMEOUT)
            if not imei or not cmd:
                _resp("400 Bad Request", {"error": "Se requieren: imei, command"})
                await writer.drain(); return
            res = await send_command_to_gps(imei, cmd, timeout=to)
            _resp("200 OK" if res.get("ok") else "500 Internal Server Error", res)
            await writer.drain(); return

        _resp("404 Not Found", {"error": "Use GET /status o POST /command"})
        await writer.drain()
    except Exception as e:
        log.debug("[CMD-HTTP] Error %s: %s", peer, e)
    finally:
        try:
            writer.close(); await writer.wait_closed()
        except Exception:
            pass


# ── Servidor TCP principal ──────────────────────────────────────────────────

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    imei: Optional[str] = None
    try:
        peer_ip = addr[0] if addr else ""

        # Handshake: 30 s para identificarse o se cierra. Evita que un
        # escaneo de puertos deje sockets colgados.
        first_bytes = await asyncio.wait_for(reader.readexactly(2), timeout=30)

        if first_bytes[:1] == b"{":
            imei = IP_IMEI_MAP.get(peer_ip)
            if not imei:
                log.warning("JSON desde IP no mapeada %s", peer_ip)
                return
            await handle_json_client(reader, writer, imei, first_bytes, addr)
            return

        imei_len = struct.unpack(">H", first_bytes)[0]
        if not (8 <= imei_len <= MAX_IMEI_LEN):
            log.warning("imei_len invalido (%d) desde %s", imei_len, peer_ip)
            return
        imei = normalize_imei((await asyncio.wait_for(
            reader.readexactly(imei_len), timeout=30)).decode(errors="ignore"))

        if not await imei_permitido(imei):
            log.warning("IMEI rechazado: %s desde %s", imei, peer_ip)
            writer.write(b"\x00")
            await writer.drain()
            return

        log.info("Conexion %s IMEI=%s", addr, imei)
        _ACTIVE_CONNECTIONS[imei] = writer
        await registrar_conexion(imei)

        writer.write(b"\x01")
        await writer.drain()

        while True:
            try:
                data_len, payload = await read_frame(reader)
            except asyncio.TimeoutError:
                # Silencio prolongado en el borde de trama: el equipo esta
                # detenido. Se cierra y el reconecta solo.
                log.info("IMEI=%s inactivo, cerrando", imei)
                break
            except FrameDesync as e:
                # Antes esto hacia 'continue' y se leia basura como si
                # fueran tramas. Cerrar es lo unico correcto.
                log.warning("Desincronizacion IMEI=%s: %s", imei, e)
                break
            except asyncio.IncompleteReadError:
                break

            if data_len == 0:
                break
            if data_len < 0:
                log.warning("CRC invalido IMEI=%s", imei)
                writer.write((0).to_bytes(4, "big")); await writer.drain()
                continue

            if HEX_DUMP_DEBUG:
                log.debug("PAYLOAD HEX len=%d : %s", len(payload), payload[:256].hex())

            # Respuesta a un comando Codec 12
            if payload and payload[0] == 0x0C:
                texto = decode_codec12_response(payload)
                if texto is not None:
                    fut = _PENDING_RESPONSES.get(imei)
                    if fut and not fut.done():
                        fut.set_result(texto)
                    log.info("[CMD] <- IMEI=%s '%s'", imei, texto)
                continue

            try:
                codec, n1, records, _ = decode_avl_payload(payload)
            except Exception as e:
                log.error("Decode fallo IMEI=%s: %s", imei, e)
                writer.write((0).to_bytes(4, "big")); await writer.drain()
                continue

            hex_crudo = payload.hex() if random.random() * 100 < RAW_SAMPLE_PCT else None

            # ORDEN CRITICO: primero escritura durable, despues el ACK.
            ok = await publicar(imei, codec, records, hex_crudo)
            writer.write((n1 if ok else 0).to_bytes(4, "big"))
            await writer.drain()

            if ok:
                log.info("Paquete codec=0x%02X records=%d IMEI=%s", codec, n1, imei)

    except asyncio.TimeoutError:
        log.debug("Handshake sin completar desde %s", addr)
    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        log.debug("Cierre por error con %s: %s", addr, e)
    finally:
        if imei and _ACTIVE_CONNECTIONS.get(imei) is writer:
            _ACTIVE_CONNECTIONS.pop(imei, None)
            await desregistrar_conexion(imei)
            log.info("[CONN] IMEI=%s desconectado (activos: %d)", imei, len(_ACTIVE_CONNECTIONS))
        if imei:
            fut = _PENDING_RESPONSES.pop(imei, None)
            if fut and not fut.done():
                fut.cancel()
        try:
            writer.close(); await writer.wait_closed()
        except Exception:
            pass


async def main():
    global rds, http

    rds = aioredis.from_url(REDIS_URL, decode_responses=True)
    http = httpx.AsyncClient(timeout=REQUEST_TIMEOUT,
                             limits=httpx.Limits(max_connections=50))

    for intento in range(30):
        try:
            await rds.ping()
            break
        except Exception as e:
            log.warning("Esperando Redis (%d/30): %s", intento + 1, e)
            await asyncio.sleep(2)
    else:
        raise SystemExit("Redis no disponible")

    await _crear_grupo()

    tareas = [asyncio.create_task(ingest_worker(i + 1)) for i in range(max(1, INGEST_WORKERS))]
    tareas.append(asyncio.create_task(_loop_reclamar()))
    tareas.append(asyncio.create_task(_loop_refrescar_presencia()))
    tareas.append(asyncio.create_task(_loop_escuchar_comandos()))

    tcp_server = await asyncio.start_server(handle_client, TCP_HOST, TCP_PORT)
    cmd_server = await asyncio.start_server(_handle_cmd_http, CMD_HOST, CMD_PORT)

    log.info("Instancia %s", INSTANCE_ID)
    log.info("TCP Teltonika en %s", ", ".join(str(s.getsockname()) for s in tcp_server.sockets))
    log.info("Comandos HTTP en %s", ", ".join(str(s.getsockname()) for s in cmd_server.sockets))
    log.info("Whitelist=%s  raw_sample=%s%%  workers=%d",
             ENFORCE_WHITELIST, RAW_SAMPLE_PCT, INGEST_WORKERS)

    async with tcp_server, cmd_server:
        await tcp_server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
