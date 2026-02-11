import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

import requests
import feedparser
from bs4 import BeautifulSoup

# pytrends (Google Trends)
try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None


# =========================
# SECRETS / CONFIG
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

CHAT_ID_ANTIOQUIA = os.getenv("CHAT_ID_ANTIOQUIA")
CHAT_ID_CALDAS = os.getenv("CHAT_ID_CALDAS")
CHAT_ID_GUAJIRA = os.getenv("CHAT_ID_GUAJIRA")
CHAT_ID_CESAR = os.getenv("CHAT_ID_CESAR")
CHAT_ID_PREMIUM = os.getenv("CHAT_ID_PREMIUM")  # opcional (para monetizar)

MODE = os.getenv("MODE", "ALERT").strip().upper()  # ALERT | DAILY
ENABLE_TRENDS = os.getenv("ENABLE_TRENDS", "1").strip() == "1"

if not TELEGRAM_TOKEN:
    raise RuntimeError("Falta secret: TELEGRAM_TOKEN")

for k, v in {
    "CHAT_ID_ANTIOQUIA": CHAT_ID_ANTIOQUIA,
    "CHAT_ID_CALDAS": CHAT_ID_CALDAS,
    "CHAT_ID_GUAJIRA": CHAT_ID_GUAJIRA,
    "CHAT_ID_CESAR": CHAT_ID_CESAR,
}.items():
    if not v:
        raise RuntimeError(f"Falta secret: {k}")

REGIONS = {
    "antioquia": {"label": "Antioquia", "chat_id": CHAT_ID_ANTIOQUIA, "aliases": ["antioquia"]},
    "caldas": {"label": "Caldas", "chat_id": CHAT_ID_CALDAS, "aliases": ["caldas"]},
    "la guajira": {"label": "La Guajira", "chat_id": CHAT_ID_GUAJIRA, "aliases": ["la guajira", "guajira"]},
    "cesar": {"label": "Cesar (Valledupar)", "chat_id": CHAT_ID_CESAR, "aliases": ["cesar", "valledupar"]},
}

# =========================
# DATA PATHS
# =========================
DATA_DIR = "data"
SEEN_PATH = os.path.join(DATA_DIR, "seen.json")
HIST_PATH = os.path.join(DATA_DIR, "history.json")
LAST_ALERT_PATH = os.path.join(DATA_DIR, "last_alert.json")  # anti repetidos

# Cache municipios (Wikipedia) — evita pegarle siempre
MUN_CACHE_PATH = os.path.join(DATA_DIR, "municipios_cache.json")
MUN_CACHE_TTL = 30 * 24 * 3600  # 30 días

os.makedirs(DATA_DIR, exist_ok=True)


# =========================
# FUENTES
# =========================
NEWS_FEEDS = [
    "https://www.eltiempo.com/rss/politica.xml",
    "https://www.eltiempo.com/rss/politica_gobierno.xml",
    "https://www.eltiempo.com/rss/politica_congreso.xml",
    "https://www.semana.com/arc/outboundfeeds/rss/category/politica/?outputType=xml",
]

# Proxy de redes (gratis): Google News RSS con site:
SOCIAL_SITES = {
    "X": "site:x.com",
    "Instagram": "site:instagram.com",
    "TikTok": "site:tiktok.com",
    "Facebook": "site:facebook.com",
}


# =========================
# CATEGORÍAS (diccionario)
# =========================
CATEGORIES = {
    "inversion_social": [
        "inversión social", "inversion social", "programa social", "subsidio", "transferencias",
        "adulto mayor", "familias en acción", "familias en accion", "icbf", "pobreza",
    ],
    "inversion_privada": [
        "inversión privada", "inversion privada", "inversiones privadas", "empresa", "empleo",
        "industria", "comercio", "turismo", "proyecto privado", "capital privado",
    ],
    "innovacion": [
        "innovación", "innovacion", "tecnología", "tecnologia", "startup", "emprendimiento",
        "i+d", "investigación", "investigacion", "transformación digital", "transformacion digital",
    ],
    "venta_activos_publicos": [
        "venta de", "enajenación", "enajenacion", "privatización", "privatizacion",
        "concesión", "concesion", "alianza público-privada", "app",
        "venta de entidad", "venta de empresa pública", "venta de empresa publica",
    ],
    "educacion": [
        "educación", "educacion", "colegio", "escuela", "universidad", "docentes", "pae",
        "infraestructura educativa", "infraestructura de educación", "infraestructura de educacion",
    ],
    "salud": [
        "salud", "hospital", "clínica", "clinica", "urgencias", "eps", "ips",
        "infraestructura de salud", "centro de salud", "puestos de salud",
    ],
    "infra_vial": [
        "infraestructura vial", "vía", "via", "vías", "vias", "carretera", "carreteras",
        "puente", "puentes", "peaje", "túnel", "tunel", "pavimentación", "pavimentacion",
    ],
    "medioambiente": [
        "medioambiente", "ambiente", "ecosistema", "contaminación", "contaminacion",
        "deforestación", "deforestacion", "protección", "proteccion", "residuos", "basuras",
    ],
    "tragedia_emergencia": [
        "tragedia", "emergencia", "desastre", "victimas", "fallecidos", "evacuación", "evacuacion",
    ],
    "clima_riesgo": [
        "cambio climático", "cambio climatico", "inundación", "inundacion", "inundaciones",
        "sequía", "sequia", "sequías", "deslizamiento", "deslizamientos", "derrumbe", "derrumbes",
        "ola invernal", "fenómeno del niño", "fenomeno del niño", "fenomeno del nino",
        "fenómeno de la niña", "fenomeno de la niña", "fenomeno de la nina",
    ],
}

GOV_KEYWORDS = [
    "gobernación", "gobernacion", "alcaldía", "alcaldia", "concejo", "asamblea departamental",
    "contrato", "licitación", "licitacion", "convenio", "secop", "sobrecosto",
    "procuraduría", "procuraduria", "contraloría", "contraloria", "fiscalía", "fiscalia",
]

CATEGORY_LABELS = {
    "inversion_social": "Inversión social y programas de protección",
    "inversion_privada": "Inversión privada, empleo y reactivación",
    "innovacion": "Innovación, tecnología y emprendimiento",
    "venta_activos_publicos": "Venta/enajenación de activos públicos y APP",
    "educacion": "Educación e infraestructura educativa",
    "salud": "Salud e infraestructura hospitalaria",
    "infra_vial": "Infraestructura vial y movilidad",
    "medioambiente": "Medioambiente y sostenibilidad",
    "tragedia_emergencia": "Tragedias, emergencias y gestión del riesgo",
    "clima_riesgo": "Riesgo climático (inundaciones, sequías, derrumbes, etc.)",
}


# =========================
# UTILIDADES
# =========================
HASHTAG_RE = re.compile(r"(#\w+)", re.UNICODE)

def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def normalize(text: str) -> str:
    return (text or "").strip().lower()

def sha(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:24]

def item_fingerprint(title: str, link: str) -> str:
    base = (normalize(title) + "|" + (link or "").strip()).encode("utf-8")
    return hashlib.sha256(base).hexdigest()[:24]

def google_news_rss_url(query: str) -> str:
    q = requests.utils.quote(query)
    return f"https://news.google.com/rss/search?q={q}&hl=es-419&gl=CO&ceid=CO:es-419"

def fetch_entries(feed_url: str):
    parsed = feedparser.parse(feed_url)
    return parsed.entries if getattr(parsed, "entries", None) else []

def send_telegram(chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    text = (text or "").strip()

    if len(text) > 3900:
        text = text[:3900] + "\n\n[Mensaje recortado]"

    payload = {
        "chat_id": str(chat_id).strip(),
        "text": text,
        "disable_web_page_preview": True,
    }

    r = requests.post(url, json=payload, timeout=25)
    if r.status_code != 200:
        print("Telegram error:", r.status_code, r.text)
    r.raise_for_status()

def extract_hashtags(text: str) -> List[str]:
    return [normalize(h) for h in HASHTAG_RE.findall(text or "")]

def classify_categories(text: str) -> List[str]:
    text_n = normalize(text)
    matched = []
    for cat, terms in CATEGORIES.items():
        for t in terms:
            if normalize(t) in text_n:
                matched.append(cat)
                break
    return matched

def build_terms_for_trends() -> List[str]:
    return ["inundaciones", "sequía", "infraestructura vial", "salud", "educación"]


# =========================
# MUNICIPIOS (Wikipedia) + cache + MAP municipio->region
# =========================
WIKI_MUN_URLS = {
    "antioquia": "https://es.wikipedia.org/wiki/Anexo:Municipios_de_Antioquia",
    "caldas": "https://es.wikipedia.org/wiki/Anexo:Municipios_de_Caldas",
    "la guajira": "https://es.wikipedia.org/wiki/Anexo:Municipios_de_La_Guajira",
    "cesar": "https://es.wikipedia.org/wiki/Anexo:Municipios_del_Cesar",
}

def fetch_municipios_from_wikipedia(region_key: str) -> List[str]:
    url = WIKI_MUN_URLS.get(region_key)
    if not url:
        return []

    headers = {"User-Agent": "Mozilla/5.0 (PulsoElectoral/1.0)"}
    html = requests.get(url, headers=headers, timeout=30).text
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.wikitable")

    municipios = []
    for tbl in tables:
        rows = tbl.select("tr")
        for r in rows[1:]:
            cols = r.select("td")
            if not cols:
                continue
            cell = normalize(cols[0].get_text(" ", strip=True))
            if cell and cell not in municipios:
                municipios.append(cell)

    municipios = [m for m in municipios if len(m) >= 3 and not m.startswith("nota")]
    return municipios

def load_places_and_map() -> Tuple[Dict[str, List[str]], Dict[str, str], List[str]]:
    cache = load_json(MUN_CACHE_PATH, default={"ts": 0, "data": {}})
    fresh = (time.time() - cache.get("ts", 0)) < MUN_CACHE_TTL

    if fresh and cache.get("data"):
        data = cache["data"]
    else:
        data = {}
        for rk in ["antioquia", "caldas", "la guajira", "cesar"]:
            data[rk] = fetch_municipios_from_wikipedia(rk)
        save_json(MUN_CACHE_PATH, {"ts": time.time(), "data": data})

    municipios_by_region = {}
    muni_to_region = {}

    for rk, muns in data.items():
        municipios_by_region[rk] = muns
        for m in muns:
            muni_to_region[normalize(m)] = rk

    # alias extra
    muni_to_region["guajira"] = "la guajira"
    muni_to_region["valledupar"] = "cesar"

    region_aliases_flat = []
    for rk, info in REGIONS.items():
        for a in info.get("aliases", []):
            region_aliases_flat.append(normalize(a))

    return municipios_by_region, muni_to_region, region_aliases_flat

def infer_regions_from_text(
    text_n: str,
    municipios_by_region: Dict[str, List[str]],
    muni_to_region: Dict[str, str],
    region_aliases_flat: List[str]
) -> List[str]:
    hit = set()

    # alias directos
    for a in region_aliases_flat:
        if a and a in text_n:
            if a == "guajira":
                hit.add("la guajira")
            elif a == "valledupar":
                hit.add("cesar")
            elif a in ("antioquia", "caldas", "la guajira", "cesar"):
                hit.add(a)

    # municipios (escaneo)
    for rk, muns in municipios_by_region.items():
        for m in muns:
            mm = normalize(m)
            if mm and mm in text_n:
                hit.add(rk)
                break

    return list(hit)

def extract_places_for_region(text_n: str, rk: str, municipios_by_region: Dict[str, List[str]]) -> List[str]:
    places = set()

    for a in REGIONS[rk].get("aliases", []):
        aa = normalize(a)
        if aa in text_n:
            places.add(aa)

    for m in municipios_by_region.get(rk, []):
        mm = normalize(m)
        if mm and mm in text_n:
            places.add(mm)

    out = []
    for p in places:
        if p == "guajira":
            out.append("la guajira")
        elif p == "valledupar":
            out.append("valledupar")
        else:
            out.append(p)

    return sorted(out)


# =========================
# GOOGLE TRENDS
# =========================
def fetch_google_trends_signals():
    signals = {"spikes": [], "raw": {}}
    if not ENABLE_TRENDS or TrendReq is None:
        return signals

    try:
        pytrends = TrendReq(hl="es-ES", tz=300)
        terms = build_terms_for_trends()
        pytrends.build_payload(terms, timeframe="now 1-d", geo="CO")
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            return signals

        for term in terms:
            series = df[term]
            last = float(series.iloc[-1])
            avg = float(series.mean()) if float(series.mean()) > 0 else 0.0
            signals["raw"][term] = {"last": last, "avg": avg}
            if last >= 2 * avg and last >= 20:
                signals["spikes"].append({"term": term, "last": last, "avg": avg})
    except Exception:
        return signals

    return signals


# =========================
# Picos por región (baseline propio)
# =========================
def compute_spikes_region(
    current_map: Dict[str, int],
    history_runs: List[dict],
    region_key: str,
    key: str,
    min_count: int = 2,
    factor: float = 2.0
) -> List[Tuple[str, int, float]]:
    prev = history_runs[-21:-1]
    if not prev:
        return []

    avg = {}
    used = 0

    for r in prev:
        reg = (r.get("regions") or {}).get(region_key) or {}
        m = reg.get(key, {}) or {}
        if not m:
            continue
        used += 1
        for k, v in m.items():
            avg[k] = avg.get(k, 0.0) + float(v)

    if used == 0:
        return []

    for k in list(avg.keys()):
        avg[k] = avg[k] / used

    spikes = []
    for k, c in (current_map or {}).items():
        c = float(c)
        base = float(avg.get(k, 0.0))
        if c >= min_count and (base == 0.0 or c >= factor * base):
            spikes.append((k, int(c), base))

    spikes.sort(key=lambda x: x[1], reverse=True)
    return spikes[:10]


# =========================
# Intensidad (solo 2 colores) + anti repetidos
# =========================
def compute_intensity_two_colors(score_spikes: int, volume: int, top_muni_count: int) -> Tuple[str, str]:
    """
    SOLO 2 niveles:
      🔴 ALTA
      🟡 MEDIA
    """
    if score_spikes >= 4 or volume >= 20 or top_muni_count >= 8:
        return "🔴", "ALTA"
    return "🟡", "MEDIA"

def make_alert_signature(
    region_key: str,
    spikes_cat: List[Tuple[str, int, float]],
    spikes_place: List[Tuple[str, int, float]],
    spikes_hash: List[Tuple[str, int, float]],
    top_place: List[Tuple[str, int]],
    top_cat: List[Tuple[str, int]],
    evidence_links: List[str]
) -> str:
    core = {
        "r": region_key,
        "sc": [x[0] for x in spikes_cat[:3]],
        "sp": [x[0] for x in spikes_place[:3]],
        "sh": [x[0] for x in spikes_hash[:3]],
        "tp": [x[0] for x in top_place[:3]],
        "tc": [x[0] for x in top_cat[:3]],
        "ev": evidence_links[:5],
    }
    return sha(json.dumps(core, ensure_ascii=False, sort_keys=True))

def should_skip_repeated_alert(region_key: str, signature: str, ttl_seconds: int = 6 * 3600) -> bool:
    state = load_json(LAST_ALERT_PATH, default={"regions": {}})
    reg = state.get("regions", {}).get(region_key, {})
    last_sig = reg.get("sig")
    last_ts = float(reg.get("ts", 0))

    if last_sig == signature and (time.time() - last_ts) < ttl_seconds:
        return True

    state.setdefault("regions", {})
    state["regions"][region_key] = {"sig": signature, "ts": time.time()}
    save_json(LAST_ALERT_PATH, state)
    return False


# =========================
# Helpers de redacción premium
# =========================
def pick_top_non_region_place(region_key: str, top_places: List[Tuple[str, int]]) -> Optional[Tuple[str, int]]:
    for p, cnt in top_places:
        if p != region_key:
            return (p, cnt)
    return None

def human_category(cat: str) -> str:
    return CATEGORY_LABELS.get(cat, cat)

def infer_theme_from_titles(cat: str, items: List[dict]) -> Optional[str]:
    """
    Heurística: busca palabra clave 'representativa' en títulos/evidencias.
    Especialmente útil para clima_riesgo (inundaciones/sequía/etc).
    """
    if not items:
        return None

    text = normalize(" ".join([it.get("title", "") for it in items[:12]]))
    if cat == "clima_riesgo":
        candidates = ["inundacion", "inundaciones", "sequia", "sequía", "deslizamiento", "derrumbes", "ola invernal", "fenomeno del niño", "fenómeno del niño", "fenomeno de la niña", "fenómeno de la niña"]
    elif cat == "infra_vial":
        candidates = ["vía", "via", "carretera", "puente", "peaje", "túnel", "tunel", "pavimentación", "pavimentacion"]
    elif cat == "salud":
        candidates = ["hospital", "clínica", "clinica", "eps", "urgencias"]
    elif cat == "educacion":
        candidates = ["colegio", "escuela", "universidad", "pae", "infraestructura educativa"]
    elif cat == "venta_activos_publicos":
        candidates = ["privatización", "privatizacion", "enajenación", "enajenacion", "venta", "concesión", "concesion", "app"]
    else:
        candidates = []

    for w in candidates:
        if normalize(w) in text:
            return w
    return None

def build_premium_executive_summary(
    region_label: str,
    icon: str,
    lvl: str,
    top_muni: Optional[Tuple[str, int]],
    top_cat: Optional[Tuple[str, int]],
    spikes_cat: List[Tuple[str, int, float]],
    spikes_place: List[Tuple[str, int, float]],
    volume: int,
    items: List[dict]
) -> List[str]:
    lines = []
    lines.append(f"🟣 Pulso Electoral — Informe de Riesgo Territorial — {region_label}")
    lines.append(f"{icon} Intensidad: {lvl}  |  Volumen (señales): {volume}\n")

    # Frase consultora: “intensidad alta en X por Y”
    if top_muni and top_cat:
        cat_key = top_cat[0]
        cat_label = human_category(cat_key)
        theme = infer_theme_from_titles(cat_key, items)
        if theme:
            lines.append(f"📣 Diagnóstico ejecutivo: Se observa **{lvl.lower()} intensidad** en **{top_muni[0].title()}** (menciones: {top_muni[1]}), asociada principalmente a **{cat_label.lower()}** (señales destacadas: “{theme}”).\n")
        else:
            lines.append(f"📣 Diagnóstico ejecutivo: Se observa **{lvl.lower()} intensidad** en **{top_muni[0].title()}** (menciones: {top_muni[1]}), asociada principalmente a **{cat_label.lower()}**.\n")
    elif top_muni:
        lines.append(f"📣 Diagnóstico ejecutivo: Se concentra el mayor nivel de conversación en **{top_muni[0].title()}** (menciones: {top_muni[1]}).\n")

    # Lectura rápida: picos
    if spikes_cat or spikes_place:
        lines.append("🔎 Lectura rápida (variación vs. línea base):")
        if spikes_cat:
            lines.append("• Categorías en aceleración:")
            for k, c, base in spikes_cat[:4]:
                lines.append(f"  - {human_category(k)}: {c} (prom. {base:.1f})")
        if spikes_place:
            lines.append("• Territorios en aceleración:")
            for k, c, base in spikes_place[:4]:
                # k puede ser municipio o alias
                lines.append(f"  - {k.title()}: {c} (prom. {base:.1f})")
        lines.append("")

    # Recomendación (consultora)
    lines.append("🧭 Recomendación táctica (comunicaciones):")
    if lvl == "ALTA":
        lines.append("• Activar respuesta inmediata: mensaje institucional breve + acciones verificables (qué, cuándo y quién ejecuta).")
        lines.append("• Anticipar narrativa adversa: preparar Q&A y un vocero único; evitar contradicciones públicas.")
    else:
        lines.append("• Monitoreo reforzado y posicionamiento preventivo: un pronunciamiento de contexto + señal de gestión.")
        lines.append("• Identificar actor/fuente dominante en las próximas 2–4 horas (medios/redes) y ajustar tono.")
    lines.append("")

    return lines


# =========================
# CORE
# =========================
def main():
    seen = load_json(SEEN_PATH, default={"items": {}})
    history = load_json(HIST_PATH, default={"runs": []})

    municipios_by_region, muni_to_region, region_aliases_flat = load_places_and_map()

    category_terms = []
    for terms in CATEGORIES.va
