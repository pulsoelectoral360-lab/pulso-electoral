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
for terms in CATEGORIES.values():
    for t in terms:
        category_terms.append(normalize(t))

def main():
    seen = load_json(SEEN_PATH, default={"items": {}})
    history = load_json(HIST_PATH, default={"runs": []})

    municipios_by_region, muni_to_region, region_aliases_flat = load_places_and_map()

    category_terms = []
    for terms in CATEGORIES.values():
        category_terms.extend([normalize(t) for t in terms])
    keywords_global = category_terms + [normalize(k) for k in GOV_KEYWORDS]

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # por región
    region_counts_category = {rk: {} for rk in REGIONS.keys()}
    region_counts_place = {rk: {} for rk in REGIONS.keys()}
    region_counts_hashtag = {rk: {} for rk in REGIONS.keys()}
    region_items = {rk: [] for rk in REGIONS.keys()}  # evidencias

    def bump(d: Dict[str, int], k: str, n: int = 1):
        d[k] = d.get(k, 0) + n

    def register(rk: str, hit_places: List[str], hit_cats: List[str], hit_hash: List[str], item: dict):
        for c in hit_cats:
            bump(region_counts_category[rk], c)
        for p in hit_places:
            bump(region_counts_place[rk], p)
        for h in hit_hash:
            bump(region_counts_hashtag[rk], h)
        region_items[rk].append(item)

    # ---------- 1) NOTICIAS ----------
    for feed in NEWS_FEEDS:
        for e in fetch_entries(feed)[:40]:
            title = getattr(e, "title", "") or ""
            link = getattr(e, "link", "") or ""
            summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""

            fp = item_fingerprint(title, link)
            if fp in seen["items"]:
                continue

            text = f"{title} {summary}"
            text_n = normalize(text)

            hit_regions = infer_regions_from_text(text_n, municipios_by_region, muni_to_region, region_aliases_flat)
            if not hit_regions:
                continue

            hit_cats = classify_categories(text)

            # si no cae en categorías, exige gov keyword (para no meter basura)
            if not hit_cats and not any(normalize(g) in text_n for g in GOV_KEYWORDS):
                continue

            hit_hash = extract_hashtags(text)

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": "news"}

            for rk in hit_regions:
                hit_places = extract_places_for_region(text_n, rk, municipios_by_region)
                if not hit_places:
                    hit_places = [rk]

                item = {
                    "src": "news",
                    "title": title.strip(),
                    "link": link.strip(),
                    "places": hit_places[:6],
                    "cats": hit_cats[:3],
                    "hashtags": hit_hash[:6],
                }
                register(rk, hit_places, hit_cats, hit_hash, item)

    # ---------- 2) PROXY SOCIAL (gratis) ----------
    social_queries = []
    for rk in REGIONS.keys():
        for term in [
            "inundaciones", "sequía", "deslizamientos", "infraestructura vial",
            "salud", "educación", "cambio climático", "corrupción", "inseguridad"
        ]:
            for platform, site in SOCIAL_SITES.items():
                q = f'{site} "{rk}" "{term}"'
                social_queries.append((platform, q))

    social_queries = social_queries[:28]

    for platform, query in social_queries:
        feed_url = google_news_rss_url(query)
        for e in fetch_entries(feed_url)[:15]:
            title = getattr(e, "title", "") or ""
            link = getattr(e, "link", "") or ""
            summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""

            fp = item_fingerprint(title, link)
            if fp in seen["items"]:
                continue

            text = f"{title} {summary}"
            text_n = normalize(text)

            hit_regions = infer_regions_from_text(text_n, municipios_by_region, muni_to_region, region_aliases_flat)
            if not hit_regions:
                continue

            hit_cats = classify_categories(text)
            hit_hash = extract_hashtags(text)

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": f"social:{platform}"}

            for rk in hit_regions:
                hit_places = extract_places_for_region(text_n, rk, municipios_by_region)
                if not hit_places:
                    hit_places = [rk]

                item = {
                    "src": f"social:{platform}",
                    "title": title.strip(),
                    "link": link.strip(),
                    "places": hit_places[:6],
                    "cats": hit_cats[:3],
                    "hashtags": hit_hash[:6],
                }
                register(rk, hit_places, hit_cats, hit_hash, item)

    # ---------- Limpieza seen ----------
    cutoff = time.time() - (7 * 24 * 3600)
    seen["items"] = {k: v for k, v in seen["items"].items() if v.get("ts", 0) >= cutoff}
    save_json(SEEN_PATH, seen)

    # ---------- Trends ----------
    trends = fetch_google_trends_signals()

    # ---------- Guardar history por región ----------
    run_snapshot = {"ts": now_iso, "regions": {}}
    for rk in REGIONS.keys():
        run_snapshot["regions"][rk] = {
            "category": region_counts_category[rk],
            "place": region_counts_place[rk],
            "hashtag": region_counts_hashtag[rk],
        }
    history.setdefault("runs", [])
    history["runs"].append(run_snapshot)
    history["runs"] = history["runs"][-350:]
    save_json(HIST_PATH, history)

    # ========= DAILY (por región) =========
    if MODE == "DAILY":
        window_start = now - timedelta(hours=8)

        def parse_ts(s: str) -> Optional[datetime]:
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        runs_window = []
        for r in history["runs"]:
            t = parse_ts(r.get("ts", ""))
            if t and t >= window_start:
                runs_window.append(r)

        for rk, info in REGIONS.items():
            chat_id = info["chat_id"]
            label = info["label"]

            agg_cat, agg_place, agg_hash = {}, {}, {}
            for r in runs_window:
                reg = (r.get("regions") or {}).get(rk) or {}
                for k, v in (reg.get("category") or {}).items():
                    agg_cat[k] = agg_cat.get(k, 0) + int(v)
                for k, v in (reg.get("place") or {}).items():
                    agg_place[k] = agg_place.get(k, 0) + int(v)
                for k, v in (reg.get("hashtag") or {}).items():
                    agg_hash[k] = agg_hash.get(k, 0) + int(v)

            top_cat = sorted(agg_cat.items(), key=lambda x: x[1], reverse=True)[:10]
            top_place = sorted(agg_place.items(), key=lambda x: x[1], reverse=True)[:10]
            top_hash = sorted(agg_hash.items(), key=lambda x: x[1], reverse=True)[:10]

            top_muni = pick_top_non_region_place(rk, top_place)
            top_cat_main = top_cat[0] if top_cat else None

            volume = sum(agg_cat.values()) + sum(agg_place.values()) + sum(agg_hash.values())
            # daily no usa spikes; intensidad por volumen/municipio
            icon, lvl = compute_intensity_two_colors(score_spikes=0, volume=volume, top_muni_count=(top_muni[1] if top_muni else 0))

            lines = []
            lines.append(f"🟣 Pulso Electoral — Reporte Ejecutivo (8h) — {label}")
            lines.append(f"{icon} Intensidad: {lvl}  |  Volumen (señales): {volume}\n")

            if top_muni and top_cat_main:
                cat_label = human_category(top_cat_main[0])
                lines.append(f"📍 Foco territorial: **{top_muni[0].title()}** (menciones: {top_muni[1]}), con predominio temático en **{cat_label.lower()}**.\n")
            elif top_muni:
                lines.append(f"📍 Foco territorial: **{top_muni[0].title()}** (menciones: {top_muni[1]}).\n")

            lines.append("📈 Temas con mayor tracción:")
            if top_cat:
                for k, v in top_cat[:8]:
                    lines.append(f"- {human_category(k)}: {v}")
            else:
                lines.append("- (sin señales relevantes)")

            lines.append("\n🗺️ Territorios más mencionados:")
            if top_place:
                for k, v in top_place[:8]:
                    lines.append(f"- {k.title()}: {v}")
            else:
                lines.append("- (sin señales relevantes)")

            if top_hash:
                lines.append("\n#️⃣ Palabras/hashtags recurrentes:")
                for k, v in top_hash[:8]:
                    lines.append(f"- {k}: {v}")

            if trends.get("spikes"):
                lines.append("\n🔎 Señales nacionales (Google Trends, CO):")
                for s in trends["spikes"][:5]:
                    lines.append(f"- {s['term']}: {s['last']} (avg {s['avg']:.1f})")

            send_telegram(chat_id, "\n".join(lines))

        # Premium DAILY (opcional)
        if CHAT_ID_PREMIUM:
            lines = []
            lines.append("💎 PREMIUM — Pulso Electoral (Reporte Ejecutivo 8h)")
            lines.append("Comparativo de intensidad y volumen por región:\n")
            rows = []
            for rk in REGIONS.keys():
                # solo del agregado de ventana
                # (si quieres exactitud por ventana para premium, se puede ampliar)
                reg_now = run_snapshot["regions"][rk]
                vol = sum((reg_now.get("category") or {}).values()) + sum((reg_now.get("place") or {}).values()) + sum((reg_now.get("hashtag") or {}).values())
                rows.append((rk, vol))
            rows.sort(key=lambda x: x[1], reverse=True)
            for rk, vol in rows:
                lines.append(f"- {REGIONS[rk]['label']}: {vol}")

            if trends.get("raw"):
                lines.append("\nGoogle Trends RAW (último vs avg):")
                for term, d in list(trends["raw"].items())[:5]:
                    lines.append(f"- {term}: {d['last']} vs {d['avg']:.1f}")

            send_telegram(CHAT_ID_PREMIUM, "\n".join(lines))

        return

    # ========= ALERT (por región) =========
    for rk, info in REGIONS.items():
        chat_id = info["chat_id"]
        label = info["label"]

        cur_cat = region_counts_category[rk]
        cur_place = region_counts_place[rk]
        cur_hash = region_counts_hashtag[rk]
        items = region_items[rk]

        # nada que reportar
        if not items:
            continue

        sp_cat = compute_spikes_region(cur_cat, history["runs"], rk, "category", min_count=2, factor=2.0)
        sp_place = compute_spikes_region(cur_place, history["runs"], rk, "place", min_count=2, factor=2.0)
        sp_hash = compute_spikes_region(cur_hash, history["runs"], rk, "hashtag", min_count=2, factor=2.0)

        top_cat_now = sorted(cur_cat.items(), key=lambda x: x[1], reverse=True)[:8]
        top_place_now = sorted(cur_place.items(), key=lambda x: x[1], reverse=True)[:8]
        top_hash_now = sorted(cur_hash.items(), key=lambda x: x[1], reverse=True)[:8]

        top_muni = pick_top_non_region_place(rk, top_place_now)
        top_muni_count = top_muni[1] if top_muni else 0

        score_spikes = len(sp_cat) + len(sp_place) + len(sp_hash)
        volume = sum(cur_cat.values()) + sum(cur_place.values()) + sum(cur_hash.values())

        icon, lvl = compute_intensity_two_colors(score_spikes, volume, top_muni_count)

        # filtro mínimo para que ALERT sea serio (evita ruido inútil):
        # manda si: >=3 evidencias, o hay picos, o hay spikes de Trends
        strong_signal = bool(len(items) >= 3 or score_spikes >= 1 or trends.get("spikes"))
        if not strong_signal:
            continue

        evidence_links = [it.get("link", "") for it in items if it.get("link")][:8]
        sig = make_alert_signature(rk, sp_cat, sp_place, sp_hash, top_place_now, top_cat_now, evidence_links)
        if should_skip_repeated_alert(rk, sig, ttl_seconds=6 * 3600):
            print(f"[{rk}] alerta repetida (skip).")
            continue

        top_cat_main = top_cat_now[0] if top_cat_now else None

        # --- Redacción premium ---
        lines = build_premium_executive_summary(
            region_label=label,
            icon=icon,
            lvl=lvl,
            top_muni=top_muni,
            top_cat=top_cat_main,
            spikes_cat=sp_cat,
            spikes_place=sp_place,
            volume=volume,
            items=items
        )

        # Hashtags/keywords (si hay)
        if top_hash_now:
            lines.append("📌 Señales de lenguaje (hashtags/palabras):")
            for k, v in top_hash_now[:6]:
                lines.append(f"- {k}: {v}")
            lines.append("")

        # Evidencia
        lines.append("🗞️ Evidencia (muestra curada, máx. 8):")
        for it in items[:8]:
            cats = ", ".join([human_category(c) for c in (it.get("cats") or [])]) if it.get("cats") else "Sin categoría"
            places = ", ".join([(p.title() if p else "") for p in (it.get("places") or [])]) if it.get("places") else "Sin territorio"
            lines.append(f"• [{it['src']}] {it['title']}\n  ({cats} | {places})\n  {it['link']}")

        # Señal nacional opcional
        if trends.get("spikes"):
            lines.append("\n🔎 Contexto nacional (Google Trends, CO):")
            for s in trends["spikes"][:5]:
                lines.append(f"- {s['term']}: {s['last']} (avg {s['avg']:.1f})")

        send_telegram(chat_id, "\n".join(lines))

        # Premium (opcional): versión extendida
        if CHAT_ID_PREMIUM:
            plines = []
            plines.append(f"💎 PREMIUM — Informe Extendido — {label}")
            plines.append(f"{icon} Intensidad: {lvl}  |  Volumen: {volume}\n")
            if top_muni:
                plines.append(f"Foco territorial dominante: {top_muni[0].title()} (menciones: {top_muni[1]}).")
            if top_cat_main:
                plines.append(f"Vector temático dominante: {human_category(top_cat_main[0])}.\n")

            plines.append("Evidencia ampliada (máx. 10):")
            for it in items[:10]:
                plines.append(f"• {it['title']}\n  {it['link']}")

            if trends.get("raw"):
                plines.append("\nGoogle Trends RAW (último vs avg):")
                for term, d in list(trends["raw"].items())[:5]:
                    plines.append(f"- {term}: {d['last']} vs {d['avg']:.1f}")

            send_telegram(CHAT_ID_PREMIUM, "\n".join(plines))


if __name__ == "__main__":
    main()
