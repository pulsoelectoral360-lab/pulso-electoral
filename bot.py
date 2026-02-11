import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone

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
    "antioquia": {"label": "Antioquia"},
    "caldas": {"label": "Caldas"},
    "la guajira": {"label": "La Guajira"},
    "cesar": {"label": "Cesar (Valledupar)"},
}

REGION_CHAT_IDS = {
    "antioquia": CHAT_ID_ANTIOQUIA,
    "caldas": CHAT_ID_CALDAS,
    "la guajira": CHAT_ID_GUAJIRA,
    "cesar": CHAT_ID_CESAR,
}


# =========================
# DATA PATHS
# =========================
DATA_DIR = "data"
SEEN_PATH = os.path.join(DATA_DIR, "seen.json")
HIST_PATH = os.path.join(DATA_DIR, "history.json")

# Cache municipios (evita pegarle a Wikipedia en cada run)
MUN_CACHE_PATH = os.path.join(DATA_DIR, "municipios_cache.json")
MUN_CACHE_TTL = 30 * 24 * 3600  # 30 días

os.makedirs(DATA_DIR, exist_ok=True)


# =========================
# FUENTES (noticias)
# =========================
NEWS_FEEDS = [
    "https://www.eltiempo.com/rss/politica.xml",
    "https://www.eltiempo.com/rss/politica_gobierno.xml",
    "https://www.eltiempo.com/rss/politica_congreso.xml",
    "https://www.semana.com/arc/outboundfeeds/rss/category/politica/?outputType=xml",
]

# Proxy de redes: Google News RSS search con site:
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

# Extras de gobierno/contratación (útil en política)
GOV_KEYWORDS = [
    "gobernación", "gobernacion", "alcaldía", "alcaldia", "concejo", "asamblea departamental",
    "contrato", "licitación", "licitacion", "convenio", "secop", "sobrecosto",
    "procuraduría", "procuraduria", "contraloría", "contraloria", "fiscalía", "fiscalia",
]


# =========================
# UTILIDADES
# =========================
HASHTAG_RE = re.compile(r"(#\w+)", re.UNICODE)

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def normalize(text: str) -> str:
    return (text or "").strip().lower()

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

    # Telegram límite ~4096, recortamos por seguridad
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

def extract_hashtags(text: str):
    return [normalize(h) for h in HASHTAG_RE.findall(text or "")]

def classify_categories(text: str):
    text_n = normalize(text)
    matched = []
    for cat, terms in CATEGORIES.items():
        for t in terms:
            if normalize(t) in text_n:
                matched.append(cat)
                break
    return matched

def build_terms_for_trends():
    # max 5 por limitaciones de Google Trends
    return ["inundaciones", "sequía", "infraestructura vial", "salud", "educación"]


# =========================
# MUNICIPIOS (Wikipedia) + cache
# =========================
WIKI_MUN_URLS = {
    "antioquia": "https://es.wikipedia.org/wiki/Anexo:Municipios_de_Antioquia",
    "caldas": "https://es.wikipedia.org/wiki/Anexo:Municipios_de_Caldas",
    "la guajira": "https://es.wikipedia.org/wiki/Anexo:Municipios_de_La_Guajira",
    "cesar": "https://es.wikipedia.org/wiki/Anexo:Municipios_del_Cesar",
}

def fetch_municipios_from_wikipedia(region_key: str):
    url = WIKI_MUN_URLS.get(region_key)
    if not url:
        return []

    html = requests.get(url, timeout=30).text
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.wikitable")

    municipios = []
    for tbl in tables:
        rows = tbl.select("tr")
        for r in rows[1:]:
            cols = r.select("td")
            if not cols:
                continue
            cell = cols[0].get_text(" ", strip=True)
            cell = normalize(cell)
            if cell and cell not in municipios:
                municipios.append(cell)

    # filtro básico
    municipios = [m for m in municipios if len(m) >= 3 and not m.startswith("nota")]
    return municipios

def get_all_place_terms():
    cache = load_json(MUN_CACHE_PATH, default={"ts": 0, "data": {}})
    fresh = (time.time() - cache.get("ts", 0)) < MUN_CACHE_TTL

    if fresh and cache.get("data"):
        data = cache["data"]
    else:
        data = {}
        for rkey in REGIONS.keys():
            data[rkey] = fetch_municipios_from_wikipedia(rkey)
        save_json(MUN_CACHE_PATH, {"ts": time.time(), "data": data})

    place_terms = set()
    for rkey in REGIONS.keys():
        place_terms.add(normalize(rkey))
        for m in data.get(rkey, []):
            place_terms.add(normalize(m))

    return place_terms, data


# =========================
# GOOGLE TRENDS (gratis)
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
# REGION HELPERS
# =========================
def detect_region(text_n: str, municipios_by_region: dict):
    # 1) si menciona el depto directamente
    for rkey in REGIONS.keys():
        if rkey in text_n:
            return rkey

    # 2) si menciona un municipio del depto
    for rkey, muns in municipios_by_region.items():
        for m in muns:
            m = normalize(m)
            if m and m in text_n:
                return rkey

    # extra: Valledupar => Cesar
    if "valledupar" in text_n:
        return "cesar"

    return None


# =========================
# SPIKES (PICOS REALES) POR REGIÓN
# =========================
def compute_spikes_region(current_map: dict, history_runs: list, region_key: str, key: str, min_count=3, factor=2.0):
    prev = history_runs[-21:-1]
    if not prev:
        return []

    avg = {}
    used = 0
    for r in prev:
        reg = (r.get("region") or {}).get(region_key) or {}
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
    for k, c in current_map.items():
        c = float(c)
        base = float(avg.get(k, 0.0))
        if c >= min_count and (base == 0.0 or c >= factor * base):
            spikes.append((k, int(c), base))

    spikes.sort(key=lambda x: x[1], reverse=True)
    return spikes[:10]


# =========================
# CORE
# =========================
def main():
    seen = load_json(SEEN_PATH, default={"items": {}})
    history = load_json(HIST_PATH, default={"runs": []})

    place_terms, municipios_by_region = get_all_place_terms()

    category_terms = []
    for terms in CATEGORIES.values():
        category_terms.extend([normalize(t) for t in terms])

    keywords = list(place_terms) + [normalize(k) for k in GOV_KEYWORDS] + category_terms

    now = datetime.now(timezone.utc).isoformat()

    # global (por si lo quieres)
    counts_category = {}
    counts_place = {}
    counts_hashtag = {}

    # por región
    region_counts_category = {rk: {} for rk in REGIONS.keys()}
    region_counts_place = {rk: {} for rk in REGIONS.keys()}
    region_counts_hashtag = {rk: {} for rk in REGIONS.keys()}
    region_new_items = {rk: [] for rk in REGIONS.keys()}

    def bump(d, k, inc=1):
        d[k] = d.get(k, 0) + inc

    def register_region(rk, hit_place, hit_cats, hit_hashtags):
        for c in hit_cats:
            bump(region_counts_category[rk], c)
            bump(counts_category, c)
        for p in hit_place:
            bump(region_counts_place[rk], p)
            bump(counts_place, p)
        for h in hit_hashtags:
            bump(region_counts_hashtag[rk], h)
            bump(counts_hashtag, h)

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

            rk = detect_region(text_n, municipios_by_region)
            if not rk:
                continue

            hit_place = [p for p in place_terms if p in text_n][:6]
            hit_cats = classify_categories(text)

            # si no cae en categorías, al menos que toque GOV_KEYWORDS
            if not hit_cats and not any(normalize(g) in text_n for g in GOV_KEYWORDS):
                continue

            hit_hashtags = extract_hashtags(text)

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": "news", "region": rk}

            item = {
                "src": "news",
                "title": title.strip(),
                "link": link.strip(),
                "places": hit_place[:4],
                "cats": hit_cats[:3],
                "hashtags": hit_hashtags[:5],
            }
            region_new_items[rk].append(item)
            register_region(rk, hit_place, hit_cats, hit_hashtags)

    # ---------- 2) PROXY SOCIAL ----------
    social_queries = []
    for rkey in REGIONS.keys():
        for cat_terms in ["inundaciones", "sequía", "deslizamientos", "infraestructura vial", "salud", "educación", "cambio climático"]:
            for platform, site in SOCIAL_SITES.items():
                q = f'{site} "{rkey}" "{cat_terms}"'
                social_queries.append((rkey, platform, q))

    social_queries = social_queries[:24]

    for rkey, platform, query in social_queries:
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

            rk = detect_region(text_n, municipios_by_region) or rkey
            if rk not in REGIONS:
                continue

            hit_place = [p for p in place_terms if p in text_n][:6]
            hit_cats = classify_categories(text)
            hit_hashtags = extract_hashtags(text)

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": f"social:{platform}", "region": rk}

            item = {
                "src": f"social:{platform}",
                "title": title.strip(),
                "link": link.strip(),
                "places": hit_place[:4],
                "cats": hit_cats[:3],
                "hashtags": hit_hashtags[:5],
            }
            region_new_items[rk].append(item)
            register_region(rk, hit_place, hit_cats, hit_hashtags)

    # ---------- Limpieza (seen) ----------
    cutoff = time.time() - (7 * 24 * 3600)
    seen["items"] = {k: v for k, v in seen["items"].items() if v.get("ts", 0) >= cutoff}
    save_json(SEEN_PATH, seen)

    # ---------- Trends ----------
    trends = fetch_google_trends_signals()

    # ---------- History (global + por región) ----------
    run_snapshot = {
        "ts": now,
        "category": counts_category,
        "place": counts_place,
        "hashtag": counts_hashtag,
        "region": {
            rk: {
                "category": region_counts_category[rk],
                "place": region_counts_place[rk],
                "hashtag": region_counts_hashtag[rk],
            }
            for rk in REGIONS.keys()
        },
    }
    history["runs"].append(run_snapshot)
    history["runs"] = history["runs"][-200:]
    save_json(HIST_PATH, history)

    # =========================
    # DAILY (POR REGIÓN) — 1 mensaje por grupo
    # =========================
    if MODE == "DAILY":
        for rk in REGIONS.keys():
            agg_cat = region_counts_category.get(rk, {})
            agg_place = region_counts_place.get(rk, {})
            agg_hash = region_counts_hashtag.get(rk, {})

            top_cat = sorted(agg_cat.items(), key=lambda x: x[1], reverse=True)[:10]
            top_place = sorted(agg_place.items(), key=lambda x: x[1], reverse=True)[:10]
            top_hash = sorted(agg_hash.items(), key=lambda x: x[1], reverse=True)[:10]

            lines = []
            lines.append(f"🟣 Radar Opinión Pública — RESUMEN DIARIO — {REGIONS[rk]['label']}")
            lines.append(f"📍 {REGIONS[rk]['label']}\n")

            lines.append("📈 Categorías top:")
            if top_cat:
                for k, v in top_cat:
                    lines.append(f"- {k}: {v}")
            else:
                lines.append("- (sin datos)")

            lines.append("\n🗺️ Lugares top:")
            if top_place:
                for k, v in top_place:
                    lines.append(f"- {k}: {v}")
            else:
                lines.append("- (sin datos)")

            if top_hash:
                lines.append("\n#️⃣ Hashtags/palabras top:")
                for k, v in top_hash:
                    lines.append(f"- {k}: {v}")

            if trends.get("spikes"):
                lines.append("\n🔎 Google Trends (CO, sube fuerte):")
                for s in trends["spikes"][:5]:
                    lines.append(f"- {s['term']}: {s['last']} (avg {s['avg']:.1f})")

            items = region_new_items.get(rk, [])
            if items:
                lines.append("\n📰 Evidencia (máx 6 links):")
                for it in items[:6]:
                    src = it["src"]
                    cats = ", ".join(it["cats"]) if it["cats"] else "sin_categoria"
                    places = ", ".join(it["places"]) if it["places"] else "sin_lugar"
                    lines.append(f"• [{src}] {it['title']}\n  ({cats} | {places})\n  {it['link']}")

            send_telegram(REGION_CHAT_IDS[rk], "\n".join(lines))

        return

    # =========================
    # ALERT (POR REGIÓN) — SOLO pico real
    # =========================
    for rk in REGIONS.keys():
        items = region_new_items.get(rk, [])
        if not items:
            continue

        sp_cat = compute_spikes_region(region_counts_category[rk], history["runs"], rk, "category", min_count=2, factor=2.0)
        sp_place = compute_spikes_region(region_counts_place[rk], history["runs"], rk, "place", min_count=2, factor=2.0)
        sp_hash = compute_spikes_region(region_counts_hashtag[rk], history["runs"], rk, "hashtag", min_count=2, factor=2.0)

        strong = bool(sp_cat or sp_place or sp_hash or trends.get("spikes"))
        if not strong:
            print(f"[{rk}] Sin picos reales, no alerta.")
            continue

        top_cat_now = sorted(region_counts_category[rk].items(), key=lambda x: x[1], reverse=True)[:8]
        top_place_now = sorted(region_counts_place[rk].items(), key=lambd
