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
# SECRETS
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

CHAT_ID_ANTIOQUIA = os.getenv("CHAT_ID_ANTIOQUIA")
CHAT_ID_CALDAS = os.getenv("CHAT_ID_CALDAS")
CHAT_ID_GUAJIRA = os.getenv("CHAT_ID_GUAJIRA")
CHAT_ID_CESAR = os.getenv("CHAT_ID_CESAR")

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

MODE = os.getenv("MODE", "ALERT").strip().upper()  # ALERT | DAILY
ENABLE_TRENDS = os.getenv("ENABLE_TRENDS", "1").strip() == "1"


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

# Regiones objetivo
REGIONS = {
    "antioquia": {"label": "Antioquia", "chat_id": CHAT_ID_ANTIOQUIA},
    "caldas": {"label": "Caldas", "chat_id": CHAT_ID_CALDAS},
    "la guajira": {"label": "La Guajira", "chat_id": CHAT_ID_GUAJIRA},
    "cesar": {"label": "Cesar (Valledupar)", "chat_id": CHAT_ID_CESAR},
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

# Extras de gobierno/contratación
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
    return ["inundaciones", "sequía", "infraestructura vial", "salud", "educación"]


# =========================
# MUNICIPIOS - Wikipedia + cache
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
            cell = normalize(cols[0].get_text(" ", strip=True))
            if cell and cell not in municipios:
                municipios.append(cell)

    municipios = [m for m in municipios if len(m) >= 3 and not m.startswith("nota")]
    return municipios

def load_places_by_region():
    cache = load_json(MUN_CACHE_PATH, default={"ts": 0, "data": {}})
    fresh = (time.time() - cache.get("ts", 0)) < MUN_CACHE_TTL

    if fresh and cache.get("data"):
        data = cache["data"]
    else:
        data = {}
        for rkey in REGIONS.keys():
            data[rkey] = fetch_municipios_from_wikipedia(rkey)
        save_json(MUN_CACHE_PATH, {"ts": time.time(), "data": data})

    places_by_region = {}
    for rkey in REGIONS.keys():
        s = set()
        s.add(normalize(rkey))  # nombre del depto
        for m in data.get(rkey, []):
            s.add(normalize(m))
        places_by_region[rkey] = s
    return places_by_region


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
# HELPERS: promedio/picos por región
# =========================
def avg_map(prev_runs, key):
    acc = {}
    if not prev_runs:
        return acc
    for r in prev_runs:
        m = r.get(key, {})
        for k, v in m.items():
            acc[k] = acc.get(k, 0) + float(v)
    for k in list(acc.keys()):
        acc[k] = acc[k] / len(prev_runs)
    return acc

def spikes(current_map, avgm, min_count=2):
    out = []
    for k, c in current_map.items():
        c = float(c)
        base = float(avgm.get(k, 0.0))
        if c >= min_count and (base == 0.0 or c >= 2 * base):
            out.append((k, c, base))
    out.sort(key=lambda x: x[1], reverse=True)
    return out[:10]


# =========================
# CORE
# =========================
def main():
    seen = load_json(SEEN_PATH, default={"items": {}})
    history = load_json(HIST_PATH, default={"runs": []})

    places_by_region = load_places_by_region()
    all_places = set()
    for s in places_by_region.values():
        all_places |= s

    category_terms = []
    for terms in CATEGORIES.values():
        category_terms.extend([normalize(t) for t in terms])

    keywords = list(all_places) + [normalize(k) for k in GOV_KEYWORDS] + category_terms

    now = datetime.now(timezone.utc).isoformat()

    # Conteos por región
    region_counts = {
        rkey: {
            "keyword": {},
            "category": {},
            "place": {},
            "hashtag": {},
            "items": [],  # evidencias
        } for rkey in REGIONS.keys()
    }

    def bump(d, k, n=1):
        d[k] = d.get(k, 0) + n

    def register_for_region(rkey, hit_places, hit_cats, hit_hashtags, hit_keywords, item=None):
        for k in hit_keywords:
            bump(region_counts[rkey]["keyword"], k)
        for c in hit_cats:
            bump(region_counts[rkey]["category"], c)
        for p in hit_places:
            bump(region_counts[rkey]["place"], p)
        for h in hit_hashtags:
            bump(region_counts[rkey]["hashtag"], h)
        if item:
            region_counts[rkey]["items"].append(item)

    def detect_regions(text_n: str):
        hit_regions = []
        for rkey, places in places_by_region.items():
            # rápido: si menciona el depto
            if rkey in text_n:
                hit_regions.append(rkey)
                continue
            # si menciona algún municipio
            for p in places:
                if p in text_n:
                    hit_regions.append(rkey)
                    break
        return hit_regions

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

            hit_regions = detect_regions(text_n)
            if not hit_regions:
                continue

            hit_cats = classify_categories(text)
            if not hit_cats:
                if not any(normalize(g) in text_n for g in GOV_KEYWORDS):
                    continue

            hit_hashtags = extract_hashtags(text)
            hit_keywords = [k for k in keywords if k in text_n]

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": "news"}

            for rkey in hit_regions:
                # lugares exactos (los que coinciden de esa región)
                hit_places = [p for p in places_by_region[rkey] if p in text_n][:6]
                item = {
                    "src": "news",
                    "title": title.strip(),
                    "link": link.strip(),
                    "places": hit_places[:4],
                    "cats": hit_cats[:3],
                    "hashtags": hit_hashtags[:5],
                }
                register_for_region(rkey, hit_places, hit_cats, hit_hashtags, hit_keywords, item=item)

    # ---------- 2) PROXY SOCIAL ----------
    social_queries = []
    for rkey in REGIONS.keys():
        for term in ["inundaciones", "sequía", "deslizamientos", "infraestructura vial", "salud", "educación", "cambio climático"]:
            for platform, site in SOCIAL_SITES.items():
                social_queries.append((platform, f'{site} "{rkey}" "{term}"'))

    social_queries = social_queries[:24]

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

            hit_regions = detect_regions(text_n)
            if not hit_regions:
                continue

            hit_cats = classify_categories(text)
            hit_hashtags = extract_hashtags(text)
            hit_keywords = [k for k in keywords if k in text_n]

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": f"social:{platform}"}

            for rkey in hit_regions:
                hit_places = [p for p in places_by_region[rkey] if p in text_n][:6]
                item = {
                    "src": f"social:{platform}",
                    "title": title.strip(),
                    "link": link.strip(),
                    "places": hit_places[:4],
                    "cats": hit_cats[:3],
                    "hashtags": hit_hashtags[:5],
                }
                register_for_region(rkey, hit_places, hit_cats, hit_hashtags, hit_keywords, item=item)

    # ---------- Limpieza (seen) ----------
    cutoff = time.time() - (7 * 24 * 3600)
    seen["items"] = {k: v for k, v in seen["items"].items() if v.get("ts", 0) >= cutoff}
    save_json(SEEN_PATH, seen)

    # ---------- Trends (global) ----------
    trends = fetch_google_trends_signals()

    # ---------- Guardar history por región ----------
    # Estructura: history["runs"] = [{ts, regions:{antioquia:{...},...}}]
    run_snapshot = {"ts": now, "regions": {}}
    for rkey in REGIONS.keys():
        run_snapshot["regions"][rkey] = {
            "keyword": region_counts[rkey]["keyword"],
            "category": region_counts[rkey]["category"],
            "place": region_counts[rkey]["place"],
            "hashtag": region_counts[rkey]["hashtag"],
        }

    history["runs"].append(run_snapshot)
    history["runs"] = history["runs"][-250:]
    save_json(HIST_PATH, history)

    prev_runs = history["runs"][-21:-1]

    # ---------- Enviar por región ----------
    for rkey, rinfo in REGIONS.items():
        chat_id = rinfo["chat_id"]
        label = rinfo["label"]

        cur = run_snapshot["regions"][rkey]
        cur_cat = cur.get("category", {})
        cur_place = cur.get("place", {})
        cur_hash = cur.get("hashtag", {})

        # promedios región
        prev_region = [pr["regions"].get(rkey, {}) for pr in prev_runs if pr.get("regions")]
        avg_cat = avg_map(prev_region, "category")
        avg_place = avg_map(prev_region, "place")
        avg_hash = avg_map(prev_region, "hashtag")

        sp_cat = spikes(cur_cat, avg_cat, min_count=2)
        sp_place = spikes(cur_place, avg_place, min_count=2)
        sp_hash = spikes(cur_hash, avg_hash, min_count=2)

        top_cat_now = sorted(cur_cat.items(), key=lambda x: x[1], reverse=True)[:8]
        top_place_now = sorted(cur_place.items(), key=lambda x: x[1], reverse=True)[:8]
        top_hash_now = sorted(cur_hash.items(), key=lambda x: x[1], reverse=True)[:8]
        items = region_counts[rkey]["items"]

        if MODE == "DAILY":
            # Agrega últimos runs del día (aprox) SOLO de esa región
            last = history["runs"][-96:] if history["runs"] else []
            agg_cat, agg_place, agg_hash = {}, {}, {}
            for rr in last:
                m = rr.get("regions", {}).get(rkey, {})
                for k, v in m.get("category", {}).items():
                    agg_cat[k] = agg_cat.get(k, 0) + int(v)
                for k, v in m.get("place", {}).items():
                    agg_place[k] = agg_place.get(k, 0) + int(v)
                for k, v in m.get("hashtag", {}).items():
                    agg_hash[k] = agg_hash.get(k, 0) + int(v)

            top_cat = sorted(agg_cat.items(), key=lambda x: x[1], reverse=True)[:10]
            top_place = sorted(agg_place.items(), key=lambda x: x[1], reverse=True)[:10]
            top_hash = sorted(agg_hash.items(), key=lambda x: x[1], reverse=True)[:10]

            lines = []
            lines.append(f"🟣 Radar Opinión Pública — RESUMEN (3x día)")
            lines.append(f"📍 {label}\n")

            lines.append("📈 Categorías top:")
            for k, v in top_cat:
                lines.append(f"- {k}: {v}")

            lines.append("\n🗺️ Lugares top:")
            for k, v in top_place:
                lines.append(f"- {k}: {v}")

            if top_hash:
                lines.append("\n#️⃣ Hashtags/palabras top:")
                for k, v in top_hash:
                    lines.append(f"- {k}: {v}")

            if trends.get("spikes"):
                lines.append("\n🔎 Google Trends (CO, sube fuerte):")
                for s in trends["spikes"][:5]:
                    lines.append(f"- {s['term']}: {s['last']} (avg {s['avg']:.1f})")

            send_telegram(CHAT_ID_ANTIOQUIA, "\n".join(lines))
            send_telegram(CHAT_ID_CALDAS, "\n".join(lines))
            send_telegram(CHAT_ID_GUAJIRA, "\n".join(lines))
            send_telegram(CHAT_ID_CESAR, "\n".join(lines))
            return

        # MODE == ALERT
        strong_signal = bool(sp_cat or sp_place or sp_hash or (len(items) >= 3))
        if not strong_signal:
            print(f"[{rkey}] sin señales fuertes.")
            continue

        lines = []
        lines.append("🟣 Radar Opinión Pública — ALERTA")
        lines.append(f"📍 {label}\n")

        if sp_cat:
            lines.append("🔥 Suben categorías:")
            for k, c, base in sp_cat[:6]:
                lines.append(f"- {k}: {int(c)} (prom {base:.1f})")

        if sp_place:
            lines.append("\n🔥 Sube en lugares:")
            for k, c, base in sp_place[:6]:
                lines.append(f"- {k}: {int(c)} (prom {base:.1f})")

        if sp_hash:
            lines.append("\n🔥 Suben hashtags/palabras:")
            for k, c, base in sp_hash[:6]:
                lines.append(f"- {k}: {int(c)} (prom {base:.1f})")

        lines.append("\n📈 Top ahora (categorías):")
        for k, v in top_cat_now:
            lines.append(f"- {k}: {v}")

        lines.append("\n🗺️ Top ahora (lugares):")
        for k, v in top_place_now:
            lines.append(f"- {k}: {v}")

        if top_hash_now:
            lines.append("\n#️⃣ Top ahora (hashtags/palabras):")
            for k, v in top_hash_now:
                lines.append(f"- {k}: {v}")

        if trends.get("spikes"):
            lines.append("\n🔎 Google Trends (CO, sube fuerte):")
            for s in trends["spikes"][:5]:
                lines.append(f"- {s['term']}: {s['last']} (avg {s['avg']:.1f})")

        if items:
            lines.append("\n📰 Evidencia (máx 8 links):")
            for it in items[:8]:
                cats = ", ".join(it["cats"]) if it["cats"] else "sin_categoria"
                places = ", ".join(it["places"]) if it["places"] else "sin_lugar"
                lines.append(f"• [{it['src']}] {it['title']}\n  ({cats} | {places})\n  {it['link']}")

        send_telegram(chat_id, "\n".join(lines))


if __name__ == "__main__":
    main()
