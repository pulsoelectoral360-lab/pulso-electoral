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
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError("Faltan secrets: TELEGRAM_TOKEN o TELEGRAM_CHAT_ID")

MODE = os.getenv("MODE", "ALERT").strip().upper()  # ALERT | DAILY
ENABLE_TRENDS = os.getenv("ENABLE_TRENDS", "1").strip() == "1"


# =========================
# DATA PATHS
# =========================
DATA_DIR = "data"
SEEN_PATH = os.path.join(DATA_DIR, "seen.json")
HIST_PATH = os.path.join(DATA_DIR, "history.json")
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
    "antioquia": {"label": "Antioquia"},
    "caldas": {"label": "Caldas"},
    "la guajira": {"label": "La Guajira"},
    "cesar": {"label": "Cesar (Valledupar)"},
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

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    # Telegram límite ~4096
    text = (text or "").strip()
    if len(text) > 3900:
        text = text[:3900] + "\n\n[Mensaje recortado]"

    payload = {
        "chat_id": str(TELEGRAM_CHAT_ID).strip(),
        "text": text,
        "disable_web_page_preview": True,
    }

    r = requests.post(url, json=payload, timeout=25)

    # Si falla, imprime respuesta exacta (CLAVE para corregir)
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
    # términos “representativos” (máx 5) para Trends por límites de Google
    return [
        "inundaciones",
        "sequía",
        "infraestructura vial",
        "salud",
        "educación",
    ]

# =========================
# MUNICIPIOS (TODOS) - automático desde Wikipedia
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
            # normalmente el municipio está en el 1er td o tiene un <a>
            cell = cols[0].get_text(" ", strip=True)
            cell = normalize(cell)
            # filtra vacíos
            if cell and cell not in municipios:
                municipios.append(cell)

    # Algunas tablas meten textos raros; filtrado básico
    municipios = [m for m in municipios if len(m) >= 3 and not m.startswith("nota")]
    return municipios

def get_all_place_terms():
    # Construye un set grande: departamento + todos sus municipios
    place_terms = set()
    for rkey in REGIONS.keys():
        place_terms.add(normalize(rkey))
        municipios = fetch_municipios_from_wikipedia(rkey)
        for m in municipios:
            place_terms.add(normalize(m))
    return place_terms


# =========================
# GOOGLE TRENDS (gratis)
# =========================
def fetch_google_trends_signals():
    """
    Devuelve dict con señales de aumento fuerte (si se puede).
    """
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

        # mide "último valor vs promedio" para cada término
        for term in terms:
            series = df[term]
            last = float(series.iloc[-1])
            avg = float(series.mean()) if float(series.mean()) > 0 else 0.0
            signals["raw"][term] = {"last": last, "avg": avg}
            if last >= 2 * avg and last >= 20:
                signals["spikes"].append({"term": term, "last": last, "avg": avg})
    except Exception:
        # si falla Trends, no tumba el bot
        return signals

    return signals


# =========================
# CORE
# =========================
def main():
    # Persistencia
    seen = load_json(SEEN_PATH, default={"items": {}})
    history = load_json(HIST_PATH, default={"runs": []})

    place_terms = get_all_place_terms()

    # Keywords generales = lugares + gobierno + términos de categorías (flatten)
    category_terms = []
    for terms in CATEGORIES.values():
        category_terms.extend([normalize(t) for t in terms])
    keywords = list(place_terms) + [normalize(k) for k in GOV_KEYWORDS] + category_terms

    now = datetime.now(timezone.utc).isoformat()

    # Conteos del run
    counts_keyword = {}
    counts_category = {}
    counts_place = {}
    counts_hashtag = {}
    new_items = []  # “novedades” (news + proxy social)

    def register_hit(hit_place, hit_cats, hit_hashtags, hit_keywords):
        for k in hit_keywords:
            counts_keyword[k] = counts_keyword.get(k, 0) + 1
        for c in hit_cats:
            counts_category[c] = counts_category.get(c, 0) + 1
        for p in hit_place:
            counts_place[p] = counts_place.get(p, 0) + 1
        for h in hit_hashtags:
            counts_hashtag[h] = counts_hashtag.get(h, 0) + 1

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

            # filtra: debe mencionar al menos 1 lugar o 1 región target
            hit_place = [p for p in place_terms if p in text_n]
            if not hit_place:
                continue

            hit_cats = classify_categories(text)
            if not hit_cats:
                # si no cae en categorías, igual puede ser relevante por gobierno/contratación
                if not any(normalize(g) in text_n for g in GOV_KEYWORDS):
                    continue

            hit_hashtags = extract_hashtags(text)
            hit_keywords = [k for k in keywords if k in text_n]

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": "news"}
            new_items.append({"src": "news", "title": title.strip(), "link": link.strip(),
                              "places": hit_place[:4], "cats": hit_cats[:3], "hashtags": hit_hashtags[:5]})

            register_hit(hit_place, hit_cats, hit_hashtags, hit_keywords)

    # ---------- 2) PROXY SOCIAL (Google News RSS + site:x.com etc) ----------
    # Construimos queries por región + categorías clave (para detectar conversación web)
    social_queries = []
    for rkey in REGIONS.keys():
        for cat_terms in [
            "inundaciones", "sequía", "deslizamientos", "infraestructura vial", "salud", "educación", "cambio climático"
        ]:
            for platform, site in SOCIAL_SITES.items():
                q = f'{site} "{rkey}" "{cat_terms}"'
                social_queries.append((platform, q))

    # limita para no exceder (gratis)
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

            hit_place = [p for p in place_terms if p in text_n]
            if not hit_place:
                continue

            hit_cats = classify_categories(text)
            hit_hashtags = extract_hashtags(text)
            hit_keywords = [k for k in keywords if k in text_n]

            seen["items"][fp] = {"ts": time.time(), "title": title, "link": link, "src": f"social:{platform}"}
            new_items.append({"src": f"social:{platform}", "title": title.strip(), "link": link.strip(),
                              "places": hit_place[:4], "cats": hit_cats[:3], "hashtags": hit_hashtags[:5]})

            register_hit(hit_place, hit_cats, hit_hashtags, hit_keywords)

    # ---------- Limpieza (seen) ----------
    cutoff = time.time() - (7 * 24 * 3600)
    seen["items"] = {k: v for k, v in seen["items"].items() if v.get("ts", 0) >= cutoff}
    save_json(SEEN_PATH, seen)

    # ---------- History ----------
    run_snapshot = {
        "ts": now,
        "keyword": counts_keyword,
        "category": counts_category,
        "place": counts_place,
        "hashtag": counts_hashtag,
    }
    history["runs"].append(run_snapshot)
    history["runs"] = history["runs"][-200:]  # guarda más para tendencias
    save_json(HIST_PATH, history)

    # ---------- Tendencias (picos) ----------
    # Promedio simple últimos 20 runs
    prev = history["runs"][-21:-1]
    def avg_map(key):
        acc = {}
        if not prev:
            return acc
        for r in prev:
            m = r.get(key, {})
            for k, v in m.items():
                acc[k] = acc.get(k, 0) + float(v)
        for k in list(acc.keys()):
            acc[k] = acc[k] / len(prev)
        return acc

    avg_category = avg_map("category")
    avg_place = avg_map("place")
    avg_hashtag = avg_map("hashtag")

    def spikes(current_map, avgm, min_count=3):
        out = []
        for k, c in current_map.items():
            c = float(c)
            base = float(avgm.get(k, 0.0))
            if c >= min_count and (base == 0.0 or c >= 2 * base):
                out.append((k, c, base))
        out.sort(key=lambda x: x[1], reverse=True)
        return out[:10]

    spikes_category = spikes(counts_category, avg_category, min_count=2)
    spikes_place = spikes(counts_place, avg_place, min_count=2)
    spikes_hashtag = spikes(counts_hashtag, avg_hashtag, min_count=2)

    # Google Trends
    trends = fetch_google_trends_signals()

    # ---------- MODOS ----------
    if MODE == "DAILY":
        # resumen 24h (aprox): últimos 96 runs si corre cada 15 min (ajusta si cambias cron)
        last = history["runs"][-96:] if len(history["runs"]) >= 1 else []
        agg_cat = {}
        agg_place = {}
        agg_hash = {}
        for r in last:
            for k, v in r.get("category", {}).items():
                agg_cat[k] = agg_cat.get(k, 0) + int(v)
            for k, v in r.get("place", {}).items():
                agg_place[k] = agg_place.get(k, 0) + int(v)
            for k, v in r.get("hashtag", {}).items():
                agg_hash[k] = agg_hash.get(k, 0) + int(v)

        top_cat = sorted(agg_cat.items(), key=lambda x: x[1], reverse=True)[:10]
        top_place = sorted(agg_place.items(), key=lambda x: x[1], reverse=True)[:10]
        top_hash = sorted(agg_hash.items(), key=lambda x: x[1], reverse=True)[:10]

        lines = []
        lines.append("🟣 Radar Opinión Pública — RESUMEN 24H")
        lines.append("📍 Antioquia • Caldas • Cesar(Valledupar) • La Guajira\n")

        lines.append("📈 Categorías top:")
        for k, v in top_cat:
            lines.append(f"- {k}: {v}")

        lines.append("\n🗺️ Lugares top (municipios/deptos):")
        for k, v in top_place:
            lines.append(f"- {k}: {v}")

        if top_hash:
            lines.append("\n#️⃣ Hashtags/palabras top:")
            for k, v in top_hash:
                lines.append(f"- {k}: {v}")

        if trends.get("spikes"):
            lines.append("\n🔎 Google Trends (sube fuerte):")
            for s in trends["spikes"][:5]:
                lines.append(f"- {s['term']}: {s['last']} (avg {s['avg']:.1f})")

        send_telegram("\n".join(lines))
        return

    # ALERT: manda solo si hay señales fuertes
    strong_signal = bool(spikes_category or spikes_place or spikes_hashtag or trends.get("spikes") or len(new_items) >= 5)
    if not strong_signal:
        print("Sin señales fuertes (no alerta).")
        return

    top_cat_now = sorted(counts_category.items(), key=lambda x: x[1], reverse=True)[:8]
    top_place_now = sorted(counts_place.items(), key=lambda x: x[1], reverse=True)[:8]
    top_hash_now = sorted(counts_hashtag.items(), key=lambda x: x[1], reverse=True)[:8]

    lines = []
    lines.append("🟣 Radar Opinión Pública — ALERTA")
    lines.append("📍 Antioquia • Caldas • Cesar(Valledupar) • La Guajira\n")

    if spikes_category:
        lines.append("🔥 Suben categorías:")
        for k, c, base in spikes_category[:6]:
            lines.append(f"- {k}: {int(c)} (prom {base:.1f})")

    if spikes_place:
        lines.append("\n🔥 Sube en lugares:")
        for k, c, base in spikes_place[:6]:
            lines.append(f"- {k}: {int(c)} (prom {base:.1f})")

    if spikes_hashtag:
        lines.append("\n🔥 Suben hashtags/palabras:")
        for k, c, base in spikes_hashtag[:6]:
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
        lines.append("\n🔎 Google Trends (sube fuerte):")
        for s in trends["spikes"][:5]:
            lines.append(f"- {s['term']}: {s['last']} (avg {s['avg']:.1f})")

    if new_items:
        lines.append("\n📰 Evidencia (máx 8 links):")
        for it in new_items[:8]:
            src = it["src"]
            cats = ", ".join(it["cats"]) if it["cats"] else "sin_categoria"
            places = ", ".join(it["places"]) if it["places"] else "sin_lugar"
            lines.append(f"• [{src}] {it['title']}\n  ({cats} | {places})\n  {it['link']}")

    send_telegram("\n".join(lines))


if __name__ == "__main__":
    main()
