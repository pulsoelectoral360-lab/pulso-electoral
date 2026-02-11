import os
import json
import time
import hashlib
from datetime import datetime, timezone
import requests
import feedparser

# =========================
# CONFIG
# =========================

FEEDS = [
    # El Tiempo (RSS oficial)
    "https://www.eltiempo.com/rss/politica.xml",
    "https://www.eltiempo.com/rss/politica_gobierno.xml",
    "https://www.eltiempo.com/rss/politica_congreso.xml",
    # Semana (RSS)
    "https://www.semana.com/arc/outboundfeeds/rss/category/politica/?outputType=xml",
]

GOOGLE_NEWS_QUERIES = [
    # Política / gobierno
    "Antioquia inversión social",
    "Antioquia infraestructura vial",
    "Antioquia educación",
    "Antioquia salud",
    "Antioquia medioambiente",
    "Caldas inversión social",
    "Caldas infraestructura vial",
    "Caldas educación",
    "Caldas salud",
    "Caldas medioambiente",
    "Valledupar inversión social",
    "Valledupar infraestructura vial",
    "Valledupar educación",
    "Valledupar salud",
    "Valledupar inundaciones",
    "La Guajira inversión social",
    "La Guajira infraestructura vial",
    "La Guajira educación",
    "La Guajira salud",
    "La Guajira sequía",
    "La Guajira cambio climático",
    # Emergencias / clima
    "Antioquia deslizamientos",
    "Antioquia derrumbes",
    "Antioquia ola invernal",
    "Caldas deslizamientos",
    "Caldas ola invernal",
    "Caldas inundaciones",
    "Valledupar ola invernal",
    "La Guajira fenómeno del niño",
    "La Guajira fenómeno de la niña",
]

# ========= REGIONES + MUNICIPIOS =========
# Para no “matar” el repo con 125 municipios de Antioquia en el código,
# soportamos cargar una lista completa desde data/municipios.json.
# Si existe, ese archivo manda. Si no existe, usamos un fallback razonable.

DEFAULT_MUNICIPIOS = {
    "antioquia": [
        # principales + subregiones frecuentes (puedes ampliar con municipios.json)
        "medellín", "bello", "itagüí", "envigado", "sabaneta", "la estrella", "copacabana",
        "girardota", "barbosa", "caldas",
        "rionegro", "la ceja", "el retiro", "guarne", "marinilla", "el santuario", "carmen de viboral",
        "santa fe de antioquia", "sopetrán", "san jerónimo", "olaya",
        "apartadó", "turbo", "carepa", "chigorodó", "necoclí", "arboletes",
        "caucasia", "tarazá", "cáceres", "nechí", "el bagre", "zaragoza",
        "segovia", "remedios", "amalfi", "yolombó",
        "andes", "jardín", "jericó", "támesis", "urrao",
        "sonson", "abejorral", "nariño",
    ],
    "caldas": [
        "manizales", "aguadas", "anserma", "aranzazu", "belalcázar", "chinchiná", "filadelfia",
        "la dorada", "la merced", "manzanares", "marmato", "marquetalia", "marulanda",
        "neira", "norcasia", "pácora", "palestina", "pensilvania", "riosucio", "risaralda",
        "salamina", "samaná", "san josé", "supía", "victoria", "villamaría", "vitebo",
    ],
    "la guajira": [
        "riohacha", "maicao", "uribia", "manaure", "fonseca", "san juan del cesar",
        "barrancas", "distracción", "dibulla", "el molino", "hato nuevo", "la jagua del pilar",
        "albania", "ureña",  # (nota: ureña es Norte de Santander; si no lo usas, elimínalo)
        "villanueva",
    ],
    "valledupar": [
        # Valledupar es municipio (Cesar). Si quieres más de Cesar, se puede ampliar.
        "valledupar",
    ],
}

# Temas principales (lo que quieres monitorear)
TOPIC_KEYWORDS = [
    "inversión social", "inversion social",
    "inversión privada", "inversion privada", "inversiones privadas",
    "innovación", "innovacion",
    "venta de entidades", "venta de empresas públicas", "venta de empresas publicas",
    "privatización", "privatizacion",
    "educación", "educacion",
    "salud",
    "infraestructura", "infraestructura vial", "vías", "vias", "carreteras", "puentes",
    "infraestructura de salud", "hospital", "clínica", "clinica", "ips",
    "infraestructura de educación", "colegio", "escuela", "universidad",
    "medioambiente", "ambiente", "sostenibilidad", "sostenible",
    "tragedia", "emergencia", "desastre",
    "cambio climático", "cambio climatico",
    "inundaciones", "inundación", "inundacion",
    "sequías", "sequía", "sequia",
    "deslizamientos", "derrumbe", "derrumbes", "deslave",
    "ola invernal",
    "fenómeno del niño", "fenomeno del niño", "fenomeno del nino",
    "fenómeno de la niña", "fenomeno de la niña", "fenomeno de la nina",
]

# Extra: instituciones / contratación (si te interesa seguirlo)
GOV_KEYWORDS = [
    "gobernación", "gobernacion", "alcaldía", "alcaldia", "concejo", "asamblea departamental",
    "contrato", "licitación", "licitacion", "convenio", "secop", "sobrecosto",
    "procuraduría", "procuraduria", "contraloría", "contraloria", "fiscalía", "fiscalia",
]

MODE = os.getenv("MODE", "ALERT").strip().upper()  # ALERT | DAILY
MAX_ENTRIES_PER_FEED = int(os.getenv("MAX_ENTRIES_PER_FEED", "40"))
SEEN_DAYS = int(os.getenv("SEEN_DAYS", "7"))
HISTORY_RUNS = int(os.getenv("HISTORY_RUNS", "60"))

# ========= TELEGRAM =========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError("Faltan secrets: TELEGRAM_TOKEN o TELEGRAM_CHAT_ID")

# ========= DATA =========
DATA_DIR = "data"
SEEN_PATH = os.path.join(DATA_DIR, "seen.json")
HIST_PATH = os.path.join(DATA_DIR, "history.json")
MUNICIPIOS_PATH = os.path.join(DATA_DIR, "municipios.json")
os.makedirs(DATA_DIR, exist_ok=True)


# =========================
# HELPERS
# =========================

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
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": False
    }
    r = requests.post(url, json=payload, timeout=25)
    r.raise_for_status()

def get_municipios():
    """
    Si existe data/municipios.json lo usamos.
    Formato esperado:
    {
      "antioquia": ["medellín", ...],
      "caldas": [...],
      "la guajira": [...],
      "valledupar": ["valledupar"]
    }
    """
    data = load_json(MUNICIPIOS_PATH, default=None)
    if isinstance(data, dict) and data:
        # normaliza
        out = {}
        for k, arr in data.items():
            out[normalize(k)] = [normalize(x) for x in (arr or []) if str(x).strip()]
        return out
    # fallback
    out = {}
    for k, arr in DEFAULT_MUNICIPIOS.items():
        out[normalize(k)] = [normalize(x) for x in arr]
    return out

def build_keywords():
    municipios = get_municipios()

    regions = ["antioquia", "caldas", "la guajira", "valledupar"]
    region_terms = []
    for r in regions:
        region_terms.append(r)
        for m in municipios.get(r, []):
            region_terms.append(m)

    # keywords finales: regiones/municipios + temas + gobierno/contratación
    keywords = []
    keywords.extend(region_terms)
    keywords.extend(TOPIC_KEYWORDS)
    keywords.extend(GOV_KEYWORDS)

    # normaliza y elimina duplicados conservando orden
    seen = set()
    final = []
    for k in keywords:
        nk = normalize(k)
        if not nk or nk in seen:
            continue
        seen.add(nk)
        final.append(nk)
    return final

def match_keywords(title: str, summary: str, keywords):
    text = normalize(title) + " " + normalize(summary)
    hits = [k for k in keywords if k in text]
    return hits


# =========================
# SOCIAL (placeholder seguro)
# =========================
def fetch_social_trends():
    """
    IMPORTANTE:
    - Facebook/Instagram/TikTok/X NO ofrecen "tendencias" libremente sin API/Proveedor.
    - Aquí debes conectar APIs oficiales o un proveedor autorizado.
    - Retorna formato:
      [
        {"platform": "x", "tag": "#algo", "count": 1234},
        ...
      ]
    """
    return []


# =========================
# MAIN
# =========================
def main():
    keywords = build_keywords()

    seen = load_json(SEEN_PATH, default={"items": {}, "last_cleanup": 0})
    history = load_json(HIST_PATH, default={"runs": []})

    # Construir feeds Google News por queries
    feeds = list(FEEDS)
    for q in GOOGLE_NEWS_QUERIES:
        feeds.append(google_news_rss_url(q))

    now_iso = datetime.now(timezone.utc).isoformat()
    run_counts = {}   # keyword -> count
    new_items = []    # items nuevos detectados

    # Escaneo RSS
    for feed in feeds:
        entries = fetch_entries(feed)
        for e in entries[:MAX_ENTRIES_PER_FEED]:
            title = getattr(e, "title", "") or ""
            link = getattr(e, "link", "") or ""
            summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""

            fp = item_fingerprint(title, link)
            if fp in seen["items"]:
                continue

            hits = match_keywords(title, summary, keywords)
            if not hits:
                continue

            # Guardar como visto
            seen["items"][fp] = {
                "title": title,
                "link": link,
                "ts": time.time(),
                "feed": feed,
                "hits": hits[:12],
            }

            # Contadores
            for h in hits:
                run_counts[h] = run_counts.get(h, 0) + 1

            new_items.append({
                "title": title.strip(),
                "link": link.strip(),
                "hits": hits[:8],
                "feed": feed,
            })

    # Guardar history del run
    history["runs"].append({
        "ts": now_iso,
        "counts": run_counts
    })
    history["runs"] = history["runs"][-HISTORY_RUNS:]

    # Limpieza seen (para no crecer infinito): borra items viejos
    cutoff = time.time() - (SEEN_DAYS * 24 * 3600)
    seen["items"] = {k: v for k, v in seen["items"].items() if v.get("ts", 0) >= cutoff}

    save_json(SEEN_PATH, seen)
    save_json(HIST_PATH, history)

    # =========================
    # Tendencias del run (picos)
    # =========================
    prev_runs = history["runs"][-21:-1]  # últimos 20 antes del run actual
    avg = {}
    if prev_runs:
        for r in prev_runs:
            for k, c in r.get("counts", {}).items():
                avg[k] = avg.get(k, 0) + c
        for k in list(avg.keys()):
            avg[k] = avg[k] / max(1, len(prev_runs))

    spikes = []
    for k, c in run_counts.items():
        base = avg.get(k, 0.0)
        # pico si count>=3 y (base==0 o duplica promedio)
        if c >= 3 and (base == 0.0 or c >= 2 * base):
            spikes.append((k, c, base))
    spikes = sorted(spikes, key=lambda x: x[1], reverse=True)[:6]

    top_hits = sorted(run_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    # =========================
    # SOCIAL (si conectas APIs)
    # =========================
    social = fetch_social_trends()  # hoy retorna []
    # si tienes social, también podrías cruzarlo con keywords/temas

    # =========================
    # MODOS
    # =========================
    if MODE == "DAILY":
        last_runs = history["runs"][-96:]  # ~24h si corre cada 15 min (ajusta según tu cron)
        agg = {}
        for r in last_runs:
            for k, c in r.get("counts", {}).items():
                agg[k] = agg.get(k, 0) + c

        top24 = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:12]

        lines = []
        lines.append("🟣 Radar Regional — RESUMEN DIARIO (24h)")
        lines.append("\n📍 Cobertura: Antioquia, Caldas, Valledupar, La Guajira")
        lines.append("\n📈 Top temas (24h):")
        for k, c in top24:
            lines.append(f"- {k}: {c}")

        if social:
            lines.append("\n📣 Tendencias redes (conectadas):")
            for s in social[:10]:
                lines.append(f"- {s.get('platform')}: {s.get('tag')} ({s.get('count')})")

        send_telegram("\n".join(lines))
        return

    # MODE ALERT (default)
    if not new_items and not spikes:
        # Nada relevante hoy, no enviamos
        print("Sin novedades (no hay noticias nuevas ni picos).")
        return

    lines = []
    lines.append("🟣 Radar Regional — ALERTA")
    lines.append("\n📍 Cobertura: Antioquia, Caldas, Valledupar, La Guajira")

    if spikes:
        lines.append("\n🔥 Tendencias en pico:")
        for k, c, base in spikes:
            lines.append(f"- {k}: {c} (prom {base:.1f})")

    if top_hits:
        lines.append("\n📈 Top temas del run:")
        for k, c in top_hits[:8]:
            lines.append(f"- {k}: {c}")

    if new_items:
        lines.append("\n📰 Noticias nuevas relevantes (máx 10):")
        for a in new_items[:10]:
            tags = ", ".join(a["hits"][:3])
            lines.append(f"• {a['title']}\n  ({tags})\n  {a['link']}")

    if social:
        lines.append("\n📣 Tendencias redes (conectadas):")
        for s in social[:10]:
            lines.append(f"- {s.get('platform')}: {s.get('tag')} ({s.get('count')})")

    send_telegram("\n".join(lines))


if __name__ == "__main__":
    main()
