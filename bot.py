import os
import json
import time
import hashlib
from datetime import datetime, timezone
import requests
import feedparser

# ========= CONFIG =========
FEEDS = [
    # El Tiempo (RSS oficial)
    "https://www.eltiempo.com/rss/politica.xml",
    "https://www.eltiempo.com/rss/politica_gobierno.xml",
    "https://www.eltiempo.com/rss/politica_congreso.xml",

    # Semana (RSS)
    "https://www.semana.com/arc/outboundfeeds/rss/category/politica/?outputType=xml",

    # Google News RSS (Colombia) - búsqueda por temas (se construye abajo también)
]

BASE_KEYWORDS = [
    # Núcleo (tu foco)
    "cámara de representantes",
    "antioquia",
    "escándalo",
    "corrupción",
    "inseguridad",

    # Extra clave
    "fiscalía", "procuraduría", "contraloría", "cne",
    "contrato", "licitación", "convenio", "secop", "sobrecosto",
    "reforma", "proyecto de ley", "ponencia", "votación", "debate",
    "extorsión", "homicidio", "clan del golfo", "eln", "disidencias",
    "medellín", "valle de aburrá", "urabá", "oriente antioqueño",
    "encuesta", "intención de voto", "aval", "coalición"
]

# Google News RSS: si quieres, aquí agregas queries puntuales
GOOGLE_NEWS_QUERIES = [
    "Cámara de Representantes Antioquia",
    "corrupción Antioquia",
    "inseguridad Medellín",
    "Congreso Colombia reforma",
]

# ========= SECRETS (GitHub Actions) =========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError("Faltan secrets: TELEGRAM_TOKEN o TELEGRAM_CHAT_ID")

DATA_DIR = "data"
SEEN_PATH = os.path.join(DATA_DIR, "seen.json")
HIST_PATH = os.path.join(DATA_DIR, "history.json")

os.makedirs(DATA_DIR, exist_ok=True)


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
    # feedparser maneja RSS/Atom
    parsed = feedparser.parse(feed_url)
    return parsed.entries if getattr(parsed, "entries", None) else []


def match_keywords(title: str, summary: str, keywords):
    text = normalize(title) + " " + normalize(summary)
    hits = [k for k in keywords if k in text]
    return hits


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": False
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()


def main():
    seen = load_json(SEEN_PATH, default={"items": {}, "last_cleanup": 0})
    history = load_json(HIST_PATH, default={"runs": []})

    # Construir feeds Google News por queries
    feeds = list(FEEDS)
    for q in GOOGLE_NEWS_QUERIES:
        feeds.append(google_news_rss_url(q))

    # Para tendencias del run
    now = datetime.now(timezone.utc).isoformat()
    run_counts = {}  # keyword -> count
    new_alerts = []

    # Escaneo
    for feed in feeds:
        entries = fetch_entries(feed)
        for e in entries[:40]:  # limita por feed
            title = getattr(e, "title", "") or ""
            link = getattr(e, "link", "") or ""
            summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""

            fp = item_fingerprint(title, link)
            if fp in seen["items"]:
                continue  # ya lo vimos

            hits = match_keywords(title, summary, BASE_KEYWORDS)
            if not hits:
                continue  # no relevante para tu radar

            # Marcar como visto
            seen["items"][fp] = {
                "title": title,
                "link": link,
                "ts": time.time(),
                "feed": feed,
                "hits": hits
            }

            # Contar para tendencias
            for h in hits:
                run_counts[h] = run_counts.get(h, 0) + 1

            # Preparar alerta (pero la mandamos “agrupada”)
            new_alerts.append({
                "title": title.strip(),
                "link": link.strip(),
                "hits": hits[:5],
                "feed": feed
            })
history = load_json(HIST_PATH, {"runs": []})

history["runs"].append({
    "ts": now,
    "counts": run_counts
})

history["runs"] = history["runs"][-60:]

    # Limpieza seen (para no crecer infinito)
    # Borra items vistos hace > 7 días
    cutoff = time.time() - (7 * 24 * 3600)
    seen["items"] = {k: v for k, v in seen["items"].items() if v.get("ts", 0) >= cutoff}

    save_json(SEEN_PATH, seen)
    save_json(HIST_PATH, history)

    # --------- ALERTAS ----------
    if not new_alerts:
        # No spamear: si quieres “heartbeat” diario, lo hacemos luego
        return

    # Tendencias: comparar este run vs promedio de runs anteriores
    # Promedio simple (últimos 20 runs previos)
    prev_runs = history["runs"][-21:-1]
    avg = {}
    if prev_runs:
        for r in prev_runs:
            for k, c in r.get("counts", {}).items():
                avg[k] = avg.get(k, 0) + c
        for k in list(avg.keys()):
            avg[k] = avg[k] / max(1, len(prev_runs))

    # Detectar “picos”: count >= 3 y 2x el promedio
    spikes = []
    for k, c in run_counts.items():
        base = avg.get(k, 0.0)
        if c >= 3 and (base == 0.0 or c >= 2 * base):
            spikes.append((k, c, base))

    # Construir mensaje
    top_hits = sorted(run_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    lines = []
    lines.append("🟣 Pulso Electoral — ALERTA (nuevas noticias)")
    if spikes:
        lines.append("\n🔥 Tendencias en pico:")
        for k, c, base in spikes[:6]:
            lines.append(f"- {k}: {c} (prom. {base:.1f})")

    lines.append("\n📈 Top temas del momento:")
    for k, c in top_hits:
        lines.append(f"- {k}: {c}")

    lines.append("\n📰 Nuevas noticias relevantes (máx 10):")
    for a in new_alerts[:10]:
        tags = ", ".join(a["hits"][:3])
        lines.append(f"• {a['title']}\n  ({tags})\n  {a['link']}")

    msg = "\n".join(lines)
    # ========= MODO DE EJECUCIÓN =========
MODE = os.getenv("MODE", "ALERT").strip().upper()

# baseline promedio (últimos 20 runs anteriores)
history = load_history()
prev = history["runs"][-21:-1]
avg = {}
if prev:
    for r in prev:
        for k, c in r.get("counts", {}).items():
            avg[k] = avg.get(k, 0) + c
    for k in list(avg.keys()):
        avg[k] = avg[k] / max(1, len(prev))

spikes = []
for k, c in run_counts.items():
    base = avg.get(k, 0.0)
    if c >= 3 and (base == 0.0 or c >= 2 * base):
        spikes.append((k, c, base))

spikes = sorted(spikes, key=lambda x: x[1], reverse=True)[:6]

# ========= ALERT MODE =========
if MODE == "ALERT":
   if not spikes:
    print("No hay picos detectados")

    lines = []
    lines.append("🟣 Pulso Electoral — ALERTA (pico detectado)")
    lines.append("\n🔥 Tendencias en pico:")
    for k, c, base in spikes:
        lines.append(f"- {k}: {c} (prom {base:.1f})")

if not new_items:
    print("No hay noticias nuevas")
else:
    lines.append("\n📰 Noticias nuevas:")
    for a in new_items[:10]:
        tags = ", ".join(a["hits"][:3])
        lines.append(f"• {a['title']}\n ({tags})\n {a['link']}")

# ========= DAILY MODE =========
if MODE == "DAILY":
    last_runs = history["runs"][-96:]
    agg = {}
    for r in last_runs:
        for k, c in r.get("counts", {}).items():
            agg[k] = agg.get(k, 0) + c

    top24 = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:10]

    lines = []
    lines.append("🟣 Pulso Electoral — RESUMEN DIARIO (24h)")
    lines.append("\n📈 Top temas:")
    for k, c in top24:
        lines.append(f"- {k}: {c}")

    send_telegram("\n".join(lines))
    


if __name__ == "__main__":
    main()
