"""
Bot Keuangan Telegram — Production-Ready Financial Tracker
==========================================================
Architecture : aiogram 3.x  ·  aiosqlite  ·  Google Gemini AI
Timezone     : Asia/Jakarta (WIB)
Author       : Antigravity Vibe Coding
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import BufferedInputFile, Message
from dotenv import load_dotenv
from google import genai
import gspread
from google.oauth2.service_account import Credentials
# ── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("bot_keuangan")

# ── Environment & Config ─────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")

_raw_ids = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: set[int] = {
    int(uid.strip()) for uid in _raw_ids.split(",") if uid.strip().isdigit()
}

WIB = ZoneInfo("Asia/Jakarta")
DB_PATH = "keuangan.db"

KATEGORI_VALID: list[str] = [
    "Makanan & Minuman",
    "Transportasi",
    "Kebutuhan Harian",
    "Tagihan & Hiburan",
    "Kesehatan",
    "Pendidikan",
    "Pemasukan",
    "Lainnya",
]

# ── Startup Validation ──────────────────────────────────────────────────
def _validate_env() -> None:
    missing: list[str] = []
    if not BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    if not ALLOWED_USER_IDS:
        missing.append("ALLOWED_USER_IDS")
    if missing:
        logger.critical("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════
#  DATABASE LAYER (aiosqlite — fully async)
# ══════════════════════════════════════════════════════════════════════════

_db: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    """Return the singleton database connection, creating it if needed."""
    global _db
    if _db is None:
        _db = await aiosqlite.connect(DB_PATH)
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL;")
        await _db.execute("PRAGMA foreign_keys=ON;")
    return _db


async def init_db() -> None:
    """Create the `transaksi` table if it does not exist."""
    db = await get_db()
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS transaksi (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     BIGINT   NOT NULL,
            tipe        TEXT     NOT NULL,
            nominal     INTEGER  NOT NULL,
            kategori    TEXT     NOT NULL,
            keterangan  TEXT     NOT NULL DEFAULT '',
            tanggal     DATE     NOT NULL,
            created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_transaksi_user_tanggal "
        "ON transaksi(user_id, tanggal);"
    )
    await db.commit()
    logger.info("Database initialized: %s", DB_PATH)


async def close_db() -> None:
    """Gracefully close the database connection."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None
        logger.info("Database connection closed.")


async def insert_transaksi(
    user_id: int,
    tipe: str,
    nominal: int,
    kategori: str,
    keterangan: str,
    tanggal: str,
) -> int:
    """Insert a single transaction and return its row id."""
    db = await get_db()
    now = datetime.now(WIB).isoformat()
    cursor = await db.execute(
        """
        INSERT INTO transaksi (user_id, tipe, nominal, kategori, keterangan, tanggal, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (user_id, tipe, nominal, kategori, keterangan, tanggal, now),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore[return-value]


async def delete_last_transaksi(user_id: int) -> dict[str, Any] | None:
    """Delete the most recent transaction for a user; return its data or None."""
    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT * FROM transaksi WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user_id,),
    )
    if not row:
        return None

    record = dict(row[0])
    await db.execute("DELETE FROM transaksi WHERE id = ?", (record["id"],))
    await db.commit()
    return record


async def get_rekap(user_id: int, bulan: int, tahun: int) -> list[dict[str, Any]]:
    """Fetch all transactions for a user in a given month/year."""
    db = await get_db()
    start = f"{tahun:04d}-{bulan:02d}-01"
    if bulan == 12:
        end = f"{tahun + 1:04d}-01-01"
    else:
        end = f"{tahun:04d}-{bulan + 1:02d}-01"

    rows = await db.execute_fetchall(
        """
        SELECT tipe, nominal, kategori, keterangan, tanggal
        FROM transaksi
        WHERE user_id = ? AND tanggal >= ? AND tanggal < ?
        ORDER BY tanggal ASC, id ASC
        """,
        (user_id, start, end),
    )
    return [dict(r) for r in rows]


async def export_all(user_id: int) -> list[dict[str, Any]]:
    """Export every transaction for a user."""
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT id, tipe, nominal, kategori, keterangan, tanggal, created_at
        FROM transaksi
        WHERE user_id = ?
        ORDER BY tanggal ASC, id ASC
        """,
        (user_id,),
    )
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════
#  GOOGLE SHEETS SYNC LAYER
# ══════════════════════════════════════════════════════════════════════════

_gspread_client: gspread.Client | None = None


def _get_gspread_client() -> gspread.Client:
    """Get or initialize the Google Sheets client."""
    global _gspread_client
    if _gspread_client is None:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
        _gspread_client = gspread.authorize(creds)
    return _gspread_client


async def append_to_sheet(row_data: list[Any]) -> None:
    """Append a row to 'Keuangan Johan' Google Sheet asynchronously."""
    def _append():
        try:
            client = _get_gspread_client()
            sheet = client.open_by_key("1H9RIHnBLI2_ot_kSs6V9vzv6EcpttQvL0D1RFB1GzA").worksheet("Keuangan")
            sheet.append_row(row_data)
        except Exception as e:
            print(f"ERROR appending to Google Sheets: {e}")
            raise

    try:
        await asyncio.to_thread(_append)
        logger.info("Successfully appended to Google Sheets")
    except Exception as exc:
        logger.error("Failed to append to Google Sheets: %s", exc)


# ══════════════════════════════════════════════════════════════════════════
#  AI PARSING ENGINE (Google Gemini)
# ══════════════════════════════════════════════════════════════════════════

_gemini_client: genai.Client | None = None


def _get_gemini_client() -> genai.Client:
    global _gemini_client
    if _gemini_client is None:
        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    return _gemini_client


_SYSTEM_PROMPT = f"""\
Kamu adalah asisten keuangan. Tugasmu adalah mengekstrak transaksi keuangan \
dari pesan pengguna dalam Bahasa Indonesia.

ATURAN KETAT:
1. Kembalikan HANYA JSON array yang valid, TANPA teks tambahan, TANPA markdown fencing.
2. Setiap elemen array harus memiliki field berikut:
   - "tipe": salah satu dari "Pengeluaran" atau "Pemasukan"
   - "nominal": integer dalam Rupiah (konversi "rb" = ribu = ×1000, "jt" = juta = ×1.000.000)
   - "kategori": salah satu dari {json.dumps(KATEGORI_VALID, ensure_ascii=False)}
   - "keterangan": deskripsi singkat transaksi
   - "tanggal": format "YYYY-MM-DD". Jika tidak disebutkan, gunakan hari ini: {{today}}
3. Jika ada BEBERAPA transaksi dalam satu pesan, ekstrak SEMUA sebagai elemen terpisah.
4. Jika pesan tidak mengandung transaksi keuangan, kembalikan array kosong: []
5. Default tipe adalah "Pengeluaran" kecuali jelas merupakan pemasukan (gaji, transfer masuk, dll).
6. Pilih kategori yang PALING TEPAT dari daftar di atas. Jika tipe = "Pemasukan", kategori = "Pemasukan".
"""


def _build_system_prompt() -> str:
    today = datetime.now(WIB).strftime("%Y-%m-%d")
    return _SYSTEM_PROMPT.replace("{today}", today)


def _strip_json_fences(raw: str) -> str:
    """Remove markdown code fences that LLMs sometimes wrap around JSON."""
    raw = raw.strip()
    # Remove ```json ... ``` or ``` ... ```
    raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
    raw = re.sub(r"\n?```\s*$", "", raw)
    return raw.strip()


async def parse_transaksi_text(text: str) -> list[dict[str, Any]]:
    """Send user text to Gemini and parse the JSON response."""
    client = _get_gemini_client()

    response = await asyncio.to_thread(
        client.models.generate_content,
        model=GEMINI_MODEL,
        contents=text,
        config=genai.types.GenerateContentConfig(
            system_instruction=_build_system_prompt(),
            temperature=0.1,
        ),
    )

    raw_text = response.text or ""
    logger.info("Gemini raw response: %s", raw_text[:500])

    cleaned = _strip_json_fences(raw_text)
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        logger.error("JSON parse failed: %s — raw: %s", exc, raw_text[:300])
        return []

    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        logger.warning("Unexpected AI output type: %s", type(parsed).__name__)
        return []

    return _validate_transactions(parsed)


async def parse_transaksi_voice(voice_bytes: bytes, mime_type: str = "audio/ogg") -> list[dict[str, Any]]:
    """Send voice audio bytes to Gemini and parse the JSON response."""
    client = _get_gemini_client()

    # Build multimodal content: audio + text instruction
    audio_part = genai.types.Part.from_bytes(data=voice_bytes, mime_type=mime_type)
    text_part = (
        "Transkrip audio di atas dan ekstrak semua transaksi keuangan. "
        "Kembalikan HANYA JSON array sesuai format yang ditentukan."
    )

    response = await asyncio.to_thread(
        client.models.generate_content,
        model=GEMINI_MODEL,
        contents=[audio_part, text_part],
        config=genai.types.GenerateContentConfig(
            system_instruction=_build_system_prompt(),
            temperature=0.1,
        ),
    )

    raw_text = response.text or ""
    logger.info("Gemini voice raw response: %s", raw_text[:500])

    cleaned = _strip_json_fences(raw_text)
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        logger.error("Voice JSON parse failed: %s — raw: %s", exc, raw_text[:300])
        return []

    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return []

    return _validate_transactions(parsed)


def _validate_transactions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate and sanitize AI-parsed transactions."""
    valid: list[dict[str, Any]] = []
    today_str = datetime.now(WIB).strftime("%Y-%m-%d")

    for item in items:
        if not isinstance(item, dict):
            continue

        tipe = item.get("tipe", "Pengeluaran")
        if tipe not in ("Pengeluaran", "Pemasukan"):
            tipe = "Pengeluaran"

        try:
            nominal = int(item.get("nominal", 0))
        except (ValueError, TypeError):
            nominal = 0
        if nominal <= 0:
            continue  # skip nonsense

        kategori = item.get("kategori", "Lainnya")
        if kategori not in KATEGORI_VALID:
            kategori = "Lainnya"
        if tipe == "Pemasukan":
            kategori = "Pemasukan"

        keterangan = str(item.get("keterangan", ""))[:200]

        tanggal = item.get("tanggal", today_str)
        try:
            datetime.strptime(tanggal, "%Y-%m-%d")
        except (ValueError, TypeError):
            tanggal = today_str

        valid.append(
            {
                "tipe": tipe,
                "nominal": nominal,
                "kategori": kategori,
                "keterangan": keterangan,
                "tanggal": tanggal,
            }
        )

    return valid


# ══════════════════════════════════════════════════════════════════════════
#  FORMATTING HELPERS
# ══════════════════════════════════════════════════════════════════════════


def format_rupiah(n: int) -> str:
    """Format integer to Indonesian Rupiah string: Rp1.234.567"""
    return f"Rp{n:,.0f}".replace(",", ".")


def format_transaksi_summary(items: list[dict[str, Any]]) -> str:
    """Build a Markdown summary of recorded transactions."""
    if not items:
        return "⚠️ Tidak ada transaksi yang terdeteksi dari pesanmu."

    lines: list[str] = [f"✅ *Transaksi Dicatat{_escape_md('!')}*\n"]
    total_masuk = 0
    total_keluar = 0

    for i, t in enumerate(items, 1):
        emoji = "🟢" if t["tipe"] == "Pemasukan" else "🔴"
        nom = format_rupiah(t["nominal"])
        # Escape MarkdownV2 special chars in user-generated content
        ket = _escape_md(t["keterangan"])
        kat = _escape_md(t["kategori"])
        tgl = _escape_md(t["tanggal"])
        nom_escaped = _escape_md(nom)

        lines.append(f"{emoji} *{i}\\)* {ket}")
        lines.append(f"   Nominal: *{nom_escaped}*")
        lines.append(f"   Kategori: {kat}")
        lines.append(f"   Tanggal: {tgl}\n")

        if t["tipe"] == "Pemasukan":
            total_masuk += t["nominal"]
        else:
            total_keluar += t["nominal"]

    lines.append("─" * 25)
    if total_masuk > 0:
        lines.append(f"💰 Total Pemasukan: *{_escape_md(format_rupiah(total_masuk))}*")
    if total_keluar > 0:
        lines.append(f"💸 Total Pengeluaran: *{_escape_md(format_rupiah(total_keluar))}*")

    return "\n".join(lines)


def _escape_md(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    specials = r"_*[]()~`>#+-=|{}.!"
    escaped = ""
    for ch in text:
        if ch in specials:
            escaped += "\\" + ch
        else:
            escaped += ch
    return escaped


# ══════════════════════════════════════════════════════════════════════════
#  BOT ROUTER & HANDLERS
# ══════════════════════════════════════════════════════════════════════════

router = Router(name="main_router")


# ── Security Filter ──────────────────────────────────────────────────────

@router.message.outer_middleware()
async def auth_middleware(handler, event: Message, data: dict[str, Any]):
    """Block messages from users not in ALLOWED_USER_IDS."""
    user = event.from_user
    if user is None or user.id not in ALLOWED_USER_IDS:
        logger.warning(
            "Unauthorized access attempt from user_id=%s",
            user.id if user else "unknown",
        )
        return  # silently ignore
    return await handler(event, data)


# ── /start & /help ───────────────────────────────────────────────────────

HELP_TEXT = _escape_md(
    "🤖 Bot Keuangan Pribadi\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "📝 Cara Pakai:\n"
    '• Kirim teks: "Makan siang 25rb, bensin 50rb"\n'
    "• Kirim voice note: Sebutkan transaksimu\n"
    '• Multi transaksi: "Gaji 5jt dan bayar listrik 500rb"\n\n'
    "📋 Perintah:\n"
    "• /rekap — Rekap bulan ini\n"
    "• /rekap MM-YYYY — Rekap bulan tertentu\n"
    "• /batal — Hapus transaksi terakhir\n"
    "• /export — Download CSV semua transaksi\n"
    "• /help — Tampilkan bantuan ini\n\n"
    "💡 Tips:\n"
    '• Gunakan "rb" untuk ribuan, "jt" untuk jutaan\n'
    "• Bot otomatis menentukan kategori & tipe\n"
    "• Tanggal default = hari ini (WIB)\n"
)


@router.message(CommandStart())
@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT, parse_mode=ParseMode.MARKDOWN_V2)


# ── /rekap [MM-YYYY] ────────────────────────────────────────────────────

@router.message(Command("rekap"))
async def cmd_rekap(message: Message) -> None:
    user_id = message.from_user.id  # type: ignore[union-attr]
    args = (message.text or "").split(maxsplit=1)

    now = datetime.now(WIB)
    bulan, tahun = now.month, now.year

    if len(args) > 1:
        try:
            parts = args[1].strip().split("-")
            bulan = int(parts[0])
            tahun = int(parts[1])
            if not (1 <= bulan <= 12) or tahun < 2000:
                raise ValueError
        except (ValueError, IndexError):
            await message.answer(
                _escape_md("⚠️ Format salah. Gunakan: /rekap MM-YYYY (contoh: /rekap 10-2026)"),
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

    rows = await get_rekap(user_id, bulan, tahun)
    if not rows:
        await message.answer(
            _escape_md(f"📭 Tidak ada transaksi di bulan {bulan:02d}-{tahun}."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # Aggregate
    total_masuk = sum(r["nominal"] for r in rows if r["tipe"] == "Pemasukan")
    total_keluar = sum(r["nominal"] for r in rows if r["tipe"] == "Pengeluaran")
    saldo = total_masuk - total_keluar

    # Per-category breakdown
    cat_totals: dict[str, int] = {}
    for r in rows:
        key = f"{'🟢' if r['tipe'] == 'Pemasukan' else '🔴'} {r['kategori']}"
        cat_totals[key] = cat_totals.get(key, 0) + r["nominal"]

    lines: list[str] = [
        _escape_md(f"📊 Rekap Keuangan {bulan:02d}-{tahun}"),
        _escape_md("━" * 28),
        f"💰 Pemasukan : *{_escape_md(format_rupiah(total_masuk))}*",
        f"💸 Pengeluaran : *{_escape_md(format_rupiah(total_keluar))}*",
        f"💼 Saldo Bersih : *{_escape_md(format_rupiah(saldo))}*",
        "",
        _escape_md("📂 Per Kategori:"),
    ]
    for cat, total in sorted(cat_totals.items(), key=lambda x: -x[1]):
        lines.append(f"  • {_escape_md(cat)}: *{_escape_md(format_rupiah(total))}*")

    lines.append(f"\n_Total {_escape_md(str(len(rows)))} transaksi_")

    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)


# ── /batal ───────────────────────────────────────────────────────────────

@router.message(Command("batal"))
async def cmd_batal(message: Message) -> None:
    user_id = message.from_user.id  # type: ignore[union-attr]
    deleted = await delete_last_transaksi(user_id)
    if deleted is None:
        await message.answer(
            _escape_md("📭 Tidak ada transaksi untuk dibatalkan."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    emoji = "🟢" if deleted["tipe"] == "Pemasukan" else "🔴"
    lines = [
        _escape_md("🗑️ Transaksi Terakhir Dihapus:"),
        "",
        f"{_escape_md(emoji)} {_escape_md(deleted['keterangan'])}",
        f"  Tipe: {_escape_md(deleted['tipe'])}",
        f"  Nominal: *{_escape_md(format_rupiah(deleted['nominal']))}*",
        f"  Kategori: {_escape_md(deleted['kategori'])}",
        f"  Tanggal: {_escape_md(str(deleted['tanggal']))}",
    ]
    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)


# ── /export ──────────────────────────────────────────────────────────────

@router.message(Command("export"))
async def cmd_export(message: Message) -> None:
    user_id = message.from_user.id  # type: ignore[union-attr]
    rows = await export_all(user_id)
    if not rows:
        await message.answer(
            _escape_md("📭 Belum ada transaksi untuk di-export."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # Build CSV in-memory
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=["id", "tipe", "nominal", "kategori", "keterangan", "tanggal", "created_at"],
    )
    writer.writeheader()
    writer.writerows(rows)

    csv_bytes = buf.getvalue().encode("utf-8-sig")  # BOM for Excel compatibility
    now_str = datetime.now(WIB).strftime("%Y%m%d_%H%M%S")
    filename = f"transaksi_{now_str}.csv"

    doc = BufferedInputFile(csv_bytes, filename=filename)
    await message.answer_document(
        doc,
        caption=_escape_md(f"📁 Export {len(rows)} transaksi"),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ── Text Message Handler ────────────────────────────────────────────────

@router.message(F.text & ~F.text.startswith("/"))
async def handle_text(message: Message) -> None:
    user_id = message.from_user.id  # type: ignore[union-attr]
    text = message.text or ""
    if not text.strip():
        return

    processing_msg = await message.answer(_escape_md("⏳ Memproses transaksi..."), parse_mode=ParseMode.MARKDOWN_V2)

    try:
        transactions = await parse_transaksi_text(text)
    except Exception as exc:
        logger.exception("AI parsing error: %s", exc)
        await processing_msg.edit_text(
            _escape_md("❌ Gagal memproses pesan. Silakan coba lagi."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if not transactions:
        await processing_msg.edit_text(
            _escape_md(
                "⚠️ Tidak ada transaksi yang terdeteksi.\n"
                "Coba format seperti: \"Makan siang 25rb\""
            ),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # Insert all transactions and sync to Google Sheets
    for t in transactions:
        await insert_transaksi(
            user_id=user_id,
            tipe=t["tipe"],
            nominal=t["nominal"],
            kategori=t["kategori"],
            keterangan=t["keterangan"],
            tanggal=t["tanggal"],
        )
        now_str = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
        await append_to_sheet([t["tanggal"], t["tipe"], t["nominal"], t["kategori"], t["keterangan"], now_str])

    summary = format_transaksi_summary(transactions)
    await processing_msg.edit_text(summary, parse_mode=ParseMode.MARKDOWN_V2)
    logger.info("User %d recorded %d transactions", user_id, len(transactions))


# ── Voice Message Handler ───────────────────────────────────────────────

@router.message(F.voice)
async def handle_voice(message: Message, bot: Bot) -> None:
    user_id = message.from_user.id  # type: ignore[union-attr]

    processing_msg = await message.answer(
        _escape_md("🎤 Memproses voice note..."), parse_mode=ParseMode.MARKDOWN_V2
    )

    try:
        # Download voice file
        voice = message.voice
        if voice is None:
            return

        file = await bot.get_file(voice.file_id)
        if file.file_path is None:
            await processing_msg.edit_text(
                _escape_md("❌ Gagal mengunduh voice note."),
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

        voice_data = io.BytesIO()
        await bot.download_file(file.file_path, voice_data)
        voice_bytes = voice_data.getvalue()

        mime = voice.mime_type or "audio/ogg"
        transactions = await parse_transaksi_voice(voice_bytes, mime_type=mime)

    except Exception as exc:
        logger.exception("Voice processing error: %s", exc)
        await processing_msg.edit_text(
            _escape_md("❌ Gagal memproses voice note. Silakan coba lagi."),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if not transactions:
        await processing_msg.edit_text(
            _escape_md(
                "⚠️ Tidak ada transaksi yang terdeteksi dari voice note.\n"
                "Coba sebutkan dengan jelas, contoh: \"Makan siang dua puluh lima ribu\""
            ),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # Insert all transactions and sync to Google Sheets
    for t in transactions:
        await insert_transaksi(
            user_id=user_id,
            tipe=t["tipe"],
            nominal=t["nominal"],
            kategori=t["kategori"],
            keterangan=t["keterangan"],
            tanggal=t["tanggal"],
        )
        now_str = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
        await append_to_sheet([t["tanggal"], t["tipe"], t["nominal"], t["kategori"], t["keterangan"], now_str])

    summary = format_transaksi_summary(transactions)
    await processing_msg.edit_text(summary, parse_mode=ParseMode.MARKDOWN_V2)
    logger.info("User %d recorded %d transactions via voice", user_id, len(transactions))


# ══════════════════════════════════════════════════════════════════════════
#  APPLICATION ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════


async def main() -> None:
    _validate_env()

    logger.info("Starting Bot Keuangan...")
    logger.info("Allowed users: %s", ALLOWED_USER_IDS)
    logger.info("Gemini model: %s", GEMINI_MODEL)

    # Initialize database
    await init_db()

    # Build bot & dispatcher
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
    )
    dp = Dispatcher()
    dp.include_router(router)

    # Graceful shutdown
    dp.shutdown.register(close_db)

    logger.info("Bot is polling...")
    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
