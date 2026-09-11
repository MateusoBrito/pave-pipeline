"""Generate a data completeness report for MongoDB and PostgreSQL."""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import select

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from src.database.models import AlvoColeta, Documento
from src.database.postgres import get_session

load_dotenv(ROOT_DIR / ".env")

DEFAULT_OUTPUT = ROOT_DIR / "reports" / "monitoramento_pipeline.md"


@dataclass(frozen=True)
class MongoRecord:
    """Represent the fields needed to audit one MongoDB document."""

    native_id: str | None
    target_id: int | None
    collected_at: datetime | None
    processed: bool


PLATFORM_COLLECTIONS = {
    "reddit": ("reddit",),
    "meta": ("meta",),
    "youtube": ("youtube_videos", "youtube_comments"),
}


def get_mongo_database() -> Any:
    """Return the configured MongoDB database."""
    user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    database = os.getenv("MONGO_DATABASE", "panorama")

    if user and password:
        uri = f"mongodb://{user}:{password}@{host}:{port}/"
    else:
        uri = os.getenv("MONGO_URI", f"mongodb://{host}:{port}/")

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client[database]


def object_id_datetime(value: Any) -> datetime | None:
    """Return the UTC creation time embedded in a MongoDB ObjectId."""
    if isinstance(value, ObjectId):
        return value.generation_time.astimezone(timezone.utc)
    return None


def normalize_target(value: Any) -> str | None:
    """Normalize a collection target while preserving meaningful IDs."""
    if value is None:
        return None
    text = str(value).strip()
    if text.startswith("r/"):
        text = text[2:]
    return text or None


def target_id_for(
    target_map: dict[tuple[str, str], int],
    channel: Any,
    term: Any = "",
) -> int | None:
    """Find a PostgreSQL collection target for a MongoDB document."""
    if channel is None:
        return None
    channel_text = str(channel).strip()
    term_text = "" if term is None else str(term).strip()
    return target_map.get((channel_text, term_text)) or target_map.get(
        (normalize_target(channel_text) or "", term_text)
    )


def load_target_maps(session) -> dict[str, dict[tuple[str, str], int]]:
    """Load collection-target mappings from PostgreSQL."""
    result: dict[str, dict[tuple[str, str], int]] = defaultdict(dict)
    rows = session.execute(
        select(
            AlvoColeta.id,
            AlvoColeta.fonte_codigo,
            AlvoColeta.canal,
            AlvoColeta.termo_busca,
        )
    )
    for target_id, platform, channel, term in rows:
        result[platform][(channel, term)] = target_id
        normalized_channel = normalize_target(channel)
        if normalized_channel:
            result[platform][(normalized_channel, term)] = target_id
    return result


def load_postgres_keys(session) -> dict[str, set[tuple[int, str]]]:
    """Load document keys grouped by platform from PostgreSQL."""
    result: dict[str, set[tuple[int, str]]] = defaultdict(set)
    rows = session.execute(
        select(Documento.alvo_coleta_id, Documento.id_nativo, AlvoColeta.fonte_codigo)
        .join(AlvoColeta, Documento.alvo_coleta_id == AlvoColeta.id)
    )
    for target_id, native_id, platform in rows:
        result[platform].add((target_id, native_id))
    return result


def youtube_video_channels(mongo_db: Any) -> dict[str, str]:
    """Build a video-to-channel map used to resolve YouTube comments."""
    return {
        str(document["videoId"]): str(document["channelId"])
        for document in mongo_db["youtube_videos"].find(
            {"videoId": {"$exists": True}, "channelId": {"$exists": True}},
            {"videoId": 1, "channelId": 1},
        )
    }


def mongo_record(
    platform: str,
    collection: str,
    document: dict[str, Any],
    target_map: dict[tuple[str, str], int],
    video_channels: dict[str, str],
) -> MongoRecord:
    """Convert a MongoDB document into the audit representation."""
    if platform == "reddit":
        native_id = document.get("id")
        target_id = target_id_for(
            target_map,
            document.get("_subreddit_busca"),
            document.get("_termo_busca", ""),
        )
    elif platform == "meta":
        native_id = document.get("id")
        target_id = target_id_for(
            target_map,
            document.get("page_id") or document.get("_entidade_busca"),
        )
    elif collection == "youtube_videos":
        native_id = document.get("videoId")
        target_id = target_id_for(target_map, document.get("channelId"))
    else:
        native_id = document.get("commentId")
        video_id = document.get("videoId")
        target_id = target_id_for(target_map, video_channels.get(str(video_id)))

    return MongoRecord(
        native_id=str(native_id) if native_id is not None else None,
        target_id=target_id,
        collected_at=object_id_datetime(document.get("_id")),
        processed="processed_text" in document,
    )


def inspect_platform(
    platform: str,
    mongo_db: Any,
    target_map: dict[tuple[str, str], int],
    video_channels: dict[str, str],
) -> tuple[dict[str, Any], Counter[str], set[tuple[int, str]]]:
    """Collect daily MongoDB metrics and comparable document keys."""
    daily: Counter[str] = Counter()
    raw_keys: set[tuple[int, str]] = set()
    total = 0
    unprocessed = 0
    without_id = 0
    without_target = 0

    for collection in PLATFORM_COLLECTIONS[platform]:
        projection = {"_id": 1, "processed_text": 1}
        if platform == "reddit":
            projection.update({"id": 1, "_subreddit_busca": 1, "_termo_busca": 1})
        elif platform == "meta":
            projection.update({"id": 1, "page_id": 1, "_entidade_busca": 1})
        elif collection == "youtube_videos":
            projection.update({"videoId": 1, "channelId": 1})
        else:
            projection.update({"commentId": 1, "videoId": 1})

        for document in mongo_db[collection].find({}, projection):
            record = mongo_record(
                platform, collection, document, target_map, video_channels
            )
            total += 1
            if not record.processed:
                unprocessed += 1
            if record.native_id is None:
                without_id += 1
            elif record.target_id is None:
                without_target += 1
            else:
                raw_keys.add((record.target_id, record.native_id))

            day = record.collected_at.date().isoformat() if record.collected_at else "unknown"
            daily[day] += 1

    return {
        "mongo_total": total,
        "unprocessed": unprocessed,
        "without_id": without_id,
        "without_target": without_target,
    }, daily, raw_keys


def format_bool(value: bool) -> str:
    """Format a boolean for the Portuguese report."""
    return "SIM" if value else "NÃO"


def build_report(
    generated_at: datetime,
    summaries: dict[str, dict[str, Any]],
    daily: dict[str, Counter[str]],
) -> str:
    """Build the Markdown monitoring report."""
    lines = [
        "# Monitoramento do pipeline",
        "",
        f"Gerado em: `{generated_at.isoformat()}`",
        "",
        "## Resumo por plataforma",
        "",
        "| Plataforma | MongoDB | PostgreSQL | Sem pré-processar | Ausentes no PostgreSQL | Sem alvo | Sem ID | Tudo carregado |",
        "|---|---:|---:|---:|---:|---:|---:|:---:|",
    ]
    for platform in PLATFORM_COLLECTIONS:
        item = summaries[platform]
        lines.append(
            f"| {platform} | {item['mongo_total']} | {item['postgres_total']} | "
            f"{item['unprocessed']} | {item['missing_postgres']} | "
            f"{item['without_target']} | {item['without_id']} | "
            f"{format_bool(item['all_loaded'])} |"
        )

    lines.extend(
        [
            "",
            "## Quantidade diária coletada",
            "",
            "A data usa o timestamp UTC do `_id` do MongoDB, equivalente ao momento de coleta.",
            "",
            "| Data UTC | Plataforma | Documentos no MongoDB |",
            "|---|---|---:|",
        ]
    )
    for platform in PLATFORM_COLLECTIONS:
        for day, count in sorted(daily[platform].items()):
            lines.append(f"| {day} | {platform} | {count} |")

    lines.extend(
        [
            "",
            "## Critérios",
            "",
            "- `Sem pré-processar`: documento sem o campo `processed_text` no MongoDB.",
            "- `Ausentes no PostgreSQL`: chave `(alvo_coleta_id, id_nativo)` presente no MongoDB e ausente na tabela `documento`.",
            "- `Sem alvo`: documento cujo canal/termo não encontrou um `alvo_coleta` no PostgreSQL.",
            "- `Tudo carregado`: não há documentos sem alvo, sem ID ou ausentes no PostgreSQL.",
        ]
    )
    return "\n".join(lines) + "\n"


def generate_report(output_path: Path) -> Path:
    """Generate and save the pipeline monitoring report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    session = get_session()
    mongo_client = None
    try:
        target_maps = load_target_maps(session)
        postgres_keys = load_postgres_keys(session)
        mongo_db = get_mongo_database()
        mongo_client = mongo_db.client
        video_channels = youtube_video_channels(mongo_db)

        summaries: dict[str, dict[str, Any]] = {}
        daily: dict[str, Counter[str]] = {}
        for platform in PLATFORM_COLLECTIONS:
            summary, platform_daily, mongo_keys = inspect_platform(
                platform,
                mongo_db,
                target_maps.get(platform, {}),
                video_channels,
            )
            postgres_platform_keys = postgres_keys.get(platform, set())
            summary.update(
                postgres_total=len(mongo_keys & postgres_platform_keys),
                missing_postgres=len(mongo_keys - postgres_platform_keys),
                extra_postgres=len(postgres_platform_keys - mongo_keys),
            )
            summary["all_loaded"] = not any(
                (
                    summary["missing_postgres"],
                    summary["without_target"],
                    summary["without_id"],
                )
            )
            summaries[platform] = summary
            daily[platform] = platform_daily

        report = build_report(datetime.now(timezone.utc), summaries, daily)
        output_path.write_text(report, encoding="utf-8")
        return output_path
    finally:
        if mongo_client is not None:
            mongo_client.close()
        session.close()


def main() -> None:
    """Generate the monitoring report from the configured databases."""
    parser = argparse.ArgumentParser(description="Gera o relatório de monitoramento do pipeline")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Caminho do relatório Markdown de saída",
    )
    args = parser.parse_args()
    print(f"Relatório gerado em: {generate_report(args.output)}")


if __name__ == "__main__":
    main()