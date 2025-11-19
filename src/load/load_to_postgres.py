# src/load/load_to_postgres.py
import os, re, json, hashlib
from glob import glob
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from src.utils.config import DB_CONN, RAW_DIR

# -------- DB helpers --------
def get_engine():
    return create_engine(DB_CONN, future=True)

def ensure_schema_and_tables(engine):
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.trips (
        trip_id TEXT PRIMARY KEY,
        started_at TIMESTAMPTZ,
        ended_at TIMESTAMPTZ,
        start_station_id TEXT,
        start_station_name TEXT,
        end_station_id TEXT,
        end_station_name TEXT,
        start_lat DOUBLE PRECISION,
        start_lng DOUBLE PRECISION,
        end_lat DOUBLE PRECISION,
        end_lng DOUBLE PRECISION,
        member_casual TEXT,
        rideable_type TEXT,
        duration_seconds INT,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS raw.gbfs_station_information (
        ts TIMESTAMPTZ NOT NULL,
        doc JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS raw.gbfs_station_status (
        ts TIMESTAMPTZ NOT NULL,
        doc JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS raw.weather_raw (
        ts TIMESTAMPTZ NOT NULL,
        doc JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS raw.gbfs_system_regions (
        ts TIMESTAMPTZ NOT NULL,
        doc JSONB NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

# -------- Trips loader --------
def _hash_trip(row):
    # Stable hash à partir de champs discriminants (tolère variations de colonnes)
    parts = [
        str(row.get("started_at") or ""),
        str(row.get("ended_at") or ""),
        str(row.get("start_station_id") or ""),
        str(row.get("end_station_id") or ""),
        str(row.get("member_casual") or ""),
        str(row.get("rideable_type") or ""),
    ]
    return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()

def _normalize_trips_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    df = df.rename(columns={v: k for k, v in cols.items()})  # lower snake-ish

    # Harmoniser principaux champs (anciens jeux vs récents)
    # Récents (2021+) : started_at, ended_at, start_station_id, end_station_id, start_lat, start_lng, ...
    # Anciens (<=2019) : starttime, stoptime, start station id, end station id, start station latitude, ...
    mapping = {
        "starttime": "started_at",
        "stoptime": "ended_at",
        "start_station_id": "start_station_id",
        "end_station_id": "end_station_id",
        "start station id": "start_station_id",
        "end station id": "end_station_id",
        "start_station_name": "start_station_name",
        "end_station_name": "end_station_name",
        "start station name": "start_station_name",
        "end station name": "end_station_name",
        "start_lat": "start_lat",
        "start_lng": "start_lng",
        "end_lat": "end_lat",
        "end_lng": "end_lng",
        "start station latitude": "start_lat",
        "start station longitude": "start_lng",
        "end station latitude": "end_lat",
        "end station longitude": "end_lng",
        "member_casual": "member_casual",
        "usertype": "member_casual",  # ancien : 'Subscriber'/'Customer'
        "rideable_type": "rideable_type",
        "bikeid": "rideable_type",    # fallback minimal pour hashing
    }

    for src, tgt in mapping.items():
        if src in df.columns and tgt not in df.columns:
            df[tgt] = df[src]

    # Normaliser member_casual à partir de usertype si besoin
    if "member_casual" in df.columns and df["member_casual"].dtype == object:
        df["member_casual"] = (
            df["member_casual"]
            .astype(str)
            .str.lower()
            .replace({"subscriber": "member", "customer": "casual"})
        )

    # Dates
    for col in ("started_at", "ended_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Coords
    for col in ("start_lat", "start_lng", "end_lat", "end_lng"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Durée
    if "started_at" in df.columns and "ended_at" in df.columns:
        dur = (df["ended_at"] - df["started_at"]).dt.total_seconds()
        df["duration_seconds"] = dur.fillna(0).astype("int64", errors="ignore")

    # Colonnes finales attendues (certaines peuvent manquer → remplissage NaN)
    final_cols = [
        "started_at","ended_at",
        "start_station_id","start_station_name",
        "end_station_id","end_station_name",
        "start_lat","start_lng","end_lat","end_lng",
        "member_casual","rideable_type","duration_seconds"
    ]
    for c in final_cols:
        if c not in df.columns:
            df[c] = None

    # trip_id pour dédoublonner
    df["trip_id"] = df.apply(_hash_trip, axis=1)
    return df[["trip_id"] + final_cols]

def load_trips_from_csvs(engine, raw_dir: str):
    # CSV possibles dans data/raw et data/raw/citibike
    patterns = [
        os.path.join(raw_dir, "*.csv"),
        os.path.join(raw_dir, "citibike", "*.csv"),
    ]
    files = []
    for p in patterns:
        files.extend(glob(p))
    files = sorted(files)

    if not files:
        print("ℹ️  Aucun CSV de trips trouvé dans data/raw/.")
        return

    for f in files:
        print(f"→ Chargement trips depuis {f}")
        # Chargement en chunks pour gros fichiers
        chunks = pd.read_csv(f, low_memory=False, chunksize=200_000)
        total = 0
        for chunk in chunks:
            df = _normalize_trips_df(chunk)
            df["source_file"] = os.path.basename(f)

            # On insère en staging, puis upsert vers raw.trips
            with engine.begin() as conn:
                df.to_sql("_trips_stage", con=conn, schema="raw", if_exists="replace", index=False)
                upsert = """
                INSERT INTO raw.trips AS t (
                    trip_id, started_at, ended_at,
                    start_station_id, start_station_name,
                    end_station_id, end_station_name,
                    start_lat, start_lng, end_lat, end_lng,
                    member_casual, rideable_type, duration_seconds,
                    source_file
                )
                SELECT
                    trip_id, started_at, ended_at,
                    start_station_id, start_station_name,
                    end_station_id, end_station_name,
                    start_lat, start_lng, end_lat, end_lng,
                    member_casual, rideable_type, duration_seconds,
                    source_file
                FROM raw._trips_stage
                ON CONFLICT (trip_id) DO UPDATE
                SET
                    duration_seconds = EXCLUDED.duration_seconds,
                    source_file = EXCLUDED.source_file
                ;
                DROP TABLE IF EXISTS raw._trips_stage;
                """
                conn.execute(text(upsert))
            total += len(df)
        print(f"✅ {total} lignes insérées/dédupliquées (raw.trips)")

# -------- GBFS loader --------
def _latest_gbfs_snapshots(base_dir: str):
    snap_root = os.path.join(base_dir, "gbfs")
    if not os.path.isdir(snap_root):
        return None
    # dossiers horodatés YYYYMMDDTHHMMSSZ
    snaps = [d for d in glob(os.path.join(snap_root, "*")) if os.path.isdir(d)]
    if not snaps:
        return None
    snaps.sort()
    return snaps[-1]

def load_gbfs(engine, raw_dir: str):
    snap_dir = _latest_gbfs_snapshots(raw_dir)
    if not snap_dir:
        print("ℹ️  Aucun snapshot GBFS trouvé dans data/raw/gbfs/.")
        return

    def _load_one(json_name: str, table: str):
        path = os.path.join(snap_dir, json_name)
        if not os.path.isfile(path):
            print(f"ℹ️  {json_name} introuvable dans {snap_dir}")
            return
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)
        # ts : utiliser last_updated si dispo, sinon maintenant
        ts_epoch = doc.get("last_updated")
        if ts_epoch is not None:
            ts = datetime.fromtimestamp(int(ts_epoch), tz=timezone.utc)
        else:
            ts = datetime.now(tz=timezone.utc)
        payload = {"ts": ts, "doc": json.dumps(doc)}
        df = pd.DataFrame([payload])
        with engine.begin() as conn:
            df.to_sql(table, con=conn, schema="raw", if_exists="append", index=False)
        print(f"✅ GBFS → raw.{table}: {path}")

    _load_one("station_information.json", "gbfs_station_information")
    _load_one("station_status.json", "gbfs_station_status")
    _load_one("system_regions.json", "gbfs_system_regions")


# -------- Entrée principale --------
def load_to_postgres():
    os.makedirs(RAW_DIR, exist_ok=True)
    engine = get_engine()
    ensure_schema_and_tables(engine)
    load_trips_from_csvs(engine, RAW_DIR)
    load_gbfs(engine, RAW_DIR)
    print("🎯 Chargement terminé.")

if __name__ == "__main__":
    load_to_postgres()
