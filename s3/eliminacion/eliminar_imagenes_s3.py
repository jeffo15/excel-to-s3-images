# eliminar_imagenes_s3.py
# - Entrada: archivo de texto (una URL/path por línea) O CSV con una columna de URLs
# - Acepta por celda: key, s3://bucket/key, o URL HTTPS pública
# - Elimina los objetos en S3 en paralelo
# - Guarda LOGS: delete_ok.csv / delete_errors.csv
#
# Requisitos:
#   py -m pip install boto3 pandas
#
# Variables recomendadas en .env o en el sistema (el script las lee al iniciar):
#   AWS_REGION
#   AWS_S3_BUCKET_NAME
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#
# Por defecto el borrado usa SIEMPRE el bucket AWS_S3_BUCKET_NAME y solo extrae la key
# desde cada URL o línea (--bucket-source url para usar el bucket que venga en la URL).
#
# Ejemplos (desde la raíz del repositorio):
#   py s3/eliminacion/eliminar_imagenes_s3.py --paths-file a_borrar.txt --dry-run
#   py s3/eliminacion/eliminar_imagenes_s3.py --format csv --paths-file urls.csv --dry-run
#   py s3/eliminacion/eliminar_imagenes_s3.py --format csv --paths-file urls.csv --url-col Url --workers 16
#
# Formato TXT (una entrada por línea):
#   marketfit/carpeta/imagen.png
#   s3://mi-bucket/otra/ruta.jpg
#   https://mi-bucket.s3.amazonaws.com/ruta/archivo.webp
#
# Formato CSV: una columna con URLs (si el archivo tiene varias columnas, usa --url-col).
#
# Líneas vacías y líneas que empiezan por # (solo TXT) se ignoran.

import argparse
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError


def load_env_dotenv() -> None:
    """
    Carga variables desde .env: directorio actual, raíz del repo (dos niveles arriba del script), carpeta del script.
    No sobrescribe variables que ya existan en el entorno del proceso.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.normpath(os.path.join(here, "..", ".."))
    candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(repo_root, ".env"),
        os.path.join(here, ".env"),
    ]
    for path in candidates:
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8-sig") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
        break


def _env_strip(name: str) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return None
    s = v.strip()
    return s if s else None


def aws_region_from_env() -> str | None:
    r = _env_strip("AWS_REGION") or _env_strip("AWS_DEFAULT_REGION")
    return r or None


def build_s3_client(region: str | None, boto_cfg: Config):
    """
    Cliente S3 con credenciales explícitas desde el entorno (evita perfil/default distinto al .env).
    """
    ak = _env_strip("AWS_ACCESS_KEY_ID")
    sk = _env_strip("AWS_SECRET_ACCESS_KEY")
    st = _env_strip("AWS_SESSION_TOKEN")
    kwargs: dict = {"config": boto_cfg}
    if region:
        kwargs["region_name"] = region
    if ak and sk:
        kwargs["aws_access_key_id"] = ak
        kwargs["aws_secret_access_key"] = sk
        if st:
            kwargs["aws_session_token"] = st
    return boto3.client("s3", **kwargs)


def extract_bucket_and_key_from_public_s3_url(url: str):
    """
    Igual que en migrar_imagenes_publicas.py:
      1) https://s3.amazonaws.com/<bucket>/<key>
      2) https://s3.<region>.amazonaws.com/<bucket>/<key>
      3) https://<bucket>.s3.amazonaws.com/<key>
      4) https://<bucket>.s3.<region>.amazonaws.com/<key>
    Retorna (bucket, key) o (None, None)
    """
    u = urlparse(url.strip())
    host = u.netloc.lower()
    path = unquote(u.path.lstrip("/"))

    if host == "s3.amazonaws.com" or (host.startswith("s3.") and host.endswith(".amazonaws.com")):
        parts = path.split("/", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else (None, None)

    m = re.match(r"^(?P<bucket>[^.]+)\.s3(\.[^.]+)?\.amazonaws\.com$", host)
    if m:
        return m.group("bucket"), path

    return None, None


def parse_s3_uri(uri: str):
    """s3://bucket/key -> (bucket, key) o (None, None)"""
    u = uri.strip()
    if not u.lower().startswith("s3://"):
        return None, None
    rest = u[5:].lstrip("/")
    parts = rest.split("/", 1)
    if len(parts) != 2 or not parts[0]:
        return None, None
    return parts[0], unquote(parts[1])


def resolve_bucket_and_key(line: str, default_bucket: str):
    """
    Retorna (bucket, key, error_str). error_str vacío si OK.
    Usa el bucket que indique la URL o el s3://.
    """
    s = line.strip()
    if not s:
        return None, None, "empty line"

    b, k = parse_s3_uri(s)
    if b and k:
        return b, k, ""

    if s.lower().startswith("http://") or s.lower().startswith("https://"):
        b, k = extract_bucket_and_key_from_public_s3_url(s)
        if b and k:
            return b, k, ""
        return None, None, "could not parse bucket/key from URL"

    if not default_bucket:
        return None, None, "line is a key but --bucket was not set"

    key = s.lstrip("/")
    return default_bucket, key, ""


def resolve_bucket_and_key_env_bucket(line: str, bucket: str):
    """
    Siempre borra en `bucket` (p. ej. AWS_S3_BUCKET_NAME). Solo obtiene la object key desde la línea/URL.
    Retorna (bucket, key, error_str).
    """
    s = line.strip()
    if not s:
        return None, None, "empty line"
    if not bucket:
        return None, None, "falta AWS_S3_BUCKET_NAME o --bucket"

    b, k = parse_s3_uri(s)
    if b and k:
        return bucket, k, ""

    if s.lower().startswith("http://") or s.lower().startswith("https://"):
        _b, k = extract_bucket_and_key_from_public_s3_url(s)
        if k:
            return bucket, k, ""
        return None, None, "could not parse key from URL"

    return bucket, s.lstrip("/"), ""


def delete_one(s3, bucket: str, key: str, *, dry_run: bool):
    if dry_run:
        return True, None, {"dry_run": True}
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        return True, None, {"dry_run": False}
    except ClientError as e:
        return False, str(e), {"dry_run": False}
    except Exception as e:
        return False, str(e), {"dry_run": False}


def load_path_lines(paths_file: str) -> list[str]:
    with open(paths_file, encoding="utf-8") as f:
        lines = f.readlines()
    out = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        out.append(stripped)
    return out


def load_urls_from_csv(paths_file: str, url_col: str | None, encoding: str) -> tuple[list[str], str]:
    df = pd.read_csv(paths_file, encoding=encoding)
    if df.empty:
        return [], ""

    if url_col:
        col = url_col.strip()
        if col not in df.columns:
            raise SystemExit(f'No existe la columna "{col}". Columnas: {list(df.columns)}')
    else:
        if len(df.columns) == 1:
            col = df.columns[0]
        else:
            raise SystemExit(
                "El CSV tiene más de una columna. Indica cuál tiene las URLs con --url-col. "
                f"Columnas: {list(df.columns)}"
            )

    series = df[col].astype(str).str.strip()
    series = series.replace({"nan": "", "None": "", "<NA>": ""})
    out = []
    for val in series:
        if not val:
            continue
        out.append(val)
    return out, col


def main():
    load_env_dotenv()

    parser = argparse.ArgumentParser(
        description="Eliminación masiva de objetos en S3 a partir de un listado de paths (paralelo + logs)"
    )
    parser.add_argument(
        "--paths-file",
        required=True,
        help="Archivo de entrada: .txt (una ruta por línea) o .csv según --format",
    )
    parser.add_argument(
        "--format",
        choices=("txt", "csv"),
        default="txt",
        help='Tipo de archivo: "txt" línea a línea, "csv" una columna de URLs (default: txt)',
    )
    parser.add_argument(
        "--url-col",
        default="",
        help='Nombre de la columna con URLs en el CSV. Si el CSV tiene una sola columna, se usa automáticamente.',
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Codificación del CSV (default: utf-8)",
    )
    parser.add_argument(
        "--bucket",
        default="",
        help="Bucket por defecto para keys sin URL. Si omites, se usa AWS_S3_BUCKET_NAME del entorno/.env.",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Región del cliente S3. Si omites, se usa AWS_REGION o AWS_DEFAULT_REGION del entorno/.env.",
    )
    parser.add_argument(
        "--bucket-source",
        choices=("env", "url"),
        default=None,
        help='De dónde sale el bucket al borrar: "env" = siempre AWS_S3_BUCKET_NAME/--bucket; '
        '"url" = el que venga en la URL/s3:// (default: env si hay bucket en .env, si no url).',
    )
    parser.add_argument("--workers", type=int, default=16, help="Hilos en paralelo (default: 16)")
    parser.add_argument("--max", type=int, default=0, help="Limitar a N eliminaciones (0 = todas)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No borra nada; solo valida paths y escribe el log de éxito simulado.",
    )
    parser.add_argument("--ok-log", default="delete_ok.csv", help="CSV de eliminaciones OK (default: delete_ok.csv)")
    parser.add_argument(
        "--error-log",
        default="delete_errors.csv",
        help="CSV de líneas con error (default: delete_errors.csv)",
    )

    args = parser.parse_args()

    region = args.region or aws_region_from_env()
    default_bucket = (args.bucket or "").strip() or (_env_strip("AWS_S3_BUCKET_NAME") or "")

    if args.bucket_source:
        bucket_source = args.bucket_source
    else:
        bucket_source = "env" if default_bucket else "url"

    if args.format == "csv":
        url_col = args.url_col.strip() or None
        lines, csv_col = load_urls_from_csv(args.paths_file, url_col, args.encoding)
        print("CSV columna de URLs:", csv_col or "(vacío)")
    else:
        lines = load_path_lines(args.paths_file)
    if args.max and args.max > 0:
        lines = lines[: args.max]

    total_lines = len(lines)
    print(f"Líneas a procesar: {total_lines}")
    print("Dry-run:", args.dry_run)
    print("Workers:", args.workers)
    print("Región cliente S3:", region or "(FALTA: define AWS_REGION en .env)")
    print("Modo bucket:", bucket_source, "(env = siempre el bucket de la variable; url = bucket en la URL)")
    print("Bucket usado al borrar:", default_bucket or ("(derivado de cada URL)" if bucket_source == "url" else "(no definido)"))
    ak = _env_strip("AWS_ACCESS_KEY_ID")
    sk = _env_strip("AWS_SECRET_ACCESS_KEY")
    print(
        "Credenciales AWS:",
        "OK (ACCESS_KEY + SECRET presentes)"
        if ak and sk
        else "FALTA AWS_ACCESS_KEY_ID y/o AWS_SECRET_ACCESS_KEY — revisa .env",
    )

    if not ak or not sk:
        raise SystemExit(
            "Configura AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY en .env o en el entorno antes de ejecutar."
        )
    if not region:
        raise SystemExit("Configura AWS_REGION en .env o pasa --region.")
    if bucket_source == "env" and not default_bucket:
        raise SystemExit(
            "Modo bucket=env: define AWS_S3_BUCKET_NAME o usa --bucket con el nombre del bucket."
        )

    boto_cfg = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        max_pool_connections=max(50, args.workers * 2),
    )
    s3 = build_s3_client(region, boto_cfg)

    # Resolver y validar todas las líneas antes del pool (errores de parseo)
    jobs = []
    parse_errors = []
    for i, line in enumerate(lines):
        if bucket_source == "env":
            b, k, err = resolve_bucket_and_key_env_bucket(line, default_bucket)
        else:
            b, k, err = resolve_bucket_and_key(line, default_bucket)
        if err:
            parse_errors.append({"line_index": i, "raw_line": line, "error": err})
            continue
        jobs.append((i, line, b, k))

    ok_rows = []
    error_rows = []

    for row in parse_errors:
        error_rows.append(
            {
                "line_index": row["line_index"],
                "raw_line": row["raw_line"],
                "bucket": "",
                "key": "",
                "error": row["error"],
            }
        )

    start = time.time()
    ok_count = 0
    fail_count = len(parse_errors)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = []
        for (line_idx, raw_line, bucket, key) in jobs:
            fut = ex.submit(delete_one, s3, bucket, key, dry_run=args.dry_run)
            fut._ctx = (line_idx, raw_line, bucket, key)
            futures.append(fut)

        done = 0
        total_futures = len(futures)
        for fut in as_completed(futures):
            done += 1
            line_idx, raw_line, bucket, key = fut._ctx
            success, err, meta = fut.result()

            if success:
                ok_count += 1
                full_s3 = f"s3://{bucket}/{key}"
                ok_rows.append(
                    {
                        "line_index": line_idx,
                        "raw_line": raw_line,
                        "bucket": bucket,
                        "key": key,
                        "s3_uri": full_s3,
                        "dry_run": meta.get("dry_run", False),
                    }
                )
            else:
                fail_count += 1
                error_rows.append(
                    {
                        "line_index": line_idx,
                        "raw_line": raw_line,
                        "bucket": bucket,
                        "key": key,
                        "error": err or "unknown",
                    }
                )

            if done % 500 == 0 or done == total_futures:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                print(f"Progreso: {done}/{total_futures} | OK={ok_count} FAIL={fail_count} | {rate:.2f} obj/s")

    if ok_rows:
        pd.DataFrame(ok_rows).to_csv(args.ok_log, index=False)
        print(f"OK log: {args.ok_log} ({len(ok_rows)} filas)")

    if error_rows:
        pd.DataFrame(error_rows).to_csv(args.error_log, index=False)
        print(f"Error log: {args.error_log} ({len(error_rows)} filas)")

    elapsed = time.time() - start
    delete_failures = fail_count - len(parse_errors)
    print("\n--- RESUMEN ---")
    print(f"Líneas con error de formato/URL: {len(parse_errors)}")
    print(f"Eliminaciones OK: {ok_count}" + (" (simulado, dry-run)" if args.dry_run else ""))
    print(f"Fallos al borrar en S3: {delete_failures}")
    print(f"Tiempo: {elapsed:.1f} s")


if __name__ == "__main__":
    main()
