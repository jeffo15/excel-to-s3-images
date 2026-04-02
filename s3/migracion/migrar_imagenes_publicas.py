# migrar_imagenes_publicas_parallel_v2.py
# - Lee un Excel con columna "Id" y columnas que empiezan por "Imagen"
# - Descarga desde URLs públicas y sube a S3 destino (streaming, sin guardar a disco)
# - Mantiene el path desde "marketfit/..." (o el valor que pases en --keep-from)
# - Sube con metadata correcta para que el browser RENDERICE y no “descargue”:
#     - ContentType (desde header HTTP o por extensión)
#     - ContentDisposition = inline
#     - CacheControl (opcional, recomendado)
# - Paralelo con threads
# - Guarda LOGS:
#     - ok.csv     (subidas correctas)
#     - errors.csv (fallidas con detalle)
#
# Requisitos:
#   py -m pip install pandas openpyxl boto3 requests
#
# Ejemplo (desde la raíz del repositorio):
#   py s3/migracion/migrar_imagenes_publicas.py --excel telefonica.xlsx --dst-bucket logicsoft-centauro-bucket --keep-from marketfit --workers 16
#   py s3/migracion/migrar_imagenes_publicas.py --excel telefonica.xlsx --dst-bucket logicsoft-centauro-bucket --keep-from marketfit --workers 16 --max 5000
#
# Nota:
#   - Si tu destino tiene versioning o quieres evitar re-subir si ya existe igual, usa --skip-if-exists
#   - Si quieres que quede privado/público explícitamente, usa --acl private|public-read (opcional)

import argparse
import mimetypes
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote

import boto3
import pandas as pd
import requests
from botocore.config import Config
from botocore.exceptions import ClientError


def extract_bucket_and_key_from_public_s3_url(url: str):
    """
    Soporta:
      1) https://s3.amazonaws.com/<bucket>/<key>
      2) https://s3.<region>.amazonaws.com/<bucket>/<key>
      3) https://<bucket>.s3.amazonaws.com/<key>
      4) https://<bucket>.s3.<region>.amazonaws.com/<key>
    Retorna (bucket, key) o (None, None)
    """
    u = urlparse(url)
    host = u.netloc.lower()
    path = unquote(u.path.lstrip("/"))

    if host == "s3.amazonaws.com" or (host.startswith("s3.") and host.endswith(".amazonaws.com")):
        parts = path.split("/", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else (None, None)

    m = re.match(r"^(?P<bucket>[^.]+)\.s3(\.[^.]+)?\.amazonaws\.com$", host)
    if m:
        return m.group("bucket"), path

    return None, None


def build_destination_key(
    original_key: str,
    keep_from: str,
    keep_after: str,
    dst_prefix: str,
    item_id: str,
    force_base_path: str,
):
    """
    Si force_base_path viene informado, arma el destino como:
      <force_base_path>/<item_id>/<filename>

    Si no, mantiene el path desde keep_from hacia la derecha (ej: marketfit/...),
    o desde DESPUES de keep_after (ej: si keep_after="cnt", toma lo que sigue a "cnt/").
    Si dst_prefix no está vacío, lo antepone.
    """
    original_key = original_key.lstrip("/")
    filename = original_key.split("/")[-1]

    if force_base_path:
        base = force_base_path.strip("/")
        return f"{base}/{item_id}/{filename}"

    if keep_after:
        needle_after = keep_after.strip("/") + "/"
        idx = original_key.find(needle_after)
        kept = original_key[idx + len(needle_after) :] if idx >= 0 else original_key
    elif keep_from:
        needle = keep_from.strip("/") + "/"
        idx = original_key.find(needle)
        kept = original_key[idx:] if idx >= 0 else original_key
    else:
        kept = original_key

    return f"{dst_prefix.strip('/')}/{kept}" if dst_prefix else kept


def iter_image_jobs(df, id_col: str, image_columns: list[str]):
    for i, row in df.iterrows():
        item_id = str(row.get(id_col, "")).strip()
        if not item_id or item_id.lower() in {"nan", "none"}:
            continue

        for col in image_columns:
            url = row.get(col)
            if url is None:
                continue
            url = str(url).strip()
            if not url or url.lower() in {"nan", "none"}:
                continue
            yield (i, item_id, col, url)


def normalize_content_type(ct: str | None) -> str | None:
    if not ct:
        return None
    # Quita charset/etc
    base = ct.split(";")[0].strip().lower()
    return base if base else None


def guess_content_type(url: str, http_content_type: str | None) -> str:
    """
    Priorizamos lo que diga el servidor origen. Si no viene, inferimos por extensión.
    """
    ct = normalize_content_type(http_content_type)
    if ct:
        return ct

    guessed, _ = mimetypes.guess_type(url)
    return (guessed or "application/octet-stream").lower()


def head_object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        # otros errores (permisos, etc.) deben romper
        raise


def upload_one_stream_to_s3(
    s3,
    dst_bucket: str,
    dst_key: str,
    url: str,
    *,
    max_retries: int = 5,
    timeout: int = 60,
    cache_control: str | None = "public, max-age=31536000, immutable",
    content_disposition: str = "inline",
    acl: str | None = None,
    skip_if_exists: bool = False,
):
    """
    Descarga por streaming desde URL pública y sube directo a S3 (sin guardar a disco).
    Retorna (True, None, meta_dict) o (False, error_str, meta_dict)
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    last_err = None

    # Si quieres saltarte los que ya existen:
    if skip_if_exists:
        try:
            if head_object_exists(s3, dst_bucket, dst_key):
                return True, None, {"skipped": True}
        except Exception as e:
            # si falla el head por permisos/otro, seguimos intentando subir
            last_err = e

    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
                r.raise_for_status()

                content_type = guess_content_type(url, r.headers.get("Content-Type"))
                # Si el origen manda octet-stream, a veces viene mal; intentamos re-inferir por extensión
                if content_type == "application/octet-stream":
                    ext_guess, _ = mimetypes.guess_type(url)
                    if ext_guess:
                        content_type = ext_guess.lower()

                extra_args = {
                    "ContentType": content_type,
                    "ContentDisposition": content_disposition,  # inline evita “forzar download”
                }
                if cache_control:
                    extra_args["CacheControl"] = cache_control
                if acl:
                    extra_args["ACL"] = acl

                # Streaming directo (requests raw) -> S3
                s3.upload_fileobj(r.raw, dst_bucket, dst_key, ExtraArgs=extra_args)

            return True, None, {
                "skipped": False,
                "content_type": content_type,
                "content_disposition": content_disposition,
                "cache_control": cache_control,
                "acl": acl,
            }

        except Exception as e:
            last_err = e
            time.sleep(min(2 ** (attempt - 1), 20))

    return False, str(last_err), {"skipped": False}


def main():
    parser = argparse.ArgumentParser(
        description="Migración masiva desde URLs públicas a S3 (paralelo + streaming + metadata correcta + logs)"
    )
    parser.add_argument("--excel", required=True, help="Ruta al Excel (.xlsx)")
    parser.add_argument("--dst-bucket", required=True, help="Bucket destino")
    parser.add_argument("--dst-prefix", default="", help="Prefijo destino (default: vacío)")
    parser.add_argument("--id-col", default="Id", help='Columna ID (default: "Id")')
    parser.add_argument(
        "--image-col",
        default="",
        help='Procesar solo esta columna de imagen (ej: "Imagen Antes"). Si va vacío, procesa todas las que empiezan por "Imagen".',
    )
    parser.add_argument("--keep-from", default="marketfit", help='Mantener path desde (default: "marketfit")')
    parser.add_argument(
        "--keep-after",
        default="",
        help='Mantener el path DESPUES de este segmento (ej: "cnt"). Tiene prioridad sobre --keep-from.',
    )
    parser.add_argument(
        "--force-base-path",
        default="/survey/cnt/679a1a40-03ac-44c3-9c5f-5cdbc783d274",
        help='Fuerza destino como "<base>/<Id>/<archivo>" (ej: "/survey/cnt/679..."). Tiene prioridad sobre --keep-after y --keep-from.',
    )
    parser.add_argument("--region", default=None, help="Región AWS (opcional)")
    parser.add_argument("--workers", type=int, default=16, help="Hilos en paralelo (default: 16)")
    parser.add_argument("--max", type=int, default=0, help="Limitar a N imágenes (0 = todas)")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout HTTP (segundos) (default: 60)")
    parser.add_argument("--retries", type=int, default=5, help="Reintentos por imagen (default: 5)")
    parser.add_argument("--ok-log", default="ok.csv", help="Archivo CSV para éxitos (default: ok.csv)")
    parser.add_argument("--error-log", default="errors.csv", help="Archivo CSV para errores (default: errors.csv)")

    # Metadata/headers deseados
    parser.add_argument(
        "--cache-control",
        default="public, max-age=31536000, immutable",
        help='Cache-Control a aplicar (default: "public, max-age=31536000, immutable"). Usa "" para desactivar.',
    )
    parser.add_argument(
        "--content-disposition",
        default="inline",
        help='Content-Disposition (default: "inline").',
    )
    parser.add_argument(
        "--acl",
        default="",
        help='ACL opcional: "private" o "public-read". Si lo dejas vacío, no se setea ACL.',
    )
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Si el objeto ya existe en destino, lo salta (head_object).",
    )

    args = parser.parse_args()

    cache_control = args.cache_control.strip()
    if cache_control == "":
        cache_control = None

    acl = args.acl.strip() or None

    df = pd.read_excel(args.excel, engine="openpyxl")
    if args.id_col not in df.columns:
        raise SystemExit(f'No existe la columna "{args.id_col}". Columnas: {list(df.columns)}')

    requested_image_col = args.image_col.strip()
    if requested_image_col:
        if requested_image_col not in df.columns:
            raise SystemExit(
                f'No existe la columna de imagen "{requested_image_col}". Columnas: {list(df.columns)}'
            )
        image_columns = [requested_image_col]
    else:
        image_columns = [c for c in df.columns if str(c).strip().lower().startswith("imagen")]
        if not image_columns:
            raise SystemExit("No se encontraron columnas que empiecen con 'Imagen'.")

    print("Columnas imagen:", image_columns)
    print("Workers:", args.workers)

    # Config boto3 con retries y pool grande para threads
    boto_cfg = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        max_pool_connections=max(50, args.workers * 2),
    )
    s3_kwargs = {"config": boto_cfg}
    if args.region:
        s3_kwargs["region_name"] = args.region
    s3 = boto3.client("s3", **s3_kwargs)

    jobs = list(iter_image_jobs(df, args.id_col, image_columns))
    if args.max and args.max > 0:
        jobs = jobs[: args.max]

    total = len(jobs)
    print(f"Total imágenes a procesar: {total}")

    ok_count = 0
    fail_count = 0
    skipped_count = 0
    start = time.time()

    ok_rows = []
    error_rows = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = []

        for (row_idx, item_id, col, url) in jobs:
            src_bucket, src_key = extract_bucket_and_key_from_public_s3_url(url)

            if not src_key:
                fail_count += 1
                error_rows.append(
                    {
                        "row": row_idx,
                        "id": item_id,
                        "column": col,
                        "url": url,
                        "dst_key": "",
                        "error": "Could not parse bucket/key from URL",
                    }
                )
                continue

            dst_key = build_destination_key(
                src_key,
                args.keep_from,
                args.keep_after,
                args.dst_prefix,
                item_id,
                args.force_base_path,
            )

            fut = ex.submit(
                upload_one_stream_to_s3,
                s3,
                args.dst_bucket,
                dst_key,
                url,
                max_retries=args.retries,
                timeout=args.timeout,
                cache_control=cache_control,
                content_disposition=args.content_disposition,
                acl=acl,
                skip_if_exists=args.skip_if_exists,
            )
            fut._ctx = (row_idx, item_id, col, url, dst_key)
            futures.append(fut)

        done = 0
        total_futures = len(futures)

        for fut in as_completed(futures):
            done += 1
            row_idx, item_id, col, url, dst_key = fut._ctx

            success, err, meta = fut.result()

            if success:
                ok_count += 1
                was_skipped = bool(meta.get("skipped"))
                if was_skipped:
                    skipped_count += 1

                full_s3_path = f"s3://{args.dst_bucket}/{dst_key}"
                full_http_url = f"https://{args.dst_bucket}.s3.amazonaws.com/{dst_key}"

                ok_rows.append(
                    {
                        "row": row_idx,
                        "id": item_id,
                        "column": col,
                        "url": url,
                        "dst_bucket": args.dst_bucket,
                        "dst_key": dst_key,
                        "dst_s3_path": full_s3_path,
                        "dst_http_url": full_http_url,
                        # Paths que realmente se agregaron en esta corrida.
                        "added_s3_path": "" if was_skipped else full_s3_path,
                        "added_http_url": "" if was_skipped else full_http_url,
                        "skipped": was_skipped,
                        "content_type": meta.get("content_type", ""),
                        "content_disposition": meta.get("content_disposition", ""),
                        "cache_control": meta.get("cache_control", ""),
                        "acl": meta.get("acl", ""),
                    }
                )
            else:
                fail_count += 1
                error_rows.append(
                    {
                        "row": row_idx,
                        "id": item_id,
                        "column": col,
                        "url": url,
                        "dst_key": dst_key,
                        "error": err,
                    }
                )

            if done % 200 == 0 or done == total_futures:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta_h = (total_futures - done) / rate / 3600 if rate > 0 else float("inf")
                print(
                    f"Progreso: {done}/{total_futures} | OK={ok_count} (skipped={skipped_count}) FAIL={fail_count} "
                    f"| {rate:.2f} img/s | ETA ~ {eta_h:.2f} h"
                )

    if ok_rows:
        pd.DataFrame(ok_rows).to_csv(args.ok_log, index=False)
        print(f"✅ OK log guardado en: {args.ok_log} ({len(ok_rows)} filas)")

    if error_rows:
        pd.DataFrame(error_rows).to_csv(args.error_log, index=False)
        print(f"❌ Error log guardado en: {args.error_log} ({len(error_rows)} filas)")

    elapsed = time.time() - start
    print("\n--- RESUMEN ---")
    print(f"OK: {ok_count} (skipped: {skipped_count})")
    print(f"FAIL: {fail_count}")
    print(f"Tiempo: {elapsed/3600:.2f} horas")
    print(f"Velocidad promedio: {(ok_count/elapsed):.2f} img/s")


if __name__ == "__main__":
    # Asegura que mimetypes tenga algunos comunes
    mimetypes.add_type("image/webp", ".webp")
    mimetypes.add_type("image/jpeg", ".jpg")
    mimetypes.add_type("image/jpeg", ".jpeg")
    mimetypes.add_type("image/png", ".png")
    mimetypes.add_type("image/gif", ".gif")

    main()
