import argparse
import os
import re
from urllib.parse import urlparse

import boto3
import pandas as pd
import requests


def safe_name(text: str) -> str:
    """Convierte texto a nombre seguro para rutas/keys (sin espacios/acentos/raros)."""
    s = str(text).strip().lower()
    s = re.sub(r"[^\w.-]+", "_", s, flags=re.UNICODE)  # mantiene a-zA-Z0-9_ . -
    s = s.strip("_")
    return s or "na"


def download_image(url: str, output_path: str) -> None:
    """Descarga por HTTP(S) a disco."""
    headers = {"User-Agent": "Mozilla/5.0"}  # ayuda en algunos endpoints
    with requests.get(url, stream=True, timeout=60, headers=headers) as r:
        r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def main():
    parser = argparse.ArgumentParser(description="Migración de imágenes desde URLs públicas a S3")
    parser.add_argument("--excel", required=True, help="Ruta al archivo Excel (.xlsx)")
    parser.add_argument("--dst-bucket", required=True, help="Bucket S3 destino")
    parser.add_argument("--dst-prefix", default="imagenes", help="Prefijo en bucket destino (default: imagenes)")
    parser.add_argument("--id-col", default="Id", help='Nombre de la columna ID (default: "Id")')
    parser.add_argument("--tmp-dir", default="tmp_downloads", help="Carpeta temporal para descargas (default: tmp_downloads)")
    parser.add_argument("--region", default=None, help="Región AWS (opcional)")
    args = parser.parse_args()

    # Leer Excel
    df = pd.read_excel(args.excel, engine="openpyxl")

    if args.id_col not in df.columns:
        raise SystemExit(f'❌ No existe la columna ID "{args.id_col}". Columnas encontradas: {list(df.columns)}')

    # Detectar columnas que empiezan por "Imagen"
    image_columns = [c for c in df.columns if str(c).strip().lower().startswith("imagen")]
    if not image_columns:
        raise SystemExit("❌ No se encontraron columnas que empiecen con 'Imagen' en el Excel.")

    print(f"📸 Columnas de imágenes detectadas: {image_columns}")

    # Cliente S3 (para subir SÍ necesitas credenciales configuradas en tu máquina)
    s3_kwargs = {}
    if args.region:
        s3_kwargs["region_name"] = args.region
    s3 = boto3.client("s3", **s3_kwargs)

    os.makedirs(args.tmp_dir, exist_ok=True)

    ok = fail = skip = 0

    for i, row in df.iterrows():
        item_id = str(row.get(args.id_col, "")).strip()
        if not item_id or item_id.lower() in {"nan", "none"}:
            continue

        for col in image_columns:
            url = row.get(col)

            if url is None:
                skip += 1
                continue

            url = str(url).strip()
            if not url or url.lower() in {"nan", "none"}:
                skip += 1
                continue

            try:
                # Nombre de archivo desde URL
                parsed = urlparse(url)
                filename = os.path.basename(parsed.path) or f"{safe_name(col)}.jpg"

                # Rutas locales
                local_dir = os.path.join(args.tmp_dir, safe_name(item_id), safe_name(col))
                os.makedirs(local_dir, exist_ok=True)
                local_file = os.path.join(local_dir, filename)

                # 1) Descargar desde URL pública
                download_image(url, local_file)

                # 2) Subir al bucket destino
                s3_key = f"{args.dst_prefix}/{safe_name(item_id)}/{safe_name(col)}/{filename}"
                s3.upload_file(local_file, args.dst_bucket, s3_key)

                print(f"[OK] {item_id} | {col} → s3://{args.dst_bucket}/{s3_key}")
                ok += 1

            except Exception as e:
                print(f"[ERROR] fila={i} id={item_id} col={col} url={url} | {e}")
                fail += 1

    print("\n--- RESUMEN FINAL ---")
    print(f"✔ Subidas OK: {ok}")
    print(f"➖ Celdas vacías/invalidas ignoradas: {skip}")
    print(f"❌ Errores: {fail}")


if __name__ == "__main__":
    main()