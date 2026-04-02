# Herramientas Excel → S3

Scripts en Python para **subir imágenes** desde un Excel (URLs públicas) a un bucket de Amazon S3 y para **eliminar objetos** en S3 a partir de un listado (texto o CSV). El código está agrupado por tipo de operación bajo `s3/`.

## Estructura del repositorio

```
excel-to-s3-images/
├── README.md
├── requirements.txt
├── .env.example          # plantilla de variables (copiar a .env)
├── s3/
│   ├── migracion/        # descarga desde URLs públicas → subida a S3
│   │   ├── migrar_imagenes_publicas.py
│   │   └── generar_plantilla_excel.py
│   └── eliminacion/      # borrado masivo por lista de URLs/keys
│       └── eliminar_imagenes_s3.py
└── ...
```

## Requisitos

- Python 3.9 o superior (recomendado).
- Cuenta AWS con permisos sobre el bucket (`s3:PutObject` para migración, `s3:DeleteObject` para eliminación, según tu caso).
- Acceso a Internet para descargar las imágenes de origen (solo el flujo de migración).

## Instalación

En la raíz del repositorio:

```bash
python -m pip install -r requirements.txt
```

En Windows, si usas el launcher `py`:

```bash
py -m pip install -r requirements.txt
```

## Configuración (AWS)

### Migración (`s3/migracion`)

`boto3` usa la [cadena de credenciales por defecto](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html): variables de entorno, archivo `~/.aws/credentials`, perfil, etc. Configura al menos región y credenciales con acceso de escritura al bucket destino.

### Eliminación (`s3/eliminacion`)

El script de borrado está pensado para leer **cuatro variables** desde un archivo `.env` en la raíz del proyecto (o desde el directorio de trabajo), además de poder usar variables ya exportadas en el sistema:

| Variable | Uso |
|----------|-----|
| `AWS_REGION` | Región del cliente S3 (obligatoria para este script). |
| `AWS_S3_BUCKET_NAME` | Bucket donde se ejecutan los borrados en modo por defecto (`bucket-source env`). |
| `AWS_ACCESS_KEY_ID` | Clave de acceso. |
| `AWS_SECRET_ACCESS_KEY` | Clave secreta. |

1. Copia `.env.example` a `.env`.
2. Rellena los valores reales.
3. No subas `.env` al repositorio (debe estar en `.gitignore`).

Si el `.env` está solo en la raíz del repo, el script lo localiza aunque lo ejecutes desde otra carpeta (busca `.env` en el directorio actual, en la raíz del repositorio y junto al script).

---

## 1. Migración: Excel → S3 (`s3/migracion`)

### Qué hace

- Lee un `.xlsx` con columna `Id` (configurable) y columnas cuyo nombre **empieza por `Imagen`** (o una columna concreta con `--image-col`).
- Por cada fila, descarga la imagen desde la **URL pública** y la sube al bucket destino en streaming (sin guardar archivo intermedio en disco).
- Ajusta metadatos (`Content-Type`, `Content-Disposition: inline`, `Cache-Control` opcional) para que el navegador muestre la imagen en lugar de forzar descarga.
- Puede ejecutar subidas en paralelo (`--workers`).
- Genera logs en CSV: `ok.csv` y `errors.csv` (nombres configurables).

### Ejemplo mínimo

Desde la raíz del repositorio (ajusta rutas y nombres de bucket):

```bash
py s3/migracion/migrar_imagenes_publicas.py ^
  --excel reporte.xlsx ^
  --dst-bucket mi-bucket-destino ^
  --keep-from marketfit ^
  --workers 16
```

En PowerShell puedes usar comillas y saltos sin `^`, o una sola línea.

Parámetros frecuentes (ver `--help` en el script): `--dst-prefix`, `--keep-after`, `--force-base-path`, `--max`, `--skip-if-exists`, `--acl`, `--region`.

### Plantilla de Excel

Para generar un Excel de ejemplo con la estructura esperada:

```bash
py s3/migracion/generar_plantilla_excel.py
```

Se crea `plantilla_migracion_imagenes.xlsx` en el directorio desde el que ejecutes el comando.

---

## 2. Eliminación: listado → borrado en S3 (`s3/eliminacion`)

### Qué hace

- Lee un **archivo de texto** (una URL o key por línea) o un **CSV** con una columna de URLs.
- Resuelve bucket y **object key** desde cada URL (`https://...`, `s3://...`) o usa solo la key si indicas bucket por defecto.
- Por defecto (`--bucket-source env` cuando existe `AWS_S3_BUCKET_NAME`) borra **siempre** en el bucket definido en el `.env`, extrayendo solo la **key** de cada URL (alineado con credenciales y bucket del entorno).
- Con `--bucket-source url` se usa el bucket que aparece en cada URL (comportamiento alternativo).
- Opción `--dry-run`: no llama a `delete_object`, solo valida y genera logs.
- Salidas típicas: `delete_ok.csv` y `delete_errors.csv`.

### Ejemplo con CSV

```bash
py s3/eliminacion/eliminar_imagenes_s3.py --format csv --paths-file urls.csv --dry-run
```

Borrado real (sin `--dry-run`):

```bash
py s3/eliminacion/eliminar_imagenes_s3.py --format csv --paths-file urls.csv
```

Revisa `py s3/eliminacion/eliminar_imagenes_s3.py --help` para `--url-col`, `--encoding`, `--workers`, `--bucket-source`, etc.

### Notas

- **CDN / CloudFront**: borrar en S3 no invalida caché de CloudFront; si las URLs pasan por CDN, puede hacer falta una invalidación aparte.
- **Caché del navegador**: tras borrar en S3, prueba en ventana privada si la URL directa al bucket sigue mostrando algo antiguo.

---

## Seguridad

- No commitees `.env` ni claves AWS.
- Rota las claves si se han expuesto.
- Usa políticas IAM con el mínimo permiso necesario por bucket y prefijo.

---

## Licencia / uso interno

Ajusta esta sección según la política de tu organización.
