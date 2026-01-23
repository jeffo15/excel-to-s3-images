# S3 Image Migration from Excel

A small Python tool that reads an Excel file containing an `Id` column and multiple image columns (e.g. `Imagen Antes`, `Imagen Después`, etc.), downloads images from **public URLs**, and uploads them to a **destination AWS S3 bucket** organized by record ID and column name.

## What it does

- Reads an Excel `.xlsx` file
- Detects all columns that start with `Imagen` (case-insensitive)
- For each row:
  - Uses the value in the `Id` column as the identifier
  - For each image column, downloads the image using its **public URL**
  - Uploads the file to the destination S3 bucket

### Output structure in S3

Files will be uploaded using this key format:


Example:


---

## Requirements

- Python 3.9+ recommended
- AWS credentials configured locally (only required for uploading to the destination bucket)
- Internet access to download public images

---

## Installation

Install Python dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install pandas openpyxl boto3 requests

---

## Test

```bash
python migrar_imagenes_publicas.py \
  --excel reporte_telefonica.xlsx \
  --dst-bucket mi-bucket-destino \
  --dst-prefix imagenes

---
