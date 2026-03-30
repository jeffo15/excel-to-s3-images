import pandas as pd


def main():
    data = [
        {
            "Id": "12345",
            "Imagen Antes": "https://mi-bucket.s3.amazonaws.com/marketfit/12345/antes.jpg",
        },
        {
            "Id": "",
            "Imagen Antes": "",
        },
    ]

    df = pd.DataFrame(data)
    output = "plantilla_migracion_imagenes.xlsx"
    df.to_excel(output, index=False, engine="openpyxl")
    print(f"Plantilla creada: {output}")


if __name__ == "__main__":
    main()
