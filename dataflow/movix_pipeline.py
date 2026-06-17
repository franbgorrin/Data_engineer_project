import csv
import io
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Ejemplo de función para procesar cada línea del CSV
def procesar_linea_csv(linea):
    # Usamos el módulo csv de Python para parsear la línea correctamente
    # (esto maneja comas dentro de comillas si las hay)
    lector = csv.reader([linea])
    for fila in lector:
        return fila  # Devuelve una lista con los valores de las columnas de esa fila

def run():
    options = PipelineOptions(
        runner="DataflowRunner",  # Cambia a DirectRunner si pruebas en local
        project="proyecto-rrss-479910",
        region="us-east1",
        temp_location="gs://movix/temp",
        staging_location="gs://movix/staging",
        job_name="procesar-csv-job"
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            # 1. Leer el archivo CSV desde tu bucket (subido previamente)
            | "Leer CSV" >> beam.io.ReadFromText("gs://movix/Movix_data.csv")
            
            # 2. Opcional: Saltar la cabecera si tu CSV tiene nombres de columnas en la primera línea
            # | "Filtrar cabecera" >> beam.Filter(lambda linea: not linea.startswith("Columna1,Columna2"))
            
            # 3. Procesar las líneas (convertir cada línea en una lista o tupla de elementos)
            | "Parsear columnas" >> beam.Map(procesar_linea_csv)
            
            # 4. Aquí puedes aplicar tus transformaciones (Map, Filter, etc.)
            # Por ejemplo, filtrar filas donde la columna 0 cumpla cierta condición:
            # | "Filtrar filas" >> beam.Filter(lambda fila: fila[0] != "")

            # 5. Guardar el resultado en tu bucket
            | "Guardar resultados" >> beam.io.WriteToText(
                "gs://bucket_shakespeare_pi/output/resultado_csv"
            )
        )

if __name__ == "__main__":
    run()