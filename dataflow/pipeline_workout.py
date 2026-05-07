import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    # Configuración del pipeline
    options = PipelineOptions(
        runner="DataflowRunner",  # Cambiar a DataflowRunner para GCP -- DirectRunner para local
        project="data-engineer-proyect-02",
        region="us-central1",
        temp_location="gs://bucket_shakespeare_pipeline/temp",
        staging_location="gs://bucket_shakespeare_pipeline/staging"
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Leer archivo" >> beam.io.ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
            | "Separar palabras" >> beam.FlatMap(lambda line: line.split())
            | "Contar palabras" >> beam.combiners.Count.PerElement()
            | "Guardar resultados" >> beam.io.WriteToText("gs://bucket_shakespeare_pipeline/output/wordcount")
        )

if __name__ == "__main__":
    run()