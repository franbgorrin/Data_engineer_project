import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    options = PipelineOptions(
        runner="DataflowRunner", # Cambiar a DataflowRunner para GCP -- DirectRunner para local
        project="data-engineer-proyect-02",
        region="us-east1",

        # IMPORTANTE
        temp_location="gs://bucket_shakespeare_pi/temp",
        staging_location="gs://bucket_shakespeare_pi/staging",

        # opcional pero recomendable
        job_name="wordcount-job"
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Leer archivo" >> beam.io.ReadFromText(
                "gs://dataflow-samples/shakespeare/kinglear.txt"
            )
            | "Separar palabras" >> beam.FlatMap(lambda line: line.split())
            | "Contar palabras" >> beam.combiners.Count.PerElement()
            | "Guardar resultados" >> beam.io.WriteToText(
                "gs://bucket_shakespeare_pi/output/wordcount"
            )
        )

if __name__ == "__main__":
    run()