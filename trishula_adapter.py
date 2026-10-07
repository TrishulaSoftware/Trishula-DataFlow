from dataflow_engine import PipelineRunner, TextCleanOperator, QualityFilterOperator

def get_standard_pipeline():
    return PipelineRunner([TextCleanOperator(), QualityFilterOperator(min_length=30)])
