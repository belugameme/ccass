from kedro.pipeline import Pipeline, node
from .nodes import get_stock_list, get_stocks_participants_spark_concurrent, get_stocks_participants_concurrent, transform_participants

def create_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                func=get_stock_list,
                inputs=[
                "params:stock_list.request.url",
                "params:stock_list.request.params",
                "params:stock_list.request.headers",
                "params:stock_list.response.rename_mapper"
                ],
                outputs="stock_list_psqltable",
                name='get_stock_list',
            ),
            node(
                func=get_stocks_participants_spark_concurrent,
                inputs=[
                "stock_list_s3",
                "params:stock_participants.request.url",
                "params:stock_participants.request.data",
                "params:stock_participants.request.headers",
                "params:stock_participants.response.column_search",
                "params:scheduler.current_date",
                "params:scheduler.min_date",
                "params:scheduler.max_date",
                "params:modular_pipeline.start",
                "params:modular_pipeline.end"
                ],
                outputs="stock_participants_spark",
                name='get_stocks_participants_spark_concurrent',
            ),
            node(
                func=get_stocks_participants_concurrent,
                inputs=[
                "stock_list_psqltable",
                "params:stock_participants.request.url",
                "params:stock_participants.request.data",
                "params:stock_participants.request.headers",
                "params:stock_participants.response.column_search",
                "params:scheduler.current_date",
                "params:scheduler.min_date",
                "params:scheduler.max_date",
                "params:modular_pipeline.start",
                "params:modular_pipeline.end"
                ],
                outputs="stock_participants_psqltable",
                name='get_stocks_participants_psqltable_concurrent',
            ),
            node(
                func=transform_participants,
                inputs="stock_participants_psqltable",
                outputs="stock_participants_diff",
                name='get_stocks_participants_diff',
            ),
        ]
    )