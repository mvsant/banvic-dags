{{ config(materialized='view') }}

select
    cod_colaborador,
    cod_agencia
from {{ source('raw_source', 'colaborador_agencia') }}
