{{ config(materialized='view') }}

select
    cod_agencia,
    nome as nome_agencia,
    cidade as cidade_agencia,
    uf as uf_agencia,
    tipo_agencia
from {{ source('raw_source', 'agencias') }}
