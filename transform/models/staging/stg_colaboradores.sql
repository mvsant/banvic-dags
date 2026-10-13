{{ config(materialized='view') }}

select
    cod_colaborador,
    primeiro_nome,
    ultimo_nome,
    email
from {{ source('raw_source', 'colaboradores') }}
