{{ config(materialized='table') }}

with agencias as (
    select * from {{ ref('stg_agencias') }}
),

colaboradores as (
    select * from {{ ref('stg_colaboradores') }}
),

colaborador_agencia as (
    select * from {{ ref('stg_colaborador_agencia') }}
)

select
    -- Safe text concatenation key for tracking relationships
    ca.cod_colaborador || '-' || ca.cod_agencia as relationship_id,
    ca.cod_colaborador,
    ca.cod_agencia,
    c.primeiro_nome,
    c.ultimo_nome,
    c.email,
    a.nome_agencia,
    a.cidade_agencia,
    a.uf_agencia,
    a.tipo_agencia,
    now() as transformed_at
from colaborador_agencia ca
inner join colaboradores c on ca.cod_colaborador = c.cod_colaborador
inner join agencias a on ca.cod_agencia = a.cod_agencia
