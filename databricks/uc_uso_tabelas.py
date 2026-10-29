# Databricks notebook source
# MAGIC %md
# MAGIC # Consumo das tabelas documentadas — quem mais usa cada tabela
# MAGIC Cruza a tabela gerada pelo `uc_auto_doc_revisao` (widget `tabela_doc`) com as system tables de lineage
# MAGIC (`system.access.table_lineage` e `system.access.column_lineage`) e grava três tabelas:
# MAGIC
# MAGIC | Tabela | Granularidade | Para que serve |
# MAGIC |---|---|---|
# MAGIC | `<tabela_destino>` | 1 linha por tabela × usuário | ranking de quem mais lê/escreve cada tabela |
# MAGIC | `<tabela_destino>_resumo` | 1 linha por tabela | total de usuários, top 3, situação (sem uso, só escrita, usuário único...) |
# MAGIC | `<tabela_destino>_colunas` | 1 linha por coluna | colunas mais lidas e colunas sem leitura registrada |
# MAGIC
# MAGIC Notas:
# MAGIC - Lineage registra leituras de notebooks, jobs, pipelines, dashboards e do SQL editor (sem `entity_type` = ad-hoc).
# MAGIC   Leituras fora do Unity Catalog (ex.: arquivos direto no storage) não aparecem.
# MAGIC - `column_lineage` só registra colunas que fluem para um destino (tabela/view/dashboard): "sem leitura" numa coluna
# MAGIC   é um indício, não uma certeza.
# MAGIC - As tabelas são recriadas a cada execução (foto da janela de `dias`). **Use sempre "Run all"**.

# COMMAND ----------

# Limpa widgets antigos (precisa ficar em célula separada da que os recria)
dbutils.widgets.removeAll()

# COMMAND ----------

# ---------------- Parâmetros ----------------
dbutils.widgets.text("tabela_doc", "data_science_dev.churn_models.uc_doc_revisao", "Tabela gerada pelo uc_auto_doc_revisao")
dbutils.widgets.text("tabela_destino", "data_science_dev.churn_models.uc_uso_tabelas", "Tabela destino (catalogo.schema.tabela)")
dbutils.widgets.text("dias", "90", "Janela de análise (dias)")
dbutils.widgets.text("top_n", "5", "Usuários por tabela nas visões (ranking)")
dbutils.widgets.dropdown("incluir_service_principals", "true", ["true", "false"], "Incluir service principals (jobs)")

DOC      = dbutils.widgets.get("tabela_doc").strip()
DESTINO  = dbutils.widgets.get("tabela_destino").strip()
assert DESTINO.count(".") == 2, f"tabela_destino deve ser catalogo.schema.tabela (recebido: '{DESTINO}')"
DIAS     = int(dbutils.widgets.get("dias"))
TOP_N    = int(dbutils.widgets.get("top_n"))
INCLUIR_SP = dbutils.widgets.get("incluir_service_principals") == "true"
DESTINO_RESUMO, DESTINO_COLUNAS = f"{DESTINO}_resumo", f"{DESTINO}_colunas"

# service principals aparecem em created_by pelo application id (sem @)
FILTRO_USUARIO = "" if INCLUIR_SP else "AND created_by LIKE '%@%'"
print(f"doc={DOC} | destino={DESTINO} (+ _resumo, _colunas) | dias={DIAS} | service_principals={INCLUIR_SP}")

# COMMAND ----------

# ---------------- Identificador de cada acesso ----------------
# Uma consulta pode gerar várias linhas de lineage (uma por par origem→destino); contamos consultas distintas.
# statement_id é a melhor chave, mas só existe nas versões mais novas das system tables.
def id_evento(tabela_sistema: str) -> str:
    cols = set(spark.table(tabela_sistema).columns)
    for c in ("statement_id", "event_id", "record_id"):
        if c in cols:
            # statement_id vem nulo em alguns tipos de entidade: cai para o id do registro
            return f"coalesce({c}, {'record_id' if 'record_id' in cols else 'cast(event_time AS STRING)'})"
    return "cast(event_time AS STRING)"

ID_TABELA, ID_COLUNA = id_evento("system.access.table_lineage"), id_evento("system.access.column_lineage")
print("Chave de acesso:", ID_TABELA, "|", ID_COLUNA)

# COMMAND ----------

# ---------------- Nomes de usuários e service principals ----------------
# created_by traz e-mail (usuário) ou application id (service principal); buscamos o nome de exibição.
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
nomes = []
try:
    nomes += [(sp.application_id, sp.display_name) for sp in w.service_principals.list() if sp.application_id]
except Exception as e:
    print(f"⚠ sem acesso à lista de service principals ({e.__class__.__name__}); fica o application id")
try:
    nomes += [(u.user_name, u.display_name) for u in w.users.list(attributes="userName,displayName") if u.user_name]
except Exception as e:
    print(f"⚠ sem acesso à lista de usuários ({e.__class__.__name__}); fica o e-mail")
spark.createDataFrame(nomes, "id STRING, nome STRING").dropDuplicates(["id"]).createOrReplaceTempView("_nomes")
print(f"{len(nomes)} nomes carregados")

# COMMAND ----------

# ---------------- Tabelas documentadas e eventos de uso ----------------
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _documentadas AS
    SELECT lower(d.caminho_tabela) AS tabela, d.catalogo, d.`schema`, d.tipo_objeto,
           d.descricao_tabela, t.table_owner AS dono_tabela
    FROM (SELECT caminho_tabela, first(catalogo) AS catalogo, first(`schema`) AS `schema`,
                 first(tipo_objeto) AS tipo_objeto, first(descricao_tabela, true) AS descricao_tabela
          FROM {DOC} GROUP BY caminho_tabela) d
    LEFT JOIN system.information_schema.tables t
      ON lower(concat_ws('.', t.table_catalog, t.table_schema, t.table_name)) = lower(d.caminho_tabela)
""")

# Leitura = tabela aparece como origem; escrita = tabela aparece como destino
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _eventos AS
    SELECT lower(source_table_full_name) AS tabela, created_by AS usuario, 'leitura' AS operacao,
           {ID_TABELA} AS id_evento, event_date, event_time, coalesce(entity_type, 'SQL/ad-hoc') AS origem
    FROM system.access.table_lineage
    WHERE event_date >= current_date() - INTERVAL {DIAS} DAYS
      AND source_table_full_name IS NOT NULL {FILTRO_USUARIO}
    UNION ALL
    SELECT lower(target_table_full_name), created_by, 'escrita',
           {ID_TABELA}, event_date, event_time, coalesce(entity_type, 'SQL/ad-hoc')
    FROM system.access.table_lineage
    WHERE event_date >= current_date() - INTERVAL {DIAS} DAYS
      AND target_table_full_name IS NOT NULL {FILTRO_USUARIO}
""")

# COMMAND ----------

# ---------------- 1) Uso por tabela × usuário ----------------
spark.sql(f"""
    CREATE OR REPLACE TABLE {DESTINO}
    COMMENT 'Uso das tabelas documentadas por usuário (system.access.table_lineage), recriada a cada execução'
    AS
    WITH uso AS (
      SELECT e.tabela, e.usuario,
             count(DISTINCT CASE WHEN e.operacao = 'leitura' THEN e.id_evento END) AS leituras,
             count(DISTINCT CASE WHEN e.operacao = 'escrita' THEN e.id_evento END) AS escritas,
             count(DISTINCT e.event_date)                                       AS dias_com_uso,
             min(e.event_time)                                                  AS primeiro_acesso,
             max(e.event_time)                                                  AS ultimo_acesso,
             array_join(array_sort(collect_set(e.origem)), ', ')                AS origens
      FROM _eventos e
      WHERE e.tabela IN (SELECT tabela FROM _documentadas)
      GROUP BY e.tabela, e.usuario
    )
    SELECT d.catalogo, d.`schema`, d.tabela AS caminho_tabela, d.tipo_objeto, d.descricao_tabela, d.dono_tabela,
           u.usuario, coalesce(n.nome, u.usuario) AS nome_usuario,
           CASE WHEN u.usuario LIKE '%@%' THEN 'usuario' ELSE 'service_principal' END AS tipo_usuario,
           u.leituras, u.escritas, u.dias_com_uso, u.primeiro_acesso, u.ultimo_acesso, u.origens,
           round(100 * u.leituras / nullif(sum(u.leituras) OVER (PARTITION BY d.tabela), 0), 1) AS pct_leituras_tabela,
           row_number() OVER (PARTITION BY d.tabela
                              ORDER BY u.leituras DESC, u.dias_com_uso DESC, u.escritas DESC) AS ranking,
           {DIAS} AS janela_dias, current_timestamp() AS atualizado_em
    FROM _documentadas d
    JOIN uso u ON u.tabela = d.tabela
    LEFT JOIN _nomes n ON n.id = u.usuario
""")
print(f"✓ {DESTINO}: {spark.table(DESTINO).count():,} linhas (tabela × usuário)")

# COMMAND ----------

# ---------------- 2) Resumo por tabela (inclui as tabelas sem nenhum uso) ----------------
spark.sql(f"""
    CREATE OR REPLACE TABLE {DESTINO_RESUMO}
    COMMENT 'Resumo de uso por tabela documentada: usuários, top 3 e situação'
    AS
    SELECT d.catalogo, d.`schema`, d.tabela AS caminho_tabela, d.tipo_objeto, d.descricao_tabela, d.dono_tabela,
           count_if(u.leituras > 0)            AS leitores_distintos,
           count_if(u.escritas > 0)            AS escritores_distintos,
           coalesce(sum(u.leituras), 0)        AS leituras,
           coalesce(sum(u.escritas), 0)        AS escritas,
           max(u.dias_com_uso)                 AS max_dias_com_uso,
           max(u.ultimo_acesso)                AS ultimo_acesso,
           -- array_sort ordena pelo 1º campo do struct (ranking)
           array_join(transform(slice(array_sort(collect_list(
               CASE WHEN u.leituras > 0 THEN struct(u.ranking, u.nome_usuario) END)), 1, 3),
               x -> x.nome_usuario), ', ')     AS top3_usuarios,
           CASE WHEN coalesce(sum(u.leituras), 0) = 0 AND coalesce(sum(u.escritas), 0) = 0
                     THEN 'sem uso: candidata a descontinuar'
                WHEN coalesce(sum(u.leituras), 0) = 0
                     THEN 'só escrita: alimentada mas ninguém lê'
                WHEN count_if(u.leituras > 0) = 1
                     THEN 'um único leitor: dependência de 1 usuário/job'
                ELSE 'em uso' END              AS situacao,
           {DIAS} AS janela_dias, current_timestamp() AS atualizado_em
    FROM _documentadas d
    LEFT JOIN {DESTINO} u ON u.caminho_tabela = d.tabela
    GROUP BY ALL
""")
print(f"✓ {DESTINO_RESUMO}: {spark.table(DESTINO_RESUMO).count():,} tabelas")

# COMMAND ----------

# ---------------- 3) Uso por coluna (system.access.column_lineage) ----------------
spark.sql(f"""
    CREATE OR REPLACE TABLE {DESTINO_COLUNAS}
    COMMENT 'Leituras por coluna das tabelas documentadas (system.access.column_lineage)'
    AS
    WITH uso AS (
      SELECT lower(source_table_full_name) AS tabela, lower(source_column_name) AS coluna,
             count(DISTINCT {ID_COLUNA}) AS leituras, count(DISTINCT created_by) AS usuarios_distintos,
             max(event_time) AS ultimo_acesso
      FROM system.access.column_lineage
      WHERE event_date >= current_date() - INTERVAL {DIAS} DAYS
        AND source_table_full_name IS NOT NULL {FILTRO_USUARIO}
      GROUP BY 1, 2
    )
    SELECT d.catalogo, d.`schema`, lower(d.caminho_tabela) AS caminho_tabela, d.posicao_coluna, d.coluna,
           d.tipo_coluna, coalesce(d.descricao_corrigida, d.descricao_coluna) AS descricao_coluna,
           coalesce(u.leituras, 0) AS leituras, coalesce(u.usuarios_distintos, 0) AS usuarios_distintos,
           u.ultimo_acesso,
           dense_rank() OVER (PARTITION BY d.caminho_tabela ORDER BY coalesce(u.leituras, 0) DESC) AS ranking_na_tabela,
           {DIAS} AS janela_dias, current_timestamp() AS atualizado_em
    FROM {DOC} d
    LEFT JOIN uso u ON u.tabela = lower(d.caminho_tabela) AND u.coluna = lower(d.coluna)
""")
print(f"✓ {DESTINO_COLUNAS}: {spark.table(DESTINO_COLUNAS).count():,} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visões

# COMMAND ----------

# Top N usuários por tabela
display(spark.sql(f"""
    SELECT catalogo, `schema`, caminho_tabela, dono_tabela, ranking, nome_usuario, tipo_usuario,
           leituras, escritas, pct_leituras_tabela, dias_com_uso, ultimo_acesso, origens
    FROM {DESTINO}
    WHERE ranking <= {TOP_N}
    ORDER BY catalogo, caminho_tabela, ranking
"""))

# COMMAND ----------

# Situação de cada tabela: as sem uso / só escrita aparecem primeiro
display(spark.sql(f"""
    SELECT situacao, catalogo, `schema`, caminho_tabela, dono_tabela, leitores_distintos, leituras, escritas,
           top3_usuarios, ultimo_acesso, descricao_tabela
    FROM {DESTINO_RESUMO}
    ORDER BY CASE situacao WHEN 'em uso' THEN 1 ELSE 0 END, leituras DESC, caminho_tabela
"""))

# COMMAND ----------

# Maiores consumidores: quantas tabelas documentadas cada usuário lê
display(spark.sql(f"""
    SELECT nome_usuario, tipo_usuario,
           count(DISTINCT caminho_tabela)                               AS tabelas_lidas,
           sum(leituras)                                                AS leituras,
           count_if(ranking = 1)                                        AS tabelas_em_que_e_top1,
           array_join(slice(array_sort(collect_list(caminho_tabela)), 1, 10), ', ') AS exemplos
    FROM {DESTINO}
    WHERE leituras > 0
    GROUP BY ALL
    ORDER BY leituras DESC
"""))

# COMMAND ----------

# Situação por catálogo
display(spark.sql(f"""
    SELECT catalogo, count(*) AS tabelas,
           count_if(situacao = 'em uso')                                    AS em_uso,
           count_if(situacao LIKE 'um único leitor%')                       AS leitor_unico,
           count_if(situacao LIKE 'só escrita%')                            AS so_escrita,
           count_if(situacao LIKE 'sem uso%')                               AS sem_uso,
           round(100 * count_if(situacao LIKE 'sem uso%') / count(*), 1)    AS pct_sem_uso
    FROM {DESTINO_RESUMO}
    GROUP BY catalogo ORDER BY sem_uso DESC
"""))

# COMMAND ----------

# Colunas mais lidas e colunas sem leitura nas tabelas mais usadas
display(spark.sql(f"""
    SELECT c.caminho_tabela, c.coluna, c.tipo_coluna, c.leituras, c.usuarios_distintos, c.ranking_na_tabela,
           c.descricao_coluna
    FROM {DESTINO_COLUNAS} c
    JOIN {DESTINO_RESUMO} r USING (caminho_tabela)
    WHERE r.leituras > 0
    ORDER BY r.leituras DESC, c.caminho_tabela, c.leituras DESC, c.posicao_coluna
"""))
