# Databricks notebook source
# MAGIC %md
# MAGIC # Duplicidades no catálogo — tabelas, colunas e regras de negócio repetidas
# MAGIC Usa a documentação gerada pelo `uc_auto_doc_revisao` (widget `tabela_doc`) para encontrar o que está repetido.
# MAGIC Não altera nada no Unity Catalog; grava tabelas com o prefixo do widget `prefixo_destino`:
# MAGIC
# MAGIC | Tabela | O que mostra | Como é detectado |
# MAGIC |---|---|---|
# MAGIC | `<prefixo>_tabelas` | pares de tabelas parecidas | mesmo nome em outro schema/catálogo, colunas em comum (Jaccard), descrição semelhante (embeddings) e lineage entre elas |
# MAGIC | `<prefixo>_colunas` | o mesmo conceito em várias tabelas | embeddings das descrições de colunas agrupados por similaridade; aponta nomes/tipos diferentes para o mesmo conceito |
# MAGIC | `<prefixo>_colunas_homonimas` | o mesmo nome com significados diferentes | nome de coluna que cai em mais de um grupo de conceito |
# MAGIC | `<prefixo>_regras` | regras de negócio repetidas ou divergentes | LLM extrai as regras do SQL das views e das cargas (`system.query.history`); embeddings agrupam regras equivalentes |
# MAGIC
# MAGIC Tabelas auxiliares (incrementais, não precisam ser apagadas): `<prefixo>_embeddings` (cache de embeddings)
# MAGIC e `<prefixo>_regras_extraidas` (regras por objeto; só reprocessa quando o SQL muda).
# MAGIC
# MAGIC Se a tabela do notebook `uc_uso_tabelas` existir (widget `tabela_uso_resumo`), os pares de tabelas indicam qual é a mais usada.
# MAGIC **Use sempre "Run all"**.

# COMMAND ----------

# Limpa widgets antigos (precisa ficar em célula separada da que os recria)
dbutils.widgets.removeAll()

# COMMAND ----------

# ---------------- Parâmetros ----------------
dbutils.widgets.text("tabela_doc", "data_science_dev.churn_models.uc_doc_revisao", "Tabela gerada pelo uc_auto_doc_revisao")
dbutils.widgets.text("prefixo_destino", "data_science_dev.churn_models.uc_dup", "Prefixo das tabelas destino (catalogo.schema.prefixo)")
dbutils.widgets.text("tabela_uso_resumo", "data_science_dev.churn_models.uc_uso_tabelas_resumo", "Resumo do uc_uso_tabelas (opcional)")
dbutils.widgets.text("modelo_embedding", "databricks-gte-large-en", "Serving endpoint de embeddings")
dbutils.widgets.text("modelo_endpoint", "databricks-claude-sonnet-4-5", "Serving endpoint do LLM (regras)")
dbutils.widgets.text("idioma", "português do Brasil", "Idioma das regras extraídas")
dbutils.widgets.text("limiar_jaccard", "0.8", "Jaccard mínimo de colunas (tabelas)")
dbutils.widgets.text("limiar_sim_tabela", "0.90", "Similaridade mínima de descrição (tabelas)")
dbutils.widgets.text("limiar_sim_coluna", "0.92", "Similaridade mínima de descrição (colunas)")
dbutils.widgets.text("limiar_sim_regra", "0.90", "Similaridade mínima entre regras")
dbutils.widgets.text("colunas_ignorar", "^_", "Regex de colunas técnicas a ignorar (vazio = nenhuma)")
dbutils.widgets.dropdown("extrair_regras", "true", ["true", "false"], "Extrair regras de negócio com LLM")
dbutils.widgets.dropdown("incluir_sql_carga", "true", ["true", "false"], "Regras também do SQL de carga (query.history)")
dbutils.widgets.dropdown("reprocessar_regras", "false", ["true", "false"], "Reextrair regras já extraídas")
dbutils.widgets.text("max_objetos_regras", "300", "Máx objetos novos para extrair regras (0 = todos)")
dbutils.widgets.text("preco_input_1m", "3.00", "US$ por 1M tokens de entrada")
dbutils.widgets.text("preco_output_1m", "15.00", "US$ por 1M tokens de saída")

DOC            = dbutils.widgets.get("tabela_doc").strip()
PREFIXO        = dbutils.widgets.get("prefixo_destino").strip()
assert PREFIXO.count(".") == 2, f"prefixo_destino deve ser catalogo.schema.prefixo (recebido: '{PREFIXO}')"
TABELA_USO     = dbutils.widgets.get("tabela_uso_resumo").strip()
MODELO_EMB     = dbutils.widgets.get("modelo_embedding").strip()
MODELO         = dbutils.widgets.get("modelo_endpoint").strip()
IDIOMA         = dbutils.widgets.get("idioma")
LIMIAR_JACCARD = float(dbutils.widgets.get("limiar_jaccard"))
LIMIAR_TAB     = float(dbutils.widgets.get("limiar_sim_tabela"))
LIMIAR_COL     = float(dbutils.widgets.get("limiar_sim_coluna"))
LIMIAR_REGRA   = float(dbutils.widgets.get("limiar_sim_regra"))
COLS_IGNORAR   = dbutils.widgets.get("colunas_ignorar").strip()
EXTRAIR_REGRAS = dbutils.widgets.get("extrair_regras") == "true"
INCLUIR_CARGA  = dbutils.widgets.get("incluir_sql_carga") == "true"
REPROCESSAR    = dbutils.widgets.get("reprocessar_regras") == "true"
MAX_OBJ_REGRAS = int(dbutils.widgets.get("max_objetos_regras"))
PRECO_INPUT_1M  = float(dbutils.widgets.get("preco_input_1m"))
PRECO_OUTPUT_1M = float(dbutils.widgets.get("preco_output_1m"))

CACHE_EMB, REGRAS_EXTRAIDAS = f"{PREFIXO}_embeddings", f"{PREFIXO}_regras_extraidas"
MAX_COLS_FREQUENTES = 200     # colunas presentes em mais tabelas que isso não geram candidatos (ex.: id, dt_carga)
VIZINHOS            = 10      # cada item só é comparado com seus 10 vizinhos mais próximos (evita grupos gigantes)
MAX_CHARS_SQL       = 15000   # trunca SQL longo enviado ao LLM
print(f"doc={DOC} | prefixo={PREFIXO} | embeddings={MODELO_EMB} | llm={MODELO} | extrair_regras={EXTRAIR_REGRAS}")

# COMMAND ----------

# ---------------- Utilitários ----------------
import json, re, hashlib, datetime, uuid
import numpy as np

ID_EXECUCAO = str(uuid.uuid4())
USO = {"chamadas": 0, "entrada": 0, "saida": 0}   # tokens estimados (~4 caracteres por token)

def lit(valor: str) -> str:
    """Literal de string SQL."""
    return "'" + valor.replace("\\", "\\\\").replace("'", "\\'") + "'"

def custo_usd(entrada, saida):
    return entrada / 1_000_000 * PRECO_INPUT_1M + saida / 1_000_000 * PRECO_OUTPUT_1M

def ler_json(texto: str | None) -> dict | None:
    if not texto:
        return None
    m = re.search(r"\{.*\}", texto, flags=re.S)   # ignora ```json e texto em volta
    try:
        r = json.loads(m.group(0)) if m else None
    except json.JSONDecodeError:
        return None
    return r if isinstance(r, dict) else None

def hash_texto(texto: str) -> str:
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()

def tabela_existe(nome: str) -> bool:
    try:
        return spark.catalog.tableExists(nome)
    except Exception:
        return False

# COMMAND ----------

# ---------------- Embeddings (ai_query nos executors, com cache) ----------------
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CACHE_EMB} (
    hash STRING, modelo STRING, texto STRING, emb ARRAY<FLOAT>, gerado_em TIMESTAMP)
    COMMENT 'Cache de embeddings usado pelo notebook uc_duplicidades'""")

def embeddings(textos: list[str]) -> dict[str, np.ndarray]:
    """Embedding normalizado de cada texto distinto; só chama o endpoint para textos fora do cache."""
    distintos = sorted({t for t in textos if t})
    if not distintos:
        return {}
    (spark.createDataFrame([(hash_texto(t), t) for t in distintos], "hash STRING, texto STRING")
          .createOrReplaceTempView("_textos"))
    novos = spark.sql(f"""SELECT t.* FROM _textos t LEFT ANTI JOIN {CACHE_EMB} c
                          ON c.hash = t.hash AND c.modelo = {lit(MODELO_EMB)}""")
    n_novos = novos.count()
    if n_novos:
        print(f"   🧮 calculando {n_novos:,} embeddings novos ({len(distintos) - n_novos:,} já no cache)")
        (novos.repartition(max(1, min(n_novos // 200 + 1, 64)))
              .selectExpr("hash", f"{lit(MODELO_EMB)} AS modelo", "texto",
                          f"ai_query({lit(MODELO_EMB)}, texto, failOnError => false) AS r")
              .selectExpr("hash", "modelo", "texto", "r.result AS emb", "current_timestamp() AS gerado_em")
              .where("emb IS NOT NULL")
              .write.mode("append").saveAsTable(CACHE_EMB))
    pdf = spark.sql(f"""SELECT t.texto, c.emb FROM _textos t
                        JOIN (SELECT hash, first(emb) AS emb FROM {CACHE_EMB}
                              WHERE modelo = {lit(MODELO_EMB)} GROUP BY hash) c USING (hash)""").toPandas()
    if len(pdf) < len(distintos):
        print(f"   ⚠ {len(distintos) - len(pdf):,} textos sem embedding (erro no endpoint); ficam de fora")
    vet = {t: np.asarray(e, dtype=np.float32) for t, e in zip(pdf.texto, pdf.emb)}
    return {t: v / (np.linalg.norm(v) + 1e-9) for t, v in vet.items()}

def pares_similares(M: np.ndarray, limiar: float) -> list[tuple[int, int, float]]:
    """Pares (i, j, similaridade) com i < j, entre os VIZINHOS mais próximos de cada linha, acima do limiar."""
    if len(M) < 2:
        return []
    k, pares = min(VIZINHOS + 1, len(M)), {}
    for ini in range(0, len(M), 2048):   # em blocos: não monta a matriz N×N inteira
        S = M[ini:ini + 2048] @ M.T
        viz = np.argpartition(-S, k - 1, axis=1)[:, :k]
        sims = np.take_along_axis(S, viz, axis=1)
        linhas = np.repeat(np.arange(ini, ini + len(S))[:, None], k, axis=1)
        ok = (sims >= limiar) & (viz != linhas)
        for i, j, s in zip(linhas[ok].tolist(), viz[ok].tolist(), sims[ok].tolist()):   # int/float nativos p/ o Spark
            par = (min(i, j), max(i, j))
            pares[par] = max(pares.get(par, 0.0), float(s))
    return [(i, j, s) for (i, j), s in pares.items()]

def agrupar(n: int, pares) -> list[int]:
    """Union-find: devolve o id do grupo (raiz) de cada item."""
    pai = list(range(n))
    def raiz(x):
        while pai[x] != x:
            pai[x] = pai[pai[x]]
            x = pai[x]
        return x
    for i, j, _ in pares:
        pai[raiz(i)] = raiz(j)
    return [raiz(i) for i in range(n)]

# COMMAND ----------

# ---------------- Base: documentação vigente ----------------
# Usa a descrição corrigida pelo time quando existir e ignora o que foi rejeitado na revisão
filtro_cols = f"AND NOT coluna RLIKE {lit(COLS_IGNORAR)}" if COLS_IGNORAR else ""
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _doc AS
    SELECT catalogo, `schema`, tabela, lower(caminho_tabela) AS caminho_tabela, tipo_objeto,
           descricao_tabela, descricao_linha, posicao_coluna, coluna, lower(coluna) AS coluna_norm,
           lower(tipo_coluna) AS tipo_coluna, coalesce(descricao_corrigida, descricao_coluna) AS descricao_coluna
    FROM {DOC}
    WHERE coalesce(status_revisao, '') <> 'rejeitado' {filtro_cols}
""")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW _tabelas AS
    SELECT caminho_tabela, first(tabela) AS tabela, first(tipo_objeto) AS tipo_objeto,
           first(descricao_tabela, true) AS descricao_tabela, first(descricao_linha, true) AS descricao_linha,
           count(DISTINCT coluna_norm) AS n_colunas,
           sha2(array_join(array_sort(collect_set(concat(coluna_norm, ':', tipo_coluna))), ','), 256) AS assinatura
    FROM _doc GROUP BY caminho_tabela
""")
print(f"{spark.table('_tabelas').count():,} tabelas | {spark.table('_doc').count():,} colunas na base")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Tabelas duplicadas

# COMMAND ----------

# ---- 1a) Colunas em comum (Jaccard). Candidatos vêm de colunas pouco frequentes; a interseção é exata ----
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _pares_colunas AS
    WITH cols AS (SELECT DISTINCT caminho_tabela, coluna_norm FROM _doc),
    freq AS (SELECT coluna_norm FROM cols GROUP BY coluna_norm HAVING count(*) <= {MAX_COLS_FREQUENTES}),
    candidatos AS (
      SELECT DISTINCT a.caminho_tabela AS tabela_a, b.caminho_tabela AS tabela_b
      FROM cols a JOIN cols b ON a.coluna_norm = b.coluna_norm AND a.caminho_tabela < b.caminho_tabela
      WHERE a.coluna_norm IN (SELECT coluna_norm FROM freq)
    ),
    inter AS (
      SELECT c.tabela_a, c.tabela_b, count(*) AS colunas_em_comum
      FROM candidatos c
      JOIN cols a ON a.caminho_tabela = c.tabela_a
      JOIN cols b ON b.caminho_tabela = c.tabela_b AND b.coluna_norm = a.coluna_norm
      GROUP BY 1, 2
    )
    SELECT i.*, round(i.colunas_em_comum / (ta.n_colunas + tb.n_colunas - i.colunas_em_comum), 3) AS jaccard_colunas
    FROM inter i
    JOIN _tabelas ta ON ta.caminho_tabela = i.tabela_a
    JOIN _tabelas tb ON tb.caminho_tabela = i.tabela_b
""")

# ---- 1b) Mesmo nome de tabela em outro schema/catálogo ----
spark.sql("""
    CREATE OR REPLACE TEMP VIEW _pares_nome AS
    SELECT a.caminho_tabela AS tabela_a, b.caminho_tabela AS tabela_b
    FROM _tabelas a JOIN _tabelas b ON lower(a.tabela) = lower(b.tabela) AND a.caminho_tabela < b.caminho_tabela
""")

# ---- 1c) Descrição semelhante (embeddings da descrição da tabela + granularidade da linha) ----
tabs = spark.sql("SELECT caminho_tabela, descricao_tabela, descricao_linha FROM _tabelas "
                 "WHERE descricao_tabela IS NOT NULL").collect()
texto_tab = {r.caminho_tabela: f"{r.descricao_tabela} Cada linha: {r.descricao_linha or ''}" for r in tabs}
vet = embeddings(list(texto_tab.values()))
nomes_tab = [t for t, txt in texto_tab.items() if txt in vet]
pares_desc = []
if nomes_tab:
    M = np.stack([vet[texto_tab[t]] for t in nomes_tab])
    pares_desc = [(min(nomes_tab[i], nomes_tab[j]), max(nomes_tab[i], nomes_tab[j]), round(s, 4))
                  for i, j, s in pares_similares(M, LIMIAR_TAB)]
spark.createDataFrame(pares_desc, "tabela_a STRING, tabela_b STRING, similaridade_descricao DOUBLE") \
     .createOrReplaceTempView("_pares_desc")
print(f"pares por descrição: {len(pares_desc):,}")

# ---- 1d) Lineage direto entre as duas (uma é cópia/derivação da outra?) ----
spark.sql("""
    CREATE OR REPLACE TEMP VIEW _pares_lineage AS
    SELECT DISTINCT least(lower(source_table_full_name), lower(target_table_full_name))    AS tabela_a,
                    greatest(lower(source_table_full_name), lower(target_table_full_name)) AS tabela_b
    FROM system.access.table_lineage
    WHERE source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL
      AND event_date >= current_date() - INTERVAL 365 DAYS
""")

# COMMAND ----------

# Leituras de cada tabela do par vêm do uc_uso_tabelas, se ele já tiver rodado
if tabela_existe(TABELA_USO):
    USO_JOIN = (f"LEFT JOIN {TABELA_USO} ua ON lower(ua.caminho_tabela) = p.tabela_a "
                f"LEFT JOIN {TABELA_USO} ub ON lower(ub.caminho_tabela) = p.tabela_b")
else:
    print(f"⚠ {TABELA_USO} não existe: rode o uc_uso_tabelas para saber qual tabela de cada par é a mais usada")
    USO_JOIN = ("LEFT JOIN (SELECT CAST(NULL AS BIGINT) AS leituras) ua ON false "
                "LEFT JOIN (SELECT CAST(NULL AS BIGINT) AS leituras) ub ON false")

spark.sql(f"""
    CREATE OR REPLACE TABLE {PREFIXO}_tabelas
    COMMENT 'Pares de tabelas possivelmente duplicadas (uc_duplicidades)'
    AS
    WITH p AS (
      SELECT tabela_a, tabela_b,
             bool_or(mesmo_nome) AS mesmo_nome, max(colunas_em_comum) AS colunas_em_comum,
             max(jaccard_colunas) AS jaccard_colunas, max(similaridade_descricao) AS similaridade_descricao
      FROM (SELECT tabela_a, tabela_b, true AS mesmo_nome, NULL AS colunas_em_comum, NULL AS jaccard_colunas,
                   NULL AS similaridade_descricao FROM _pares_nome
            UNION ALL
            SELECT tabela_a, tabela_b, false, colunas_em_comum, jaccard_colunas, NULL FROM _pares_colunas
            WHERE jaccard_colunas >= {LIMIAR_JACCARD}
            UNION ALL
            SELECT tabela_a, tabela_b, false, NULL, NULL, similaridade_descricao FROM _pares_desc)
      GROUP BY 1, 2
    ),
    enriquecido AS (
      SELECT p.tabela_a, p.tabela_b, p.mesmo_nome,
             ta.assinatura = tb.assinatura AS mesmo_schema,
             ta.n_colunas AS colunas_a, tb.n_colunas AS colunas_b,
             coalesce(p.colunas_em_comum, pc.colunas_em_comum, 0) AS colunas_em_comum,
             coalesce(p.jaccard_colunas, pc.jaccard_colunas, 0)   AS jaccard_colunas,
             p.similaridade_descricao,
             l.tabela_a IS NOT NULL AS tem_lineage,
             ua.leituras AS leituras_a, ub.leituras AS leituras_b,
             ta.descricao_tabela AS descricao_a, tb.descricao_tabela AS descricao_b
      FROM p
      JOIN _tabelas ta ON ta.caminho_tabela = p.tabela_a
      JOIN _tabelas tb ON tb.caminho_tabela = p.tabela_b
      LEFT JOIN _pares_colunas pc ON pc.tabela_a = p.tabela_a AND pc.tabela_b = p.tabela_b
      LEFT JOIN _pares_lineage l ON l.tabela_a = p.tabela_a AND l.tabela_b = p.tabela_b
      {USO_JOIN}
    )
    SELECT *,
           filter(array(CASE WHEN mesmo_nome THEN 'mesmo nome' END,
                        CASE WHEN mesmo_schema THEN 'mesmas colunas e tipos' END,
                        CASE WHEN jaccard_colunas >= {LIMIAR_JACCARD} AND NOT mesmo_schema THEN 'colunas quase iguais' END,
                        CASE WHEN similaridade_descricao >= {LIMIAR_TAB} THEN 'descrição semelhante' END,
                        CASE WHEN tem_lineage THEN 'uma é gerada da outra' END), x -> x IS NOT NULL) AS motivos,
           CASE WHEN tem_lineage AND jaccard_colunas >= {LIMIAR_JACCARD}
                     THEN 'cópia derivada: avaliar se a cópia ainda é necessária'
                WHEN jaccard_colunas >= {LIMIAR_JACCARD} AND similaridade_descricao >= {LIMIAR_TAB}
                     THEN 'provável duplicata: consolidar'
                WHEN mesmo_nome AND jaccard_colunas < 0.5
                     THEN 'mesmo nome com conteúdo diferente: risco de confusão'
                WHEN similaridade_descricao >= {LIMIAR_TAB} AND jaccard_colunas < 0.5
                     THEN 'mesmo assunto com estruturas diferentes: verificar sobreposição'
                ELSE 'estrutura parecida: verificar' END AS sugestao,
           CASE WHEN coalesce(leituras_a, 0) = 0 AND coalesce(leituras_b, 0) = 0 THEN NULL
                WHEN coalesce(leituras_a, 0) >= coalesce(leituras_b, 0) THEN tabela_a
                ELSE tabela_b END AS mais_usada,
           '{ID_EXECUCAO}' AS id_execucao, current_timestamp() AS atualizado_em
    FROM enriquecido
""")
print(f"✓ {PREFIXO}_tabelas: {spark.table(f'{PREFIXO}_tabelas').count():,} pares")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Colunas repetidas (mesmo conceito em várias tabelas)

# COMMAND ----------

# Cada descrição distinta vira um ponto; descrições parecidas formam um grupo de conceito.
descricoes = [r[0] for r in spark.sql(
    "SELECT DISTINCT descricao_coluna FROM _doc WHERE descricao_coluna IS NOT NULL").collect()]
vet = embeddings(descricoes)
textos_col = list(vet)
grupos = []
if textos_col:
    M = np.stack([vet[t] for t in textos_col])
    pares = pares_similares(M, LIMIAR_COL)
    grupos = agrupar(len(textos_col), pares)
    print(f"{len(textos_col):,} descrições distintas | {len(pares):,} pares semelhantes | "
          f"{len(set(grupos)):,} conceitos")
spark.createDataFrame(list(zip(textos_col, grupos)), "descricao_coluna STRING, grupo BIGINT") \
     .createOrReplaceTempView("_grupo_desc")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW _colunas_grupo AS
    SELECT d.*, g.grupo FROM _doc d JOIN _grupo_desc g USING (descricao_coluna)
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {PREFIXO}_colunas
    COMMENT 'Colunas que representam o mesmo conceito em várias tabelas (uc_duplicidades)'
    AS
    WITH s AS (
      SELECT grupo, count(DISTINCT caminho_tabela) AS tabelas_no_grupo,
             count(DISTINCT coluna_norm) AS nomes_distintos, count(DISTINCT tipo_coluna) AS tipos_distintos,
             mode(coluna_norm) AS conceito,
             array_join(slice(array_sort(collect_set(coluna_norm)), 1, 15), ', ') AS nomes_usados,
             array_join(slice(array_sort(collect_set(tipo_coluna)), 1, 10), ', ')  AS tipos_usados
      FROM _colunas_grupo GROUP BY grupo
    )
    SELECT s.grupo AS grupo_conceito, s.conceito, s.tabelas_no_grupo, s.nomes_distintos, s.tipos_distintos,
           s.nomes_usados, s.tipos_usados,
           CASE WHEN s.nomes_distintos > 1 AND s.tipos_distintos > 1 THEN 'nomes e tipos diferentes'
                WHEN s.nomes_distintos > 1 THEN 'mesmo conceito com nomes diferentes'
                WHEN s.tipos_distintos > 1 THEN 'mesmo conceito com tipos diferentes'
                ELSE 'repetida (mesmo nome e tipo)' END AS situacao,
           c.catalogo, c.`schema`, c.caminho_tabela, c.coluna, c.tipo_coluna, c.descricao_coluna,
           '{ID_EXECUCAO}' AS id_execucao, current_timestamp() AS atualizado_em
    FROM _colunas_grupo c JOIN s USING (grupo)
    WHERE s.tabelas_no_grupo >= 2
""")
print(f"✓ {PREFIXO}_colunas: {spark.table(f'{PREFIXO}_colunas').count():,} colunas em conceitos repetidos")

# Mesmo nome de coluna com significados diferentes (o nome cai em mais de um conceito)
spark.sql(f"""
    CREATE OR REPLACE TABLE {PREFIXO}_colunas_homonimas
    COMMENT 'Nomes de colunas usados com significados diferentes (uc_duplicidades)'
    AS
    SELECT coluna_norm AS coluna, count(DISTINCT grupo) AS significados_distintos,
           count(DISTINCT caminho_tabela) AS tabelas,
           slice(collect_list(named_struct('tabela', caminho_tabela, 'descricao', descricao_coluna)), 1, 20) AS exemplos,
           '{ID_EXECUCAO}' AS id_execucao, current_timestamp() AS atualizado_em
    FROM _colunas_grupo
    GROUP BY coluna_norm
    HAVING count(DISTINCT grupo) > 1
""")
print(f"✓ {PREFIXO}_colunas_homonimas: {spark.table(f'{PREFIXO}_colunas_homonimas').count():,} nomes ambíguos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Regras de negócio repetidas ou divergentes
# MAGIC Fontes de SQL: definição das views documentadas (`system.information_schema.views`) e, opcionalmente, o último SQL
# MAGIC que gravou cada tabela documentada (`system.access.table_lineage.statement_id` → `system.query.history`).
# MAGIC Lógica em PySpark/Scala de notebooks não aparece no `query.history` e fica de fora.

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {REGRAS_EXTRAIDAS} (
    objeto STRING, origem STRING, hash_sql STRING, posicao INT, nome_regra STRING, descricao_regra STRING,
    expressao STRING, colunas ARRAY<STRING>, modelo STRING, id_execucao STRING, gerado_em TIMESTAMP)
    COMMENT 'Regras de negócio extraídas por LLM do SQL de views e cargas (uc_duplicidades). nome_regra nulo = sem regra'""")

# ---- SQL de cada objeto ----
fontes = [f"""
    SELECT lower(concat_ws('.', v.table_catalog, v.table_schema, v.table_name)) AS objeto, 'view' AS origem,
           v.view_definition AS sql_texto
    FROM system.information_schema.views v
    WHERE lower(concat_ws('.', v.table_catalog, v.table_schema, v.table_name)) IN (SELECT caminho_tabela FROM _tabelas)
      AND v.view_definition IS NOT NULL"""]
if INCLUIR_CARGA:
    if "statement_id" in spark.table("system.access.table_lineage").columns:
        fontes.append("""
    SELECT objeto, 'sql_de_carga' AS origem, sql_texto FROM (
      SELECT lower(l.target_table_full_name) AS objeto, h.statement_text AS sql_texto,
             row_number() OVER (PARTITION BY lower(l.target_table_full_name) ORDER BY l.event_time DESC) AS n
      FROM system.access.table_lineage l
      JOIN system.query.history h ON h.statement_id = l.statement_id
      WHERE l.event_date >= current_date() - INTERVAL 30 DAYS
        AND lower(l.target_table_full_name) IN (SELECT caminho_tabela FROM _tabelas)
        AND h.statement_text IS NOT NULL)
    WHERE n = 1""")
    else:
        print("⚠ table_lineage sem statement_id nesta conta: usando só as views")
spark.sql("CREATE OR REPLACE TEMP VIEW _sql_objetos AS " + " UNION ALL ".join(fontes))

objetos = spark.sql(f"""
    SELECT o.objeto, o.origem, o.sql_texto, sha2(o.sql_texto, 256) AS hash_sql, t.descricao_tabela
    FROM _sql_objetos o JOIN _tabelas t ON t.caminho_tabela = o.objeto
""").collect()
ja_extraidos = {(r.objeto, r.origem, r.hash_sql) for r in spark.sql(
    f"SELECT DISTINCT objeto, origem, hash_sql FROM {REGRAS_EXTRAIDAS}").collect()}
pendentes = [o for o in objetos if REPROCESSAR or (o.objeto, o.origem, o.hash_sql) not in ja_extraidos]
if MAX_OBJ_REGRAS > 0:
    pendentes = pendentes[:MAX_OBJ_REGRAS]
print(f"{len(objetos):,} objetos com SQL | {len(pendentes):,} a extrair nesta execução")

# COMMAND ----------

# ---- Extração das regras com LLM (ai_query nos executors) ----
PROMPT_REGRAS = """Você é um engenheiro de dados sênior fazendo governança de regras de negócio.
Escreva em {idioma}. Responda SOMENTE com JSON válido, sem markdown.

Analise o SQL abaixo e extraia as REGRAS DE NEGÓCIO que ele implementa:
- filtros que definem um conjunto com significado (ex.: "cliente ativo", "venda válida");
- classificações e faixas (CASE WHEN), status derivados;
- métricas calculadas (fórmulas, agregações com significado de negócio);
- critérios de deduplicação / "registro vigente" e janelas de tempo.
Ignore renomeações simples, casts, joins puramente técnicos e colunas copiadas sem transformação.

Objeto: {objeto} ({origem})
Descrição do objeto: {descricao}
SQL:
{sql}

Retorne JSON: {{"regras": [{{"nome": "nome curto e genérico do conceito, ex.: cliente ativo",
  "descricao": "o que a regra define, em linguagem de negócio, sem nomes de tabelas",
  "expressao": "trecho SQL que implementa a regra",
  "colunas": ["colunas envolvidas"]}}]}}
Se não houver regra de negócio, retorne {{"regras": []}}."""

def ai_query_lote(prompts: list[str]) -> list[tuple[str | None, str | None]]:
    """Roda os prompts em paralelo nos executors; devolve (resposta, erro) na mesma ordem."""
    df = (spark.createDataFrame(list(enumerate(prompts)), "id INT, prompt STRING")
               .repartition(max(1, min(len(prompts), 64))))
    linhas = df.selectExpr("id", f"""ai_query({lit(MODELO)}, prompt,
                 modelParameters => named_struct('max_tokens', 4000, 'temperature', 0.1),
                 failOnError => false) AS r""").selectExpr(
                 "id", "r.result AS resposta", "r.errorMessage AS erro").collect()
    por_id = {r.id: (r.resposta, r.erro) for r in linhas}
    return [por_id.get(i, (None, "sem resposta")) for i in range(len(prompts))]

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, ArrayType, TimestampType)
schema_regras = StructType([
    StructField("objeto", StringType()), StructField("origem", StringType()), StructField("hash_sql", StringType()),
    StructField("posicao", IntegerType()), StructField("nome_regra", StringType()),
    StructField("descricao_regra", StringType()), StructField("expressao", StringType()),
    StructField("colunas", ArrayType(StringType())), StructField("modelo", StringType()),
    StructField("id_execucao", StringType()), StructField("gerado_em", TimestampType()),
])

LOTE = 50
for ini in range(0, len(pendentes) if EXTRAIR_REGRAS else 0, LOTE):
    lote = pendentes[ini:ini + LOTE]
    prompts = [PROMPT_REGRAS.format(idioma=IDIOMA, objeto=o.objeto, origem=o.origem,
                                    descricao=o.descricao_tabela or "(sem descrição)",
                                    sql=o.sql_texto[:MAX_CHARS_SQL]) for o in lote]
    agora, linhas, falhas = datetime.datetime.utcnow(), [], 0
    for o, p, (texto, erro) in zip(lote, prompts, ai_query_lote(prompts)):
        USO["chamadas"] += 1; USO["entrada"] += len(p) // 4; USO["saida"] += len(texto or "") // 4
        r = ler_json(texto)
        if r is None:
            falhas += 1   # não grava: tenta de novo na próxima execução
            continue
        regras = [x for x in r.get("regras", []) if isinstance(x, dict) and x.get("nome")]
        base = dict(objeto=o.objeto, origem=o.origem, hash_sql=o.hash_sql, modelo=MODELO,
                    id_execucao=ID_EXECUCAO, gerado_em=agora)
        # objeto sem regra ganha 1 linha com nome nulo: marca como já analisado
        linhas += [dict(base, posicao=i, nome_regra=str(x["nome"]), descricao_regra=x.get("descricao"),
                        expressao=x.get("expressao"), colunas=[str(c) for c in (x.get("colunas") or [])])
                   for i, x in enumerate(regras, start=1)] or [dict(base, posicao=0, nome_regra=None,
                        descricao_regra=None, expressao=None, colunas=[])]
    if linhas:
        spark.createDataFrame(linhas, schema=schema_regras).createOrReplaceTempView("_regras_novas")
        # remove versões anteriores do mesmo objeto/origem (SQL mudou ou reprocessar) antes de inserir
        spark.sql(f"""MERGE INTO {REGRAS_EXTRAIDAS} AS d
                      USING (SELECT DISTINCT objeto, origem FROM _regras_novas) AS n
                        ON d.objeto = n.objeto AND d.origem = n.origem
                      WHEN MATCHED THEN DELETE""")
        spark.sql(f"INSERT INTO {REGRAS_EXTRAIDAS} SELECT * FROM _regras_novas")
    print(f"💾 {min(ini + LOTE, len(pendentes))}/{len(pendentes)} objetos | {falhas} falha(s) neste lote | "
          f"≈ US$ {custo_usd(USO['entrada'], USO['saida']):.2f} (estimado)")

# COMMAND ----------

# ---- Agrupamento das regras equivalentes ----
def normalizar_sql(expr: str | None) -> str:
    """Compara lógicas ignorando caixa, espaços, crases e aliases de tabela (ex.: c.status → status)."""
    e = (expr or "").lower().replace("`", "")
    e = re.sub(r"\b[a-z_][a-z0-9_]*\.(?=[a-z_])", "", e)
    return re.sub(r"\s+", " ", e).strip(" ;")

# só regras de objetos que ainda existem na base
regras = spark.sql(f"""
    SELECT r.* FROM {REGRAS_EXTRAIDAS} r
    WHERE r.nome_regra IS NOT NULL AND r.objeto IN (SELECT caminho_tabela FROM _tabelas)
""").collect()
texto_regra = [f"{r.nome_regra}: {r.descricao_regra or ''}" for r in regras]
vet = embeddings(texto_regra)
validas = [i for i, t in enumerate(texto_regra) if t in vet]
grupo_regra = {}
if validas:
    M = np.stack([vet[texto_regra[i]] for i in validas])
    for pos, g in enumerate(agrupar(len(validas), pares_similares(M, LIMIAR_REGRA))):
        grupo_regra[validas[pos]] = validas[g]
spark.createDataFrame(
    [(r.objeto, r.origem, r.posicao, grupo_regra[i], normalizar_sql(r.expressao))
     for i, r in enumerate(regras) if i in grupo_regra],
    "objeto STRING, origem STRING, posicao INT, grupo BIGINT, expressao_norm STRING"
).createOrReplaceTempView("_grupo_regra")

spark.sql(f"""
    CREATE OR REPLACE TABLE {PREFIXO}_regras
    COMMENT 'Regras de negócio repetidas em vários objetos ou com lógicas divergentes (uc_duplicidades)'
    AS
    WITH r AS (
      SELECT e.*, g.grupo, g.expressao_norm
      FROM {REGRAS_EXTRAIDAS} e
      JOIN _grupo_regra g USING (objeto, origem, posicao)
    ),
    s AS (
      SELECT grupo, mode(lower(nome_regra)) AS conceito, count(DISTINCT objeto) AS objetos_no_grupo,
             count(DISTINCT expressao_norm) AS implementacoes_distintas
      FROM r GROUP BY grupo
    )
    SELECT s.grupo AS grupo_regra, s.conceito, s.objetos_no_grupo, s.implementacoes_distintas,
           CASE WHEN s.implementacoes_distintas > 1 THEN 'mesma regra com lógicas diferentes: alinhar definição'
                ELSE 'mesma regra repetida: centralizar numa view/tabela única' END AS situacao,
           r.objeto, r.origem, r.nome_regra, r.descricao_regra, r.expressao, r.colunas,
           '{ID_EXECUCAO}' AS id_execucao, current_timestamp() AS atualizado_em
    FROM r JOIN s USING (grupo)
    WHERE s.objetos_no_grupo >= 2
""")
print(f"✓ {PREFIXO}_regras: {spark.table(f'{PREFIXO}_regras').count():,} regras em grupos repetidos")
print(f"\n💰 Execução {ID_EXECUCAO}: {USO['chamadas']} chamadas LLM | ≈ US$ "
      f"{custo_usd(USO['entrada'], USO['saida']):.4f} (estimado; embeddings não incluídos)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visões

# COMMAND ----------

# Tabelas duplicadas: as mais "certas" primeiro
display(spark.sql(f"""
    SELECT sugestao, tabela_a, tabela_b, motivos, colunas_a, colunas_b, colunas_em_comum, jaccard_colunas,
           similaridade_descricao, tem_lineage, leituras_a, leituras_b, mais_usada, descricao_a, descricao_b
    FROM {PREFIXO}_tabelas
    ORDER BY size(motivos) DESC, jaccard_colunas DESC, similaridade_descricao DESC NULLS LAST
"""))

# COMMAND ----------

# Conceitos repetidos: 1 linha por conceito, os que têm nomes/tipos inconsistentes primeiro
display(spark.sql(f"""
    SELECT grupo_conceito, conceito, situacao, tabelas_no_grupo, nomes_distintos, nomes_usados, tipos_usados,
           first(descricao_coluna) AS exemplo_descricao,
           array_join(slice(collect_set(caminho_tabela), 1, 10), ', ') AS exemplos_tabelas
    FROM {PREFIXO}_colunas
    GROUP BY ALL
    ORDER BY (nomes_distintos > 1 OR tipos_distintos > 1) DESC, tabelas_no_grupo DESC
"""))

# COMMAND ----------

# Mesmo nome, significados diferentes
display(spark.sql(f"""
    SELECT coluna, significados_distintos, tabelas, exemplos
    FROM {PREFIXO}_colunas_homonimas
    ORDER BY significados_distintos DESC, tabelas DESC
"""))

# COMMAND ----------

# Regras de negócio: divergentes primeiro (mesmo conceito implementado de jeitos diferentes)
display(spark.sql(f"""
    SELECT grupo_regra, conceito, situacao, objetos_no_grupo, implementacoes_distintas,
           objeto, origem, nome_regra, descricao_regra, expressao
    FROM {PREFIXO}_regras
    ORDER BY implementacoes_distintas > 1 DESC, objetos_no_grupo DESC, grupo_regra, objeto
"""))
