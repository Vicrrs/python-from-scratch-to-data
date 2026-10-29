# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-documentação do Unity Catalog com LLM — modo revisão (`ai_query`)
# MAGIC Mesma saída do `uc_auto_doc_revisao` (tabela `tabela_destino`, **1 linha por coluna**, campos de revisão),
# MAGIC mas as chamadas ao LLM rodam com **`ai_query` nos executors**, em paralelo, em vez de threads no driver.
# MAGIC
# MAGIC - Metadados vêm de `system.information_schema` (1 consulta, sem centenas de chamadas de API).
# MAGIC - Tabelas são processadas em lotes de `tabelas_por_lote`; cada lote é gravado ao terminar.
# MAGIC   Se cair, a próxima execução pula o que já foi gravado.
# MAGIC - Não altera NENHUM comentário no Unity Catalog.
# MAGIC - Tokens/custo são **estimados** (~4 caracteres por token): o `ai_query` não devolve o uso real.
# MAGIC
# MAGIC **Recomendado:** rodar como Job (job cluster dedicado, Retries = 3).

# COMMAND ----------

# Limpa widgets antigos (precisa ficar em célula separada da que os recria)
dbutils.widgets.removeAll()

# COMMAND ----------

# ---------------- Parâmetros ----------------
# >>> ONDE GRAVAR: caminho completo catalogo.schema.nome_da_tabela (criada se não existir)
dbutils.widgets.text("tabela_destino", "data_science_dev.churn_models.uc_doc_revisao", "Tabela destino (catalogo.schema.tabela)")
dbutils.widgets.text("catalogos_incluir", "", "Catálogos a incluir (vírgula; vazio = todos)")
dbutils.widgets.text("catalogos_excluir", "system,samples,hive_metastore,__databricks_internal", "Catálogos a excluir")
dbutils.widgets.text("schemas_incluir", "", "Schemas a incluir (vírgula; vazio = todos)")
dbutils.widgets.text("schemas_excluir", "information_schema", "Schemas a excluir")
dbutils.widgets.text("modelo_endpoint", "databricks-claude-sonnet-4-5", "Serving endpoint do LLM")
dbutils.widgets.text("idioma", "português do Brasil", "Idioma das descrições")
dbutils.widgets.dropdown("reprocessar", "false", ["true", "false"], "Reprocessar tabelas já gravadas no destino")
dbutils.widgets.text("linhas_amostra", "5", "Linhas de amostra por tabela (0 = sem amostra)")
dbutils.widgets.dropdown("amostra_views", "false", ["true", "false"], "Amostrar views (pode ser lento)")
dbutils.widgets.text("max_tabelas_catalogo", "0", "Máx tabelas por catálogo (0 = todas)")
dbutils.widgets.text("max_tabelas_execucao", "500", "Máx tabelas novas nesta execução (0 = todas)")
dbutils.widgets.text("tabelas_por_lote", "50", "Tabelas por lote do ai_query (gravadas ao fim de cada lote)")
dbutils.widgets.text("preco_input_1m", "3.00", "US$ por 1M tokens de entrada")
dbutils.widgets.text("preco_output_1m", "15.00", "US$ por 1M tokens de saída")

def _lista(nome):
    return [x.strip() for x in dbutils.widgets.get(nome).split(",") if x.strip()]

DESTINO         = dbutils.widgets.get("tabela_destino").strip()
assert DESTINO.count(".") == 2, f"tabela_destino deve ser catalogo.schema.tabela (recebido: '{DESTINO}')"
CAT_INCLUIR     = _lista("catalogos_incluir")
CAT_EXCLUIR     = _lista("catalogos_excluir")
SCHEMA_INCLUIR  = _lista("schemas_incluir")
SCHEMA_EXCLUIR  = _lista("schemas_excluir")
MODELO          = dbutils.widgets.get("modelo_endpoint").strip()
IDIOMA          = dbutils.widgets.get("idioma")
REPROCESSAR     = dbutils.widgets.get("reprocessar") == "true"
N_AMOSTRA       = int(dbutils.widgets.get("linhas_amostra"))
AMOSTRA_VIEWS   = dbutils.widgets.get("amostra_views") == "true"
MAX_TAB_CAT     = int(dbutils.widgets.get("max_tabelas_catalogo"))
MAX_TAB_EXEC    = int(dbutils.widgets.get("max_tabelas_execucao"))
TAB_POR_LOTE    = int(dbutils.widgets.get("tabelas_por_lote"))
PRECO_INPUT_1M  = float(dbutils.widgets.get("preco_input_1m"))
PRECO_OUTPUT_1M = float(dbutils.widgets.get("preco_output_1m"))

MAX_COLS_POR_CHAMADA = 60   # tabelas largas são documentadas em lotes de colunas
MAX_CHARS_VALOR      = 80   # trunca valores longos da amostra
THREADS_AMOSTRA      = 8    # amostras são consultas Spark curtas; só elas usam threads no driver
print(f"destino={DESTINO} | reprocessar={REPROCESSAR} | modelo={MODELO} | max_tabelas_cat={MAX_TAB_CAT} | "
      f"max_tabelas_exec={MAX_TAB_EXEC} | tabelas_por_lote={TAB_POR_LOTE}")

# COMMAND ----------

# ---------------- Tabela de destino ----------------
assert "DESTINO" in globals(), "Rode a célula de Parâmetros antes (ou use Run all)."
# Colunas geradas pelo notebook + colunas de revisão (preenchidas pelo time dono da tabela).
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DESTINO} (
        catalogo STRING, `schema` STRING, tabela STRING, caminho_tabela STRING, tipo_objeto STRING,
        posicao_coluna INT, coluna STRING, tipo_coluna STRING, coluna_particao BOOLEAN,
        descricao_catalogo STRING, descricao_schema STRING, descricao_tabela STRING,
        descricao_linha STRING, descricao_coluna STRING,
        comentario_catalogo_atual STRING, comentario_schema_atual STRING,
        comentario_tabela_atual STRING, comentario_coluna_atual STRING,
        modelo STRING, id_execucao STRING, gerado_em TIMESTAMP,
        chamadas_llm_tabela BIGINT, tokens_tabela BIGINT, custo_tabela_usd DOUBLE,
        tokens_schema BIGINT, custo_schema_usd DOUBLE,
        tokens_catalogo BIGINT, custo_catalogo_usd DOUBLE,
        status_revisao STRING, descricao_corrigida STRING, observacao_revisao STRING,
        revisado_por STRING, revisado_em TIMESTAMP
    ) COMMENT 'Descrições geradas por LLM para revisão do time responsável (não aplicadas no Unity Catalog)'
""")
# gravação por lote gera vários MERGEs pequenos: o Delta compacta os arquivos sozinho
spark.sql(f"""ALTER TABLE {DESTINO} SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')""")

# COMMAND ----------

# ---------------- LLM via ai_query ----------------
import json, re, time, datetime, uuid

ID_EXECUCAO = str(uuid.uuid4())
USO = {"chamadas": 0, "entrada": 0, "saida": 0}   # tokens estimados da execução

SYSTEM_PROMPT = f"""Você é um engenheiro de dados sênior responsável pela governança e pelo catálogo de dados da empresa.
Escreva descrições de negócio claras, objetivas e úteis para analistas e para o Genie (AI/BI).
Regras:
- Escreva em {IDIOMA}.
- Descreva o SIGNIFICADO de negócio, não apenas o tipo técnico.
- Mencione unidade, formato, domínio de valores ou chaves/relacionamentos quando for evidente.
- Não invente fatos; se for incerto, seja genérico e sinalize com "provavelmente".
- Nunca repita dados sensíveis (PII) da amostra nas descrições.
- Descrições de colunas: até 200 caracteres. Tabelas/schemas/catálogos: até 500 caracteres.
- Responda SOMENTE com JSON válido, sem markdown."""

def lit(valor: str) -> str:
    """Literal de string SQL."""
    return "'" + valor.replace("\\", "\\\\").replace("'", "\\'") + "'"

def q(ident: str) -> str:
    """Quota um identificador com crases."""
    return "`" + ident.replace("`", "``") + "`"

def tokens_estimados(texto: str | None) -> int:
    return len(texto or "") // 4

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

def ai_query_lote(prompts: list[str]) -> list[tuple[str | None, str | None]]:
    """Roda os prompts em paralelo nos executors; devolve (resposta, erro) na mesma ordem."""
    df = (spark.createDataFrame(list(enumerate(prompts)), "id INT, prompt STRING")
               .repartition(max(1, min(len(prompts), 64))))   # 1 partição por prompt (até 64) = mais paralelismo
    linhas = df.selectExpr("id", f"""ai_query({lit(MODELO)}, prompt,
                 modelParameters => named_struct('max_tokens', 4000, 'temperature', 0.1),
                 failOnError => false) AS r""").selectExpr(
                 "id", "r.result AS resposta", "r.errorMessage AS erro").collect()
    por_id = {r.id: (r.resposta, r.erro) for r in linhas}
    return [por_id.get(i, (None, "sem resposta")) for i in range(len(prompts))]

def chamar_llm_lote(prompts: list[str], tentativas: int = 2):
    """Devolve (jsons, tokens) na ordem dos prompts; json=None se falhar em todas as tentativas.
    tokens[i] = (entrada, saida) estimados, somando as tentativas."""
    prompts = [SYSTEM_PROMPT + "\n\n" + p for p in prompts]
    resultados, tokens = [None] * len(prompts), [(0, 0)] * len(prompts)
    faltando = list(range(len(prompts)))
    for tentativa in range(1, tentativas + 1):
        if not faltando:
            break
        erros = []
        for i, (texto, erro) in zip(faltando, ai_query_lote([prompts[i] for i in faltando])):
            ent, sai = tokens_estimados(prompts[i]), tokens_estimados(texto)
            tokens[i] = (tokens[i][0] + ent, tokens[i][1] + sai)
            USO["chamadas"] += 1; USO["entrada"] += ent; USO["saida"] += sai
            resultados[i] = ler_json(texto)
            if resultados[i] is None:
                erros.append((i, erro or "JSON inválido"))
        faltando = [i for i, _ in erros]
        if erros:
            acao = "nova tentativa" if tentativa < tentativas else "desistindo (ficam para a próxima execução)"
            print(f"   ⚠ {len(erros)} chamada(s) falharam ({str(erros[0][1])[:200]}); {acao}")
    return resultados, tokens

# COMMAND ----------

# ---------------- Gravação no destino ----------------
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               BooleanType, TimestampType, LongType, DoubleType)

schema_linhas = StructType([
    StructField("catalogo", StringType()), StructField("schema", StringType()),
    StructField("tabela", StringType()), StructField("caminho_tabela", StringType()),
    StructField("tipo_objeto", StringType()), StructField("posicao_coluna", IntegerType()),
    StructField("coluna", StringType()), StructField("tipo_coluna", StringType()),
    StructField("coluna_particao", BooleanType()),
    StructField("descricao_tabela", StringType()), StructField("descricao_linha", StringType()),
    StructField("descricao_coluna", StringType()),
    StructField("comentario_tabela_atual", StringType()), StructField("comentario_coluna_atual", StringType()),
    StructField("modelo", StringType()), StructField("id_execucao", StringType()),
    StructField("gerado_em", TimestampType()),
    StructField("chamadas_llm_tabela", LongType()), StructField("tokens_tabela", LongType()),
    StructField("custo_tabela_usd", DoubleType()),
])

schema_resumo = StructType([
    StructField("catalogo", StringType()), StructField("schema", StringType()),
    StructField("descricao_catalogo", StringType()), StructField("descricao_schema", StringType()),
    StructField("comentario_catalogo_atual", StringType()), StructField("comentario_schema_atual", StringType()),
    StructField("tokens_schema", LongType()), StructField("custo_schema_usd", DoubleType()),
    StructField("tokens_catalogo", LongType()), StructField("custo_catalogo_usd", DoubleType()),
])

def gravar_linhas(linhas):
    """Upsert por (caminho_tabela, coluna). Reprocessar reseta a revisão só das colunas cuja descrição mudou."""
    if not linhas:
        return
    spark.createDataFrame(linhas, schema=schema_linhas).createOrReplaceTempView("_doc_novo")
    cols = [f.name for f in schema_linhas.fields]
    set_cols = ", ".join(f"d.{q(c)} = n.{q(c)}" for c in cols)
    spark.sql(f"""
        MERGE INTO {DESTINO} AS d
        USING _doc_novo AS n
          ON d.caminho_tabela = n.caminho_tabela AND d.coluna = n.coluna
        WHEN MATCHED THEN UPDATE SET {set_cols},
          d.status_revisao = CASE WHEN d.descricao_coluna <=> n.descricao_coluna
                                       AND d.descricao_tabela <=> n.descricao_tabela
                                  THEN d.status_revisao ELSE 'pendente' END
        WHEN NOT MATCHED THEN INSERT ({", ".join(q(c) for c in cols)}, status_revisao)
          VALUES ({", ".join(f"n.{q(c)}" for c in cols)}, 'pendente')
    """)

def gravar_resumos(resumos):
    """Preenche descrição/custo de schema e catálogo em todas as linhas daquele schema."""
    if not resumos:
        return
    spark.createDataFrame(resumos, schema=schema_resumo).createOrReplaceTempView("_doc_resumo")
    # coalesce: campos nulos mantêm o valor que já estava gravado
    set_cols = ", ".join(f"d.{q(f.name)} = coalesce(n.{q(f.name)}, d.{q(f.name)})"
                         for f in schema_resumo.fields[2:])
    spark.sql(f"""
        MERGE INTO {DESTINO} AS d
        USING _doc_resumo AS n
          ON d.catalogo = n.catalogo AND d.`schema` = n.`schema`
        WHEN MATCHED THEN UPDATE SET {set_cols}
    """)

# COMMAND ----------

# ---------------- Planejamento: metadados via information_schema ----------------
from collections import defaultdict

def _filtro(col, valores, incluir):
    if not valores:
        return "TRUE"
    return f"{col} {'IN' if incluir else 'NOT IN'} ({', '.join(lit(v) for v in valores)})"

filtros = " AND ".join([
    _filtro("t.table_catalog", CAT_INCLUIR, True), _filtro("t.table_catalog", CAT_EXCLUIR, False),
    _filtro("t.table_schema", SCHEMA_INCLUIR, True), _filtro("t.table_schema", SCHEMA_EXCLUIR, False)])

pendentes = spark.sql(f"""
    SELECT * FROM (
      SELECT t.table_catalog AS catalogo, t.table_schema AS `schema`, t.table_name AS tabela,
             concat_ws('.', t.table_catalog, t.table_schema, t.table_name) AS caminho_tabela,
             t.table_type AS tipo, t.comment AS comentario,
             row_number() OVER (PARTITION BY t.table_catalog ORDER BY t.table_schema, t.table_name) AS rn_cat
      FROM system.information_schema.tables t
      WHERE {filtros}
        AND ({str(REPROCESSAR).upper()} OR NOT EXISTS (
              SELECT 1 FROM {DESTINO} d
              WHERE d.caminho_tabela = concat_ws('.', t.table_catalog, t.table_schema, t.table_name)))
    )
    WHERE {MAX_TAB_CAT} = 0 OR rn_cat <= {MAX_TAB_CAT}
    ORDER BY catalogo, `schema`, tabela
    {f"LIMIT {MAX_TAB_EXEC}" if MAX_TAB_EXEC > 0 else ""}
""").collect()

colunas_por_tabela = defaultdict(list)
if pendentes:
    spark.createDataFrame([(t.catalogo, t.schema, t.tabela) for t in pendentes],
                          "catalogo STRING, `schema` STRING, tabela STRING").createOrReplaceTempView("_pendentes")
    for r in spark.sql("""
        SELECT concat_ws('.', c.table_catalog, c.table_schema, c.table_name) AS caminho_tabela,
               c.column_name, c.full_data_type, c.partition_index, c.comment
        FROM system.information_schema.columns c
        JOIN _pendentes p ON c.table_catalog = p.catalogo AND c.table_schema = p.`schema` AND c.table_name = p.tabela
        ORDER BY caminho_tabela, c.ordinal_position""").collect():
        colunas_por_tabela[r.caminho_tabela].append(dict(
            nome=r.column_name, tipo=r.full_data_type, comentario_atual=r.comment,
            particao=r.partition_index is not None))

sem_colunas = [t.caminho_tabela for t in pendentes if not colunas_por_tabela[t.caminho_tabela]]
pendentes = [t for t in pendentes if colunas_por_tabela[t.caminho_tabela]]
if sem_colunas:
    print(f"⏭ {len(sem_colunas)} tabelas sem colunas visíveis puladas (ex.: {sem_colunas[:3]})")
por_cat = defaultdict(int)
for t in pendentes:
    por_cat[t.catalogo] += 1
print(f"🚀 {len(pendentes)} tabelas a documentar: {dict(por_cat)}")

# COMMAND ----------

# ---------------- Documentação de tabelas e colunas (sem aplicar) ----------------
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F

def amostra(t):
    # trunca no Spark (executors): só strings curtas chegam ao driver, evitando OOM em colunas grandes
    if N_AMOSTRA <= 0:
        return "<sem amostra>"
    if "VIEW" in str(t.tipo) and not AMOSTRA_VIEWS:
        return "<amostra não coletada: view>"
    fqn = f"{q(t.catalogo)}.{q(t.schema)}.{q(t.tabela)}"
    try:
        df = spark.table(fqn).limit(N_AMOSTRA)
        rows = df.select(*[F.substring(F.col(q(c)).cast("string"), 1, MAX_CHARS_VALOR).alias(c)
                           for c in df.columns]).collect()
        return [r.asDict() for r in rows]
    except Exception as e:
        return f"<amostra indisponível: {e.__class__.__name__}>"

def prompts_tabela(t, colunas, dados_amostra) -> list[str]:
    """1 prompt por lote de MAX_COLS_POR_CHAMADA colunas."""
    lotes = [colunas[i:i + MAX_COLS_POR_CHAMADA] for i in range(0, len(colunas), MAX_COLS_POR_CHAMADA)]
    prompts = []
    for n, lote in enumerate(lotes):
        nomes_lote = {c["nome"] for c in lote}
        amostra_lote = ([{k: v for k, v in r.items() if k in nomes_lote} for r in dados_amostra]
                        if isinstance(dados_amostra, list) else dados_amostra)
        prompts.append(f"""Documente a tabela abaixo do Unity Catalog.

Catálogo: {t.catalogo}
Schema: {t.schema}
Tabela: {t.tabela}
Tipo: {t.tipo}
Comentário atual da tabela: {t.comentario or '(nenhum)'}
Todas as colunas da tabela (contexto): {[c['nome'] for c in colunas]}

Colunas a documentar neste lote ({n + 1}/{len(lotes)}):
{json.dumps(lote, ensure_ascii=False, default=str)}

Amostra de dados (apenas para entender o conteúdo, não reproduza):
{json.dumps(amostra_lote, ensure_ascii=False, default=str)}

Retorne JSON no formato:
{{"descricao_tabela": "o que a tabela contém e para que serve",
  "descricao_linha": "o que cada linha representa (granularidade/chave), ex.: 'um contrato por cliente por mês'",
  "colunas": {{"nome_coluna": "descrição", ...}}}}""")
    return prompts

pool = ThreadPoolExecutor(max_workers=THREADS_AMOSTRA)
feitas = falhas = 0
t_ini = time.time()
for i in range(0, len(pendentes), TAB_POR_LOTE):
    lote = pendentes[i:i + TAB_POR_LOTE]
    t0 = time.time()
    amostras = list(pool.map(amostra, lote))
    t_amostra = time.time() - t0

    chaves, prompts = [], []   # chaves[k] = (tabela, n do lote de colunas)
    for t, dados in zip(lote, amostras):
        for n, p in enumerate(prompts_tabela(t, colunas_por_tabela[t.caminho_tabela], dados)):
            chaves.append((t.caminho_tabela, n))
            prompts.append(p)
    resultados, tokens = chamar_llm_lote(prompts)

    partes = defaultdict(list)   # tabela -> [(n, json, (entrada, saida))]
    for (caminho, n), r, tk in zip(chaves, resultados, tokens):
        partes[caminho].append((n, r, tk))

    agora, linhas = datetime.datetime.utcnow(), []
    for t in lote:
        p = sorted(partes[t.caminho_tabela], key=lambda x: x[0])
        if any(r is None for _, r, _ in p):
            falhas += 1
            print(f"   ✗ {t.caminho_tabela}: LLM falhou; fica para a próxima execução")
            continue
        desc_cols = {}
        for _, r, _ in p:
            if isinstance(r.get("colunas"), dict):
                desc_cols.update(r["colunas"])
        entrada, saida = sum(tk[0] for *_, tk in p), sum(tk[1] for *_, tk in p)
        tipo_obj = "VIEW" if "VIEW" in str(t.tipo) else "TABLE"
        linhas += [dict(
            catalogo=t.catalogo, schema=t.schema, tabela=t.tabela, caminho_tabela=t.caminho_tabela,
            tipo_objeto=tipo_obj, posicao_coluna=pos, coluna=c["nome"], tipo_coluna=c["tipo"],
            coluna_particao=c["particao"],
            descricao_tabela=p[0][1].get("descricao_tabela"), descricao_linha=p[0][1].get("descricao_linha"),
            descricao_coluna=desc_cols.get(c["nome"]),
            comentario_tabela_atual=t.comentario, comentario_coluna_atual=c["comentario_atual"],
            modelo=MODELO, id_execucao=ID_EXECUCAO, gerado_em=agora,
            chamadas_llm_tabela=len(p), tokens_tabela=entrada + saida,
            custo_tabela_usd=round(custo_usd(entrada, saida), 6))
            for pos, c in enumerate(colunas_por_tabela[t.caminho_tabela], start=1)]
        feitas += 1
    gravar_linhas(linhas)

    mins = (time.time() - t_ini) / 60
    processadas = i + len(lote)
    print(f"💾 {processadas}/{len(pendentes)} tabelas ({feitas} gravadas, {falhas} falhas) | "
          f"lote: amostra {t_amostra:.0f}s + LLM/gravação {time.time() - t0 - t_amostra:.0f}s, {len(prompts)} chamadas | "
          f"{mins:.0f} min | ~{mins / processadas * (len(pendentes) - processadas):.0f} min restantes | "
          f"≈ US$ {custo_usd(USO['entrada'], USO['saida']):.2f}")
pool.shutdown()

# COMMAND ----------

# ---------------- Resumos de schema e catálogo ----------------
# Schemas com tabela gravada nesta execução, ou que ainda não têm descrição (execução anterior interrompida)
def prompt_resumo(nivel, nome, filhos, atual):
    return f"""Escreva a descrição do {nivel} "{nome}" do Unity Catalog com base no conteúdo abaixo.
Comentário atual: {atual or '(nenhum)'}
Itens contidos e suas descrições:
{json.dumps(filhos, ensure_ascii=False)[:15000]}

Retorne JSON: {{"descricao": "..."}}"""

tabelas_schema = defaultdict(dict)   # (catalogo, schema) -> {tabela: descricao_tabela}
coment_schema = {}
for r in spark.sql(f"""
    SELECT d.catalogo, d.`schema`, d.tabela, max(d.descricao_tabela) AS descricao_tabela,
           max(s.comment) AS comentario_schema
    FROM {DESTINO} d
    JOIN (SELECT DISTINCT catalogo, `schema` FROM {DESTINO}
          WHERE id_execucao = {lit(ID_EXECUCAO)} OR descricao_schema IS NULL) a
      ON d.catalogo = a.catalogo AND d.`schema` = a.`schema`
    LEFT JOIN system.information_schema.schemata s
      ON s.catalog_name = d.catalogo AND s.schema_name = d.`schema`
    GROUP BY d.catalogo, d.`schema`, d.tabela""").collect():
    tabelas_schema[(r.catalogo, r.schema)][r.tabela] = r.descricao_tabela
    coment_schema[(r.catalogo, r.schema)] = r.comentario_schema

schemas_alvo = list(tabelas_schema)
print(f"📁 {len(schemas_alvo)} schemas a resumir")
resumos = []
if schemas_alvo:
    resultados, tokens = chamar_llm_lote([
        prompt_resumo("schema", f"{c}.{s}", tabelas_schema[(c, s)], coment_schema[(c, s)]) for c, s in schemas_alvo])
    for (c, s), r, (ent, sai) in zip(schemas_alvo, resultados, tokens):
        if r is None:
            print(f"   ✗ resumo {c}.{s}: LLM falhou; fica para a próxima execução")
            continue
        resumos.append(dict(catalogo=c, schema=s, descricao_schema=r.get("descricao"),
                            comentario_schema_atual=coment_schema[(c, s)],
                            tokens_schema=ent + sai, custo_schema_usd=round(custo_usd(ent, sai), 6)))

# Catálogos com algum schema resumido agora: usa as descrições de todos os schemas dele (novas + já gravadas)
cats_alvo = sorted({r["catalogo"] for r in resumos})
if cats_alvo:
    schemas_cat = defaultdict(dict)
    for r in spark.sql(f"""
        SELECT catalogo, `schema`, max(descricao_schema) AS d FROM {DESTINO}
        WHERE catalogo IN ({", ".join(lit(c) for c in cats_alvo)}) GROUP BY 1, 2""").collect():
        if r.d:
            schemas_cat[r.catalogo][r.schema] = r.d
    for r in resumos:
        schemas_cat[r["catalogo"]][r["schema"]] = r["descricao_schema"]
    coment_cat = {r.catalog_name: r.comment for r in spark.sql(f"""
        SELECT catalog_name, comment FROM system.information_schema.catalogs
        WHERE catalog_name IN ({", ".join(lit(c) for c in cats_alvo)})""").collect()}

    resultados, tokens = chamar_llm_lote([
        prompt_resumo("catálogo", c, schemas_cat[c], coment_cat.get(c)) for c in cats_alvo])
    for c, r, (ent, sai) in zip(cats_alvo, resultados, tokens):
        if r is None:
            print(f"   ✗ resumo do catálogo {c}: LLM falhou")
        cat = dict(descricao_catalogo=r and r.get("descricao"), comentario_catalogo_atual=coment_cat.get(c),
                   tokens_catalogo=ent + sai, custo_catalogo_usd=round(custo_usd(ent, sai), 6))
        resumidos = {x["schema"] for x in resumos if x["catalogo"] == c}
        # o resumo do catálogo vale para todos os schemas dele; os não resumidos agora mantêm a descrição (coalesce)
        resumos += [dict(catalogo=c, schema=s, descricao_schema=None, comentario_schema_atual=None,
                         tokens_schema=None, custo_schema_usd=None)
                    for s in schemas_cat[c] if s not in resumidos]
        for x in resumos:
            if x["catalogo"] == c:
                x.update(cat)
    gravar_resumos(resumos)
    print(f"📚 resumos gravados: {len(schemas_alvo)} schemas, {len(cats_alvo)} catálogos")

print(f"\n💰 Execução {ID_EXECUCAO}: {USO['chamadas']} chamadas | ~{USO['entrada']:,} tokens entrada | "
      f"~{USO['saida']:,} tokens saída | ≈ US$ {custo_usd(USO['entrada'], USO['saida']):.4f} (estimado)")

# COMMAND ----------

# ---------------- Conferência ----------------
df = spark.table(DESTINO)
display(df.filter(F.col("id_execucao") == ID_EXECUCAO).orderBy("caminho_tabela", "posicao_coluna"))

# Custo por tabela (1 linha por tabela)
display(df.groupBy("catalogo", "schema", "tabela").agg(
    F.first("descricao_tabela").alias("descricao_tabela"),
    F.first("descricao_linha").alias("descricao_linha"),
    F.count("*").alias("colunas"),
    F.first("tokens_tabela").alias("tokens_tabela"),
    F.first("custo_tabela_usd").alias("custo_tabela_usd"),
).orderBy(F.desc("custo_tabela_usd")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revisão pelo time responsável
# MAGIC Visão resumida para mandar ao dono de cada tabela. O time preenche `status_revisao`
# MAGIC (`aprovado` / `corrigido` / `rejeitado`), `descricao_corrigida` e `observacao_revisao`, por exemplo:
# MAGIC ```sql
# MAGIC UPDATE <destino>
# MAGIC SET status_revisao = 'corrigido', descricao_corrigida = '...', revisado_por = current_user(), revisado_em = current_timestamp()
# MAGIC WHERE caminho_tabela = 'cat.schema.tabela' AND coluna = 'minha_coluna'
# MAGIC ```

# COMMAND ----------

display(spark.sql(f"""
    SELECT catalogo, `schema`, tabela, descricao_catalogo, descricao_schema, descricao_tabela, descricao_linha,
           posicao_coluna, coluna, tipo_coluna, descricao_coluna, comentario_coluna_atual,
           status_revisao, descricao_corrigida, observacao_revisao
    FROM {DESTINO}
    ORDER BY catalogo, `schema`, tabela, posicao_coluna
"""))

# Andamento da revisão por catálogo
display(spark.sql(f"""
    SELECT catalogo,
           count(DISTINCT caminho_tabela)            AS tabelas,
           count(*)                                  AS colunas,
           count_if(status_revisao = 'pendente')     AS pendentes,
           count_if(status_revisao = 'aprovado')     AS aprovadas,
           count_if(status_revisao = 'corrigido')    AS corrigidas,
           count_if(status_revisao = 'rejeitado')    AS rejeitadas
    FROM {DESTINO}
    GROUP BY catalogo ORDER BY catalogo
"""))
