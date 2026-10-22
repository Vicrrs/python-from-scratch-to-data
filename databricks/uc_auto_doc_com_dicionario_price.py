# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-documentação do Unity Catalog com LLM
# MAGIC Reconstruído a partir das fotos do notebook.
# MAGIC Versão com dicionário de dados: além dos COMMENTs, grava `caminho da tabela + descrição da tabela + descrição de cada coluna`
# MAGIC na tabela do widget `tabela_dicionario`. Trechos alterados estão marcados com `>>> NOVO`.

# COMMAND ----------

# ---------------- Parâmetros ----------------
dbutils.widgets.removeAll()
dbutils.widgets.text("catalogos_incluir", "data_science_dev,data_science_hml", "Catálogos a incluir (vírgula; vazio = todos)")
dbutils.widgets.text("catalogos_excluir", "system,samples,hive_metastore,__databricks_internal", "Catálogos a excluir")
dbutils.widgets.text("schemas_excluir", "information_schema", "Schemas a excluir")
dbutils.widgets.text("modelo_endpoint", "databricks-claude-sonnet-4-5", "Serving endpoint do LLM")
dbutils.widgets.text("idioma", "português do Brasil", "Idioma das descrições")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (só loga, não aplica)")
dbutils.widgets.dropdown("sobrescrever", "false", ["true", "false"], "Sobrescrever comentários existentes")
dbutils.widgets.text("linhas_amostra", "5", "Linhas de amostra por tabela")
dbutils.widgets.text("max_tabelas_catalogo", "2", "Máx tabelas por catálogo (0 = todas)")
dbutils.widgets.text("tabela_log", "data_science_dev.churn_models.uc_auto_doc_log", "Tabela Delta de log")
# >>> NOVO: tabela onde fica o dicionário de dados (1 linha por coluna de cada tabela)
dbutils.widgets.text("tabela_dicionario", "data_science_dev.churn_models.uc_dicionario_dados", "Tabela Delta do dicionário")

def _lista(nome):
    return [x.strip() for x in dbutils.widgets.get(nome).split(",") if x.strip()]

CAT_INCLUIR    = _lista("catalogos_incluir")
CAT_EXCLUIR    = set(_lista("catalogos_excluir"))
SCHEMA_EXCLUIR = set(_lista("schemas_excluir"))
MODELO         = dbutils.widgets.get("modelo_endpoint")
IDIOMA         = dbutils.widgets.get("idioma")
DRY_RUN        = dbutils.widgets.get("dry_run") == "true"
SOBRESCREVER   = dbutils.widgets.get("sobrescrever") == "true"
N_AMOSTRA      = int(dbutils.widgets.get("linhas_amostra"))
MAX_TAB_CAT    = int(dbutils.widgets.get("max_tabelas_catalogo"))
TABELA_LOG     = dbutils.widgets.get("tabela_log")
TABELA_DICIONARIO = dbutils.widgets.get("tabela_dicionario")   # >>> NOVO

# >>> NOVO (custos): tabela de custos + preço do modelo em US$ por 1 milhão de tokens
dbutils.widgets.text("tabela_custos", "data_science_dev.churn_models.uc_auto_doc_custos", "Tabela Delta de custos")
dbutils.widgets.text("preco_input_1m", "3.00", "US$ por 1M tokens de entrada")
dbutils.widgets.text("preco_output_1m", "15.00", "US$ por 1M tokens de saída")
TABELA_CUSTOS   = dbutils.widgets.get("tabela_custos")
PRECO_INPUT_1M  = float(dbutils.widgets.get("preco_input_1m"))
PRECO_OUTPUT_1M = float(dbutils.widgets.get("preco_output_1m"))

MAX_COLS_POR_CHAMADA = 60   # tabelas largas são documentadas em lotes de colunas
MAX_CHARS_VALOR      = 80   # trunca valores longos da amostra
print(f"dry_run={DRY_RUN} | sobrescrever={SOBRESCREVER} | modelo={MODELO} | max_tabelas_cat={MAX_TAB_CAT}")

# COMMAND ----------

import json, re, time, datetime, uuid
from databricks.sdk import WorkspaceClient

# >>> NOVO (custos): contador global de tokens; cada operação mede a diferença antes/depois
ID_EXECUCAO = str(uuid.uuid4())
USO = {"chamadas": 0, "entrada": 0, "saida": 0}
custos_registros = []

def _somar_uso(resp):
    USO["chamadas"] += 1
    u = getattr(resp, "usage", None)
    if u:
        USO["entrada"] += u.prompt_tokens or 0
        USO["saida"]   += u.completion_tokens or 0

def custo_usd(entrada, saida):
    return entrada / 1_000_000 * PRECO_INPUT_1M + saida / 1_000_000 * PRECO_OUTPUT_1M

def inicio_operacao():
    return dict(USO)

def registrar_custo(nivel, objeto, catalogo, inicio):
    chamadas = USO["chamadas"] - inicio["chamadas"]
    if chamadas == 0:          # nada foi enviado ao LLM (ex.: tabela já documentada)
        return 0.0
    entrada = USO["entrada"] - inicio["entrada"]
    saida   = USO["saida"]   - inicio["saida"]
    custo   = custo_usd(entrada, saida)
    custos_registros.append(dict(
        id_execucao=ID_EXECUCAO, executado_em=datetime.datetime.utcnow(),
        nivel=nivel, objeto=objeto, catalogo=catalogo, modelo=MODELO,
        chamadas_llm=chamadas, tokens_entrada=entrada, tokens_saida=saida,
        tokens_total=entrada + saida, custo_usd=round(custo, 6), dry_run=DRY_RUN))
    print(f"      💲 {nivel} {objeto}: {entrada + saida:,} tokens ≈ US$ {custo:.4f}")
    return custo

w = WorkspaceClient()
llm = w.serving_endpoints.get_open_ai_client()   # cliente OpenAI-compatível autenticado no workspace

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

def chamar_llm(prompt: str, tentativas: int = 4) -> dict:
    for i in range(tentativas):
        try:
            resp = llm.chat.completions.create(
                model=MODELO,
                messages=[{"role": "system", "content": SYSTEM_PROMPT},
                          {"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=4000,
            )
            _somar_uso(resp)   # >>> NOVO (custos): conta tokens, inclusive de tentativas que falharem no JSON
            texto = resp.choices[0].message.content.strip()
            texto = re.sub(r"^```(json)?|```$", "", texto, flags=re.M).strip()
            return json.loads(texto)
        except Exception as e:
            espera = 2 ** i * 5
            print(f"   ⚠ erro LLM ({e.__class__.__name__}: {e}); nova tentativa em {espera}s")
            time.sleep(espera)
    raise RuntimeError("LLM falhou após várias tentativas")

# COMMAND ----------

# ---------------- Utilitários SQL ----------------
def q(ident: str) -> str:
    """Quota um identificador com crases."""
    return "`" + ident.replace("`", "``") + "`"

def literal(txt: str) -> str:
    """Literal SQL seguro para COMMENT."""
    return "'" + txt.replace("\\", "\\\\").replace("'", "\\'") + "'"

def amostra(fqn: str):
    try:
        rows = spark.table(fqn).limit(N_AMOSTRA).collect()
        return [{k: (str(v)[:MAX_CHARS_VALOR] if v is not None else None)
                 for k, v in r.asDict().items()} for r in rows]
    except Exception as e:
        return f"<amostra indisponível: {e.__class__.__name__}>"

log_registros = []
def registrar(nivel, objeto, coluna, antigo, novo, status):
    log_registros.append(dict(
        executado_em=datetime.datetime.utcnow(), nivel=nivel, objeto=objeto, coluna=coluna,
        comentario_antigo=antigo, comentario_novo=novo, status=status, dry_run=DRY_RUN))

# >>> NOVO: acumula o dicionário de dados (caminho da tabela + descrição da tabela + descrição de cada coluna)
dicionario_registros = []
def registrar_dicionario(t, tipo_obj, desc_tabela, colunas, desc_cols):
    agora = datetime.datetime.utcnow()
    for pos, c in enumerate(colunas, start=1):
        # usa a descrição nova do LLM; se não houver, mantém o comentário que já existia
        descricao = desc_cols.get(c["nome"]) or c["comentario_atual"]
        dicionario_registros.append(dict(
            catalogo=t.catalog_name,
            schema=t.schema_name,
            tabela=t.name,
            caminho_tabela=t.full_name,                 # ex.: catalogo.schema.tabela
            tipo_objeto=tipo_obj,                       # TABLE / VIEW
            descricao_tabela=desc_tabela,
            posicao_coluna=pos,
            coluna=c["nome"],
            tipo_coluna=c["tipo"],
            coluna_particao=c["particao"],
            descricao_coluna=descricao,
            comentario_coluna_anterior=c["comentario_atual"],
            gerado_por_llm=c["nome"] in desc_cols,
            atualizado_em=agora,
        ))

def aplicar(sql: str, nivel, objeto, coluna, antigo, novo):
    if not novo:
        return
    if antigo and not SOBRESCREVER:
        registrar(nivel, objeto, coluna, antigo, novo, "ignorado_ja_existia")
        return
    if DRY_RUN:
        registrar(nivel, objeto, coluna, antigo, novo, "dry_run")
        return
    try:
        spark.sql(sql)
        registrar(nivel, objeto, coluna, antigo, novo, "aplicado")
    except Exception as e:
        registrar(nivel, objeto, coluna, antigo, novo, f"erro: {str(e)[:300]}")

# COMMAND ----------

# ---------------- Documentação de tabelas e colunas ----------------
def documentar_tabela(t) -> str:
    fqn = f"{q(t.catalog_name)}.{q(t.schema_name)}.{q(t.name)}"
    colunas = [dict(nome=c.name, tipo=c.type_text, comentario_atual=c.comment,
                    particao=c.partition_index is not None)
               for c in (t.columns or [])]

    tipo_obj = "VIEW" if str(t.table_type).endswith("VIEW") else "TABLE"

    precisa_tabela = SOBRESCREVER or not t.comment
    precisa_cols = [c for c in colunas if SOBRESCREVER or not c["comentario_atual"]]
    if not precisa_tabela and not precisa_cols:
        print(f"   ✓ {t.full_name}: já documentada")
        registrar_dicionario(t, tipo_obj, t.comment, colunas, {})   # >>> NOVO: grava o que já existe
        return t.comment

    dados_amostra = amostra(fqn)
    desc_tabela, desc_cols = t.comment, {}

    lotes = [colunas[i:i + MAX_COLS_POR_CHAMADA] for i in range(0, len(colunas), MAX_COLS_POR_CHAMADA)] or [[]]
    for n, lote in enumerate(lotes):
        nomes_lote = {c["nome"] for c in lote}
        amostra_lote = ([{k: v for k, v in r.items() if k in nomes_lote} for r in dados_amostra]
                        if isinstance(dados_amostra, list) else dados_amostra)
        prompt = f"""Documente a tabela abaixo do Unity Catalog.

Catálogo: {t.catalog_name}
Schema: {t.schema_name}
Tabela: {t.name}
Tipo: {t.table_type}
Comentário atual da tabela: {t.comment or '(nenhum)'}
Todas as colunas da tabela (contexto): {[c['nome'] for c in colunas]}

Colunas a documentar neste lote ({n + 1}/{len(lotes)}):
{json.dumps(lote, ensure_ascii=False, default=str)}

Amostra de dados (apenas para entender o conteúdo, não reproduza):
{json.dumps(amostra_lote, ensure_ascii=False, default=str)}

Retorne JSON no formato:
{{"descricao_tabela": "...", "colunas": {{"nome_coluna": "descrição", ...}}}}"""
        r = chamar_llm(prompt)
        if n == 0:
            desc_tabela = r.get("descricao_tabela") or desc_tabela
        desc_cols.update(r.get("colunas", {}))

    # Comentário da tabela / view
    aplicar(f"COMMENT ON TABLE {fqn} IS {literal(desc_tabela or '')}",
            "view" if tipo_obj == "VIEW" else "tabela", t.full_name, None, t.comment, desc_tabela)

    # Comentários das colunas
    for c in colunas:
        novo = desc_cols.get(c["nome"])
        sql = f"ALTER TABLE {fqn} ALTER COLUMN {q(c['nome'])} COMMENT {literal(novo or '')}"
        if tipo_obj == "VIEW":
            # Views só aceitam comentário de coluna via COMMENT ON COLUMN
            sql = f"COMMENT ON COLUMN {fqn}.{q(c['nome'])} IS {literal(novo or '')}"
        aplicar(sql, "coluna", t.full_name, c["nome"], c["comentario_atual"], novo)

    registrar_dicionario(t, tipo_obj, desc_tabela, colunas, desc_cols)   # >>> NOVO

    print(f"   ✓ {t.full_name}: {len(desc_cols)} colunas descritas")
    return desc_tabela

# COMMAND ----------

# ---------------- Varredura completa ----------------
def resumir(nivel: str, nome: str, filhos: dict, atual: str | None) -> str:
    prompt = f"""Escreva a descrição do {nivel} "{nome}" do Unity Catalog com base no conteúdo abaixo.
Comentário atual: {atual or '(nenhum)'}
Itens contidos e suas descrições:
{json.dumps(filhos, ensure_ascii=False)[:15000]}

Retorne JSON: {{"descricao": "..."}}"""
    return chamar_llm(prompt).get("descricao")

catalogos = [c for c in w.catalogs.list()
             if c.name not in CAT_EXCLUIR and (not CAT_INCLUIR or c.name in CAT_INCLUIR)]
print(f"Catálogos a processar: {[c.name for c in catalogos]}")

for cat in catalogos:
    print(f"\n📚 Catálogo {cat.name}")
    desc_schemas = {}
    tabelas_cat = 0
    try:
        schemas = [s for s in w.schemas.list(catalog_name=cat.name) if s.name not in SCHEMA_EXCLUIR]
    except Exception as e:
        print(f"   ✗ sem acesso aos schemas: {e}")
        continue

    for sch in schemas:
        if MAX_TAB_CAT > 0 and tabelas_cat >= MAX_TAB_CAT:
            print(f"   ⏭ limite de {MAX_TAB_CAT} tabelas atingido para {cat.name}")
            break
        print(f"  📁 Schema {sch.full_name}")
        desc_tabelas = {}
        try:
            tabelas = list(w.tables.list(catalog_name=cat.name, schema_name=sch.name))
            if MAX_TAB_CAT > 0:
                tabelas = tabelas[:MAX_TAB_CAT - tabelas_cat]
        except Exception as e:
            print(f"   ✗ sem acesso às tabelas: {e}")
            continue

        for t in tabelas:
            inicio = inicio_operacao()   # >>> NOVO (custos)
            try:
                desc_tabelas[t.name] = documentar_tabela(t)
                tabelas_cat += 1
            except Exception as e:
                print(f"   ✗ {t.full_name}: {e}")
                registrar("tabela", t.full_name, None, t.comment, None, f"erro: {str(e)[:300]}")
            finally:
                registrar_custo("tabela", t.full_name, cat.name, inicio)   # >>> NOVO (custos)

        if tabelas and (SOBRESCREVER or not sch.comment):
            inicio = inicio_operacao()   # >>> NOVO (custos)
            d = resumir("schema", sch.full_name, desc_tabelas, sch.comment)
            registrar_custo("schema", sch.full_name, cat.name, inicio)   # >>> NOVO (custos)
            aplicar(f"COMMENT ON SCHEMA {q(cat.name)}.{q(sch.name)} IS {literal(d or '')}",
                    "schema", sch.full_name, None, sch.comment, d)
            desc_schemas[sch.name] = d
        else:
            desc_schemas[sch.name] = sch.comment

    if schemas and (SOBRESCREVER or not cat.comment):
        inicio = inicio_operacao()   # >>> NOVO (custos)
        d = resumir("catálogo", cat.name, desc_schemas, cat.comment)
        registrar_custo("catalogo", cat.name, cat.name, inicio)   # >>> NOVO (custos)
        aplicar(f"COMMENT ON CATALOG {q(cat.name)} IS {literal(d or '')}",
                "catalogo", cat.name, None, cat.comment, d)

# COMMAND ----------

# ---------------- Grava log para auditoria / revisão ----------------
if log_registros:
    df_log = spark.createDataFrame(log_registros)
    df_log.write.mode("append").option("mergeSchema", "true").saveAsTable(TABELA_LOG)
    display(df_log.groupBy("nivel", "status").count().orderBy("nivel"))
    display(df_log)
else:
    print("Nada a registrar.")

# COMMAND ----------

# ---------------- >>> NOVO: Grava o dicionário de dados ----------------
# Upsert por (caminho_tabela, coluna): rodar de novo atualiza a linha em vez de duplicar.
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               BooleanType, TimestampType)

schema_dic = StructType([
    StructField("catalogo", StringType()),
    StructField("schema", StringType()),
    StructField("tabela", StringType()),
    StructField("caminho_tabela", StringType()),
    StructField("tipo_objeto", StringType()),
    StructField("descricao_tabela", StringType()),
    StructField("posicao_coluna", IntegerType()),
    StructField("coluna", StringType()),
    StructField("tipo_coluna", StringType()),
    StructField("coluna_particao", BooleanType()),
    StructField("descricao_coluna", StringType()),
    StructField("comentario_coluna_anterior", StringType()),
    StructField("gerado_por_llm", BooleanType()),
    StructField("atualizado_em", TimestampType()),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABELA_DICIONARIO} (
        catalogo STRING, `schema` STRING, tabela STRING, caminho_tabela STRING,
        tipo_objeto STRING, descricao_tabela STRING, posicao_coluna INT,
        coluna STRING, tipo_coluna STRING, coluna_particao BOOLEAN,
        descricao_coluna STRING, comentario_coluna_anterior STRING,
        gerado_por_llm BOOLEAN, atualizado_em TIMESTAMP
    ) COMMENT 'Dicionário de dados gerado pelo notebook de auto-documentação do Unity Catalog'
""")

if dicionario_registros:
    df_dic = spark.createDataFrame(dicionario_registros, schema=schema_dic)
    df_dic.createOrReplaceTempView("_dicionario_novo")
    spark.sql(f"""
        MERGE INTO {TABELA_DICIONARIO} AS d
        USING _dicionario_novo AS n
          ON d.caminho_tabela = n.caminho_tabela AND d.coluna = n.coluna
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"📘 {df_dic.count()} colunas gravadas em {TABELA_DICIONARIO}")
    display(df_dic.orderBy("caminho_tabela", "posicao_coluna"))
else:
    print("Nenhuma tabela processada para o dicionário.")

# COMMAND ----------

# ---------------- >>> NOVO (custos): Grava e mostra o custo em US$ ----------------
# Histórico: cada execução entra com um id_execucao novo (append).
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

schema_custos = StructType([
    StructField("id_execucao", StringType()),
    StructField("executado_em", TimestampType()),
    StructField("nivel", StringType()),          # tabela / schema / catalogo
    StructField("objeto", StringType()),         # caminho completo do objeto
    StructField("catalogo", StringType()),
    StructField("modelo", StringType()),
    StructField("chamadas_llm", LongType()),
    StructField("tokens_entrada", LongType()),
    StructField("tokens_saida", LongType()),
    StructField("tokens_total", LongType()),
    StructField("custo_usd", DoubleType()),
    StructField("dry_run", BooleanType()),
])

if custos_registros:
    df_custos = spark.createDataFrame(custos_registros, schema=schema_custos)
    df_custos.write.mode("append").option("mergeSchema", "true").saveAsTable(TABELA_CUSTOS)

    print(f"💰 Execução {ID_EXECUCAO}")
    print(f"   Total: {USO['chamadas']} chamadas | {USO['entrada']:,} tokens entrada | "
          f"{USO['saida']:,} tokens saída | ≈ US$ {custo_usd(USO['entrada'], USO['saida']):.4f}")

    # Custo de cada operação (tabela / schema / catálogo)
    display(df_custos.orderBy(F.desc("custo_usd")))

    # Custo total por catálogo (somando tabelas + schemas + o próprio catálogo)
    display(df_custos.groupBy("catalogo").agg(
        F.sum("chamadas_llm").alias("chamadas_llm"),
        F.sum("tokens_total").alias("tokens_total"),
        F.round(F.sum("custo_usd"), 4).alias("custo_usd"),
        F.count(F.when(F.col("nivel") == "tabela", 1)).alias("tabelas_documentadas"),
    ).orderBy(F.desc("custo_usd")))

    # Custo por nível
    display(df_custos.groupBy("nivel").agg(
        F.sum("tokens_total").alias("tokens_total"),
        F.round(F.sum("custo_usd"), 4).alias("custo_usd"),
        F.round(F.avg("custo_usd"), 4).alias("custo_medio_usd"),
    ))
else:
    print("Nenhuma chamada ao LLM nesta execução (custo US$ 0).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar depois de revisar (opcional)
# MAGIC Se rodou em `dry_run`, pode aplicar exatamente o que foi revisado/editado na tabela de log
# MAGIC sem chamar o LLM de novo:

# COMMAND ----------

def aplicar_do_log(execucao_desde: str):
    """Aplica comentários do log gerados em dry_run a partir de uma data (ex.: '2026-10-07')."""
    pend = spark.sql(f"""
        SELECT * FROM {TABELA_LOG}
        WHERE status = 'dry_run' AND executado_em >= '{execucao_desde}' AND comentario_novo IS NOT NULL
    """).collect()

    ok, erros = 0, 0
    for r in pend:
        partes = r.objeto.split(".")
        fqn = ".".join(q(p) for p in partes)
        if r.nivel == "catalogo":
            sql = f"COMMENT ON CATALOG {fqn} IS {literal(r.comentario_novo)}"
        elif r.nivel == "schema":
            sql = f"COMMENT ON SCHEMA {fqn} IS {literal(r.comentario_novo)}"
        elif r.nivel in ("tabela", "view"):
            sql = f"COMMENT ON TABLE {fqn} IS {literal(r.comentario_novo)}"
        else:
            sql = f"COMMENT ON COLUMN {fqn}.{q(r.coluna)} IS {literal(r.comentario_novo)}"
        try:
            spark.sql(sql)
            ok += 1
            print(f"  ✓ {r.nivel}: {r.objeto} {r.coluna or ''}")
        except Exception as e:
            erros += 1
            print(f"  ✗ {r.objeto} {r.coluna or ''}: {e}")

    print(f"\n✅ {ok} aplicados | ❌ {erros} erros | Total: {len(pend)}")

# aplicar_do_log("2026-10-08")



# COMMAND ----------

WITH cols AS (
  SELECT table_catalog, table_schema, table_name,
         count(*) AS n_cols,
         count_if(comment IS NULL OR comment = '') AS cols_sem_comentario
  FROM system.information_schema.columns
  WHERE table_catalog NOT IN ('system', 'samples', 'hive_metastore', '__databricks_internal')
    AND table_schema <> 'information_schema'
  GROUP BY ALL
),
tabs AS (
  SELECT t.table_catalog, t.table_schema, t.table_name,
         (t.comment IS NULL OR t.comment = '' OR c.cols_sem_comentario > 0) AS nao_documentada,
         c.n_cols
  FROM system.information_schema.tables t
  JOIN cols c USING (table_catalog, table_schema, table_name)
)
SELECT
  coalesce(table_catalog, 'TOTAL')                       AS catalogo,
  count(DISTINCT table_schema)                           AS schemas,
  count(*)                                               AS tabelas,
  count_if(nao_documentada)                              AS tabelas_nao_documentadas,
  sum(n_cols)                                            AS colunas,
  -- conta de padeiro: US$ 0,0107 por tabela + US$ 0,0036 por schema
  round(count_if(nao_documentada) * 0.0107
        + count(DISTINCT table_schema) * 0.0036, 2)      AS custo_estimado_usd,
  round((count_if(nao_documentada) * 0.0107
        + count(DISTINCT table_schema) * 0.0036) * 1.5, 2) AS custo_com_margem_usd
FROM tabs
GROUP BY ROLLUP (table_catalog)
ORDER BY catalogo = 'TOTAL', custo_estimado_usd DESC

# COMMAND ----------
