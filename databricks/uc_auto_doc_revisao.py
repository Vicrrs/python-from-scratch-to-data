# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-documentação do Unity Catalog com LLM — modo revisão
# MAGIC Não altera NENHUM comentário no Unity Catalog. Tudo que o LLM gera é gravado numa tabela nova
# MAGIC (`caminho_destino`.`nome_tabela_destino`), com **1 linha por coluna** contendo:
# MAGIC catálogo, schema, tabela, coluna, descrição do catálogo / schema / tabela / linha / coluna e o custo de cada operação.
# MAGIC
# MAGIC A tabela tem campos de revisão (`status_revisao`, `descricao_corrigida`, `revisado_por`, ...) para o time
# MAGIC responsável confirmar ou corrigir o que foi gerado. Reprocessar não apaga o que já foi revisado.

# COMMAND ----------

# ---------------- Parâmetros ----------------
dbutils.widgets.removeAll()
dbutils.widgets.text("caminho_destino", "data_science_dev.churn_models", "Destino: catalogo.schema")
dbutils.widgets.text("nome_tabela_destino", "uc_doc_revisao", "Destino: nome da tabela")
dbutils.widgets.text("catalogos_incluir", "data_science_dev", "Catálogos a incluir (vírgula; vazio = todos)")
dbutils.widgets.text("catalogos_excluir", "system,samples,hive_metastore,__databricks_internal", "Catálogos a excluir")
dbutils.widgets.text("schemas_incluir", "", "Schemas a incluir (vírgula; vazio = todos)")
dbutils.widgets.text("schemas_excluir", "information_schema", "Schemas a excluir")
dbutils.widgets.text("modelo_endpoint", "databricks-claude-sonnet-4-5", "Serving endpoint do LLM")
dbutils.widgets.text("idioma", "português do Brasil", "Idioma das descrições")
dbutils.widgets.dropdown("reprocessar", "false", ["true", "false"], "Reprocessar tabelas já gravadas no destino")
dbutils.widgets.text("linhas_amostra", "5", "Linhas de amostra por tabela")
dbutils.widgets.text("max_tabelas_catalogo", "2", "Máx tabelas por catálogo (0 = todas)")
dbutils.widgets.text("preco_input_1m", "3.00", "US$ por 1M tokens de entrada")
dbutils.widgets.text("preco_output_1m", "15.00", "US$ por 1M tokens de saída")

def _lista(nome):
    return [x.strip() for x in dbutils.widgets.get(nome).split(",") if x.strip()]

DESTINO         = f"{dbutils.widgets.get('caminho_destino').strip()}.{dbutils.widgets.get('nome_tabela_destino').strip()}"
CAT_INCLUIR     = _lista("catalogos_incluir")
CAT_EXCLUIR     = set(_lista("catalogos_excluir"))
SCHEMA_INCLUIR  = set(_lista("schemas_incluir"))
SCHEMA_EXCLUIR  = set(_lista("schemas_excluir"))
MODELO          = dbutils.widgets.get("modelo_endpoint")
IDIOMA          = dbutils.widgets.get("idioma")
REPROCESSAR     = dbutils.widgets.get("reprocessar") == "true"
N_AMOSTRA       = int(dbutils.widgets.get("linhas_amostra"))
MAX_TAB_CAT     = int(dbutils.widgets.get("max_tabelas_catalogo"))
PRECO_INPUT_1M  = float(dbutils.widgets.get("preco_input_1m"))
PRECO_OUTPUT_1M = float(dbutils.widgets.get("preco_output_1m"))

MAX_COLS_POR_CHAMADA = 60   # tabelas largas são documentadas em lotes de colunas
MAX_CHARS_VALOR      = 80   # trunca valores longos da amostra
print(f"destino={DESTINO} | reprocessar={REPROCESSAR} | modelo={MODELO} | max_tabelas_cat={MAX_TAB_CAT}")

# COMMAND ----------

# ---------------- Tabela de destino ----------------
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

# Tabelas já gravadas no destino (pulamos se reprocessar=false) e suas descrições (contexto p/ resumo do schema)
ja_gravadas = {r.caminho_tabela: r.descricao_tabela for r in spark.sql(
    f"SELECT DISTINCT caminho_tabela, descricao_tabela FROM {DESTINO}").collect()}
print(f"{len(ja_gravadas)} tabelas já existem em {DESTINO}")

# COMMAND ----------

import json, re, time, datetime, uuid
from databricks.sdk import WorkspaceClient

# Contador global de tokens; cada operação mede a diferença antes/depois
ID_EXECUCAO = str(uuid.uuid4())
USO = {"chamadas": 0, "entrada": 0, "saida": 0}

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

def medir(objeto, inicio) -> dict:
    """Tokens e custo gastos desde `inicio`."""
    chamadas = USO["chamadas"] - inicio["chamadas"]
    entrada  = USO["entrada"]  - inicio["entrada"]
    saida    = USO["saida"]    - inicio["saida"]
    custo    = round(custo_usd(entrada, saida), 6)
    if chamadas:
        print(f"      💲 {objeto}: {entrada + saida:,} tokens ≈ US$ {custo:.4f}")
    return dict(chamadas=chamadas, tokens=entrada + saida, custo=custo)

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
            _somar_uso(resp)   # conta tokens, inclusive de tentativas que falharem no JSON
            texto = resp.choices[0].message.content.strip()
            texto = re.sub(r"^```(json)?|```$", "", texto, flags=re.M).strip()
            return json.loads(texto)
        except Exception as e:
            espera = 2 ** i * 5
            print(f"   ⚠ erro LLM ({e.__class__.__name__}: {e}); nova tentativa em {espera}s")
            time.sleep(espera)
    raise RuntimeError("LLM falhou após várias tentativas")

# COMMAND ----------

# ---------------- Utilitários ----------------
def q(ident: str) -> str:
    """Quota um identificador com crases."""
    return "`" + ident.replace("`", "``") + "`"

def amostra(fqn: str):
    try:
        rows = spark.table(fqn).limit(N_AMOSTRA).collect()
        return [{k: (str(v)[:MAX_CHARS_VALOR] if v is not None else None)
                 for k, v in r.asDict().items()} for r in rows]
    except Exception as e:
        return f"<amostra indisponível: {e.__class__.__name__}>"

# COMMAND ----------

# ---------------- Documentação de tabelas e colunas (sem aplicar) ----------------
def documentar_tabela(t) -> tuple[str, str, list[dict]]:
    """Retorna (descricao_tabela, descricao_linha, colunas com descricao_coluna)."""
    fqn = f"{q(t.catalog_name)}.{q(t.schema_name)}.{q(t.name)}"
    colunas = [dict(nome=c.name, tipo=c.type_text, comentario_atual=c.comment,
                    particao=c.partition_index is not None)
               for c in (t.columns or [])]

    dados_amostra = amostra(fqn)
    desc_tabela, desc_linha, desc_cols = None, None, {}

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
{{"descricao_tabela": "o que a tabela contém e para que serve",
  "descricao_linha": "o que cada linha representa (granularidade/chave), ex.: 'um contrato por cliente por mês'",
  "colunas": {{"nome_coluna": "descrição", ...}}}}"""
        r = chamar_llm(prompt)
        if n == 0:
            desc_tabela = r.get("descricao_tabela")
            desc_linha  = r.get("descricao_linha")
        desc_cols.update(r.get("colunas", {}))

    for c in colunas:
        c["descricao"] = desc_cols.get(c["nome"])
    print(f"   ✓ {t.full_name}: {len(desc_cols)}/{len(colunas)} colunas descritas")
    return desc_tabela, desc_linha, colunas

def resumir(nivel: str, nome: str, filhos: dict, atual: str | None) -> str:
    prompt = f"""Escreva a descrição do {nivel} "{nome}" do Unity Catalog com base no conteúdo abaixo.
Comentário atual: {atual or '(nenhum)'}
Itens contidos e suas descrições:
{json.dumps(filhos, ensure_ascii=False)[:15000]}

Retorne JSON: {{"descricao": "..."}}"""
    return chamar_llm(prompt).get("descricao")

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
    # coalesce: schemas não reprocessados chegam com campos nulos e mantêm o valor que já estava gravado
    set_cols = ", ".join(f"d.{q(f.name)} = coalesce(n.{q(f.name)}, d.{q(f.name)})"
                         for f in schema_resumo.fields[2:])
    spark.sql(f"""
        MERGE INTO {DESTINO} AS d
        USING _doc_resumo AS n
          ON d.catalogo = n.catalogo AND d.`schema` = n.`schema`
        WHEN MATCHED THEN UPDATE SET {set_cols}
    """)

# COMMAND ----------

# ---------------- Varredura ----------------
# Grava ao final de cada catálogo: se cair no meio, o que já rodou fica salvo e é pulado na próxima execução.
catalogos = [c for c in w.catalogs.list()
             if c.name not in CAT_EXCLUIR and (not CAT_INCLUIR or c.name in CAT_INCLUIR)]
print(f"Catálogos a processar: {[c.name for c in catalogos]}")

for cat in catalogos:
    print(f"\n📚 Catálogo {cat.name}")
    linhas, resumos, desc_schemas = [], [], {}
    tabelas_cat = 0
    try:
        schemas = [s for s in w.schemas.list(catalog_name=cat.name)
                   if s.name not in SCHEMA_EXCLUIR and (not SCHEMA_INCLUIR or s.name in SCHEMA_INCLUIR)]
    except Exception as e:
        print(f"   ✗ sem acesso aos schemas: {e}")
        continue

    for sch in schemas:
        if MAX_TAB_CAT > 0 and tabelas_cat >= MAX_TAB_CAT:
            print(f"   ⏭ limite de {MAX_TAB_CAT} tabelas atingido para {cat.name}")
            break
        print(f"  📁 Schema {sch.full_name}")
        try:
            tabelas = list(w.tables.list(catalog_name=cat.name, schema_name=sch.name))
        except Exception as e:
            print(f"   ✗ sem acesso às tabelas: {e}")
            continue

        # contexto do schema: descrições já gravadas + as geradas agora
        desc_tabelas = {t.name: ja_gravadas[t.full_name] for t in tabelas if t.full_name in ja_gravadas}
        novas = 0
        for t in tabelas:
            if t.full_name in ja_gravadas and not REPROCESSAR:
                continue
            if MAX_TAB_CAT > 0 and tabelas_cat >= MAX_TAB_CAT:
                break
            inicio = inicio_operacao()
            try:
                desc_tabela, desc_linha, colunas = documentar_tabela(t)
            except Exception as e:
                print(f"   ✗ {t.full_name}: {e}")
                continue
            m = medir(t.full_name, inicio)
            tabelas_cat += 1
            novas += 1
            desc_tabelas[t.name] = desc_tabela
            agora = datetime.datetime.utcnow()
            tipo_obj = "VIEW" if str(t.table_type).endswith("VIEW") else "TABLE"
            for pos, c in enumerate(colunas, start=1):
                linhas.append(dict(
                    catalogo=cat.name, schema=sch.name, tabela=t.name, caminho_tabela=t.full_name,
                    tipo_objeto=tipo_obj, posicao_coluna=pos, coluna=c["nome"], tipo_coluna=c["tipo"],
                    coluna_particao=c["particao"],
                    descricao_tabela=desc_tabela, descricao_linha=desc_linha, descricao_coluna=c["descricao"],
                    comentario_tabela_atual=t.comment, comentario_coluna_atual=c["comentario_atual"],
                    modelo=MODELO, id_execucao=ID_EXECUCAO, gerado_em=agora,
                    chamadas_llm_tabela=m["chamadas"], tokens_tabela=m["tokens"], custo_tabela_usd=m["custo"]))

        if not desc_tabelas:
            continue
        if novas == 0 and not REPROCESSAR:
            # nada novo neste schema: reaproveita a descrição já gravada (sem custo)
            desc_schemas[sch.name] = spark.sql(f"""
                SELECT max(descricao_schema) AS d FROM {DESTINO}
                WHERE catalogo = '{cat.name}' AND `schema` = '{sch.name}'""").first().d
            continue
        inicio = inicio_operacao()
        desc_schemas[sch.name] = resumir("schema", sch.full_name, desc_tabelas, sch.comment)
        m = medir(sch.full_name, inicio)
        resumos.append(dict(catalogo=cat.name, schema=sch.name, descricao_schema=desc_schemas[sch.name],
                            comentario_schema_atual=sch.comment, tokens_schema=m["tokens"],
                            custo_schema_usd=m["custo"]))

    if not desc_schemas:
        continue
    gravar_linhas(linhas)   # antes do resumo, para o MERGE de schema/catálogo alcançar as linhas novas

    if resumos:
        inicio = inicio_operacao()
        desc_cat = resumir("catálogo", cat.name, desc_schemas, cat.comment)
        m = medir(cat.name, inicio)
        # o resumo do catálogo vale para todos os schemas dele, inclusive os não reprocessados agora
        resumidos = {r["schema"] for r in resumos}
        for s in desc_schemas:
            if s not in resumidos:
                resumos.append(dict(catalogo=cat.name, schema=s, descricao_schema=desc_schemas[s],
                                    comentario_schema_atual=None, tokens_schema=None, custo_schema_usd=None))
        for r in resumos:
            r.update(descricao_catalogo=desc_cat, comentario_catalogo_atual=cat.comment,
                     tokens_catalogo=m["tokens"], custo_catalogo_usd=m["custo"])
        gravar_resumos(resumos)
    print(f"   💾 {len(linhas)} colunas gravadas em {DESTINO}")

print(f"\n💰 Execução {ID_EXECUCAO}: {USO['chamadas']} chamadas | {USO['entrada']:,} tokens entrada | "
      f"{USO['saida']:,} tokens saída | ≈ US$ {custo_usd(USO['entrada'], USO['saida']):.4f}")

# COMMAND ----------

# ---------------- Conferência ----------------
from pyspark.sql import functions as F

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
