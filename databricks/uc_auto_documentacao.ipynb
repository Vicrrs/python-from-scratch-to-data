# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-documentação do Unity Catalog com IA
# MAGIC
# MAGIC Percorre **catálogos → schemas → tabelas → colunas**, analisa metadados + amostra de dados
# MAGIC com um LLM servido no Databricks (Foundation Model API) e grava as descrições como
# MAGIC `COMMENT` no Unity Catalog.
# MAGIC
# MAGIC **Fluxo (bottom-up):**
# MAGIC 1. Para cada tabela: lê colunas, tipos, comentários existentes e uma amostra de linhas → LLM gera descrição da tabela e de cada coluna.
# MAGIC 2. Para cada schema: LLM resume as descrições das tabelas.
# MAGIC 3. Para cada catálogo: LLM resume as descrições dos schemas.
# MAGIC 4. Tudo é registrado numa tabela Delta de log (auditoria / revisão humana) e, se `dry_run = false`, aplicado via `COMMENT ON`.
# MAGIC
# MAGIC **Permissões necessárias:** `USE CATALOG`, `USE SCHEMA`, `SELECT` (para amostrar) e ser *owner* ou ter `MODIFY` nos objetos para alterar comentários.
# MAGIC
# MAGIC **Recomendação:** rode primeiro com `dry_run = true`, revise a tabela de log e só depois aplique.

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk openai
# MAGIC %restart_python

# COMMAND ----------

# ---------------- Parâmetros ----------------
dbutils.widgets.text("catalogos_incluir", "", "Catálogos a incluir (vírgula; vazio = todos)")
dbutils.widgets.text("catalogos_excluir", "system,samples,hive_metastore,__databricks_internal", "Catálogos a excluir")
dbutils.widgets.text("schemas_excluir", "information_schema", "Schemas a excluir")
dbutils.widgets.text("modelo_endpoint", "databricks-claude-sonnet-4", "Serving endpoint do LLM")
dbutils.widgets.text("idioma", "português do Brasil", "Idioma das descrições")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (só loga, não aplica)")
dbutils.widgets.dropdown("sobrescrever", "false", ["true", "false"], "Sobrescrever comentários existentes")
dbutils.widgets.text("linhas_amostra", "5", "Linhas de amostra por tabela")
dbutils.widgets.text("tabela_log", "main.governanca.uc_auto_doc_log", "Tabela Delta de log")

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
TABELA_LOG     = dbutils.widgets.get("tabela_log")

MAX_COLS_POR_CHAMADA = 60     # tabelas largas são documentadas em lotes de colunas
MAX_CHARS_VALOR      = 80     # trunca valores longos da amostra
print(f"dry_run={DRY_RUN} | sobrescrever={SOBRESCREVER} | modelo={MODELO}")

# COMMAND ----------

import json, re, time, datetime
from databricks.sdk import WorkspaceClient

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

    precisa_tabela = SOBRESCREVER or not t.comment
    precisa_cols = [c for c in colunas if SOBRESCREVER or not c["comentario_atual"]]
    if not precisa_tabela and not precisa_cols:
        print(f"   ✓ {t.full_name}: já documentada")
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
    tipo_obj = "VIEW" if str(t.table_type).endswith("VIEW") else "TABLE"
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
    try:
        schemas = [s for s in w.schemas.list(catalog_name=cat.name) if s.name not in SCHEMA_EXCLUIR]
    except Exception as e:
        print(f"   ✗ sem acesso aos schemas: {e}")
        continue

    for sch in schemas:
        print(f"  📁 Schema {sch.full_name}")
        desc_tabelas = {}
        try:
            tabelas = list(w.tables.list(catalog_name=cat.name, schema_name=sch.name))
        except Exception as e:
            print(f"   ✗ sem acesso às tabelas: {e}")
            continue

        for t in tabelas:
            try:
                desc_tabelas[t.name] = documentar_tabela(t)
            except Exception as e:
                print(f"   ✗ {t.full_name}: {e}")
                registrar("tabela", t.full_name, None, t.comment, None, f"erro: {str(e)[:300]}")

        if tabelas and (SOBRESCREVER or not sch.comment):
            d = resumir("schema", sch.full_name, desc_tabelas, sch.comment)
            aplicar(f"COMMENT ON SCHEMA {q(cat.name)}.{q(sch.name)} IS {literal(d)}",
                    "schema", sch.full_name, None, sch.comment, d)
            desc_schemas[sch.name] = d
        else:
            desc_schemas[sch.name] = sch.comment

    if schemas and (SOBRESCREVER or not cat.comment):
        d = resumir("catálogo", cat.name, desc_schemas, cat.comment)
        aplicar(f"COMMENT ON CATALOG {q(cat.name)} IS {literal(d)}",
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
        except Exception as e:
            print(f"✗ {r.objeto} {r.coluna or ''}: {e}")
    print(f"{len(pend)} comentários aplicados.")

# aplicar_do_log("2026-10-07")