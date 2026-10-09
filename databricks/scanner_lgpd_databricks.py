# Databricks notebook source
# MAGIC %md
# MAGIC # Scanner de conformidade LGPD / PCI – Unity Catalog
# MAGIC
# MAGIC Varre **todas as colunas de todas as tabelas de todos os catálogos** e verifica, coluna a coluna:
# MAGIC
# MAGIC | Verificação | Como |
# MAGIC |---|---|
# MAGIC | A coluna guarda dado pessoal, sensível, de cartão ou credencial? | Pelo **nome** da coluna (CPF, RG, e-mail, saúde, biometria, senha, cartão...) e pelo **conteúdo** de uma amostra (CPF/CNPJ com dígito verificador válido, e-mail, cartão com Luhn, CPF dentro de texto livre) |
# MAGIC | O dado está protegido? | Classifica a amostra em: texto puro, hash forte (bcrypt/Argon2), hash simples (MD5/SHA), criptografado/token, mascarado |
# MAGIC | Há controle no Unity Catalog? | Column mask, row filter, tags de classificação e acesso amplo (`account users`) |
# MAGIC | Regras específicas | CVV armazenado (PCI proíbe), cartão legível, senha sem hash adaptativo, dado sensível (art. 11 LGPD) em claro, hash simples de CPF (reversível por força bruta) |
# MAGIC
# MAGIC **Saída:** uma linha por coluna suspeita com `severidade` (CRITICO, ALTO, MEDIO, BAIXO, INFO, OK) e a lista de achados. Opcionalmente grava numa tabela Delta para acompanhar a evolução.
# MAGIC
# MAGIC **Importante**
# MAGIC - Rode com um usuário ou service principal com `BROWSE`/`USE` e `SELECT` em tudo. O `information_schema` só mostra o que o executor pode ver.
# MAGIC - O relatório **nunca grava os valores amostrados**, só estatísticas.
# MAGIC - Se o executor for isento de um column mask (dono/admin), ele vê o dado real, e é isso que o scanner precisa para avaliar o armazenamento.
# MAGIC - É heurístico: nomes de coluna fora do padrão e amostras pequenas podem gerar falso positivo ou negativo. Revise os achados.

# COMMAND ----------

dbutils.widgets.text("catalogos_incluir", "", "01. Catálogos a incluir (vírgula; vazio = todos)")
dbutils.widgets.text("catalogos_excluir", "system,samples,__databricks_internal", "02. Catálogos a excluir")
dbutils.widgets.text("schemas_excluir", "information_schema", "03. Schemas a excluir")
dbutils.widgets.dropdown("amostrar_conteudo", "true", ["true", "false"], "04. Amostrar conteúdo")
dbutils.widgets.dropdown("varrer_todas_strings", "true", ["true", "false"], "05. Procurar PII em toda coluna texto")
dbutils.widgets.text("linhas_amostra", "500", "06. Linhas amostradas por tabela")
dbutils.widgets.text("paralelismo", "8", "07. Tabelas lidas em paralelo")
dbutils.widgets.dropdown("incluir_views", "false", ["true", "false"], "08. Incluir views")
dbutils.widgets.dropdown("incluir_foreign", "false", ["true", "false"], "09. Incluir tabelas federadas")
dbutils.widgets.dropdown("incluir_hive_metastore", "false", ["true", "false"], "10. Incluir hive_metastore")
dbutils.widgets.text("tabela_saida", "", "11. Tabela de saída (catalogo.schema.tabela)")

# COMMAND ----------

import re
import time
import unicodedata
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType, BooleanType)


def _lista(nome):
    return [x.strip() for x in dbutils.widgets.get(nome).split(",") if x.strip()]


CAT_INCLUIR = _lista("catalogos_incluir")
CAT_EXCLUIR = _lista("catalogos_excluir")
SCH_EXCLUIR = _lista("schemas_excluir")
AMOSTRAR = dbutils.widgets.get("amostrar_conteudo") == "true"
VARRER_STRINGS = dbutils.widgets.get("varrer_todas_strings") == "true"
LINHAS_AMOSTRA = int(dbutils.widgets.get("linhas_amostra") or 500)
PARALELISMO = int(dbutils.widgets.get("paralelismo") or 8)
INCLUIR_VIEWS = dbutils.widgets.get("incluir_views") == "true"
INCLUIR_FOREIGN = dbutils.widgets.get("incluir_foreign") == "true"
INCLUIR_HIVE = dbutils.widgets.get("incluir_hive_metastore") == "true"
TABELA_SAIDA = dbutils.widgets.get("tabela_saida").strip()

DATA_SCAN = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras de classificação

# COMMAND ----------

# (categoria, classe, regex sobre o nome normalizado). A primeira que casar vence, então a ordem importa.
# Classes: PCI, CREDENCIAL, SENSIVEL (art. 11 LGPD), PESSOAL, FINANCEIRO, EMPRESA
REGRAS_NOME = [
    ("CVV",              "PCI",        r"(^|_)(cvv|cvc|cvv2|cod_seguranca|codigo_seguranca|security_code)($|_)"),
    ("CARTAO_PAGAMENTO", "PCI",        r"(^|_)(pan|card_number|card_num|cc_number|cc_num|credit_card|cartao_credito|cartao_debito|num_cartao|numero_cartao|nr_cartao)($|_)"),
    ("SENHA",            "CREDENCIAL", r"senha|password|passwd|(^|_)(pwd|pass)($|_)"),
    ("SEGREDO_TOKEN",    "CREDENCIAL", r"secret|segredo|api_key|apikey|access_token|refresh_token|private_key|chave_privada|(^|_)token($|_)"),
    ("CPF",              "PESSOAL",    r"cpf"),
    ("CNPJ",             "EMPRESA",    r"cnpj"),
    ("CNS_SUS",          "SENSIVEL",   r"(^|_)cns($|_)|cartao_sus|cartao_nacional_saude"),
    ("RG",               "PESSOAL",    r"(^|_)(rg|nr_rg|num_rg|numero_rg|registro_geral|identidade|doc_identidade)($|_)"),
    ("CNH",              "PESSOAL",    r"(^|_)cnh($|_)|habilitacao"),
    ("PIS_NIS",          "PESSOAL",    r"(^|_)(pis|pasep|pis_pasep|nis|nit)($|_)"),
    ("TITULO_ELEITOR",   "PESSOAL",    r"titulo_(de_)?eleitor"),
    ("PASSAPORTE",       "PESSOAL",    r"passaporte|passport"),
    ("EMAIL",            "PESSOAL",    r"e_?mail|(^|_)mail($|_)"),
    ("TELEFONE",         "PESSOAL",    r"telefone|celular|phone|whatsapp|(^|_)(fone|tel|mobile)($|_)"),
    ("DATA_NASCIMENTO",  "PESSOAL",    r"nascimento|birth|(^|_)(dt_nasc|dat_nasc|data_nasc|dob)($|_)"),
    ("ENDERECO",         "PESSOAL",    r"endereco|logradouro|address|(^|_)(cep|zip|zipcode|rua|bairro|complemento)($|_)"),
    ("GEOLOCALIZACAO",   "PESSOAL",    r"latitude|longitude|geoloc|(^|_)(lat|lng|lon)($|_)"),
    ("ENDERECO_IP",      "PESSOAL",    r"(^|_)(ip|ip_address|ip_addr|endereco_ip|ip_origem|ip_cliente)($|_)"),
    ("NOME_PESSOA",      "PESSOAL",    r"^(nome|name)$|(^|_)(nome_completo|nome_social|nome_mae|nome_pai|full_name|first_name|last_name|sobrenome)($|_)|(^|_)(nome|name)_(cliente|pessoa|paciente|funcionario|colaborador|usuario|titular|responsavel|contato|aluno|segurado|beneficiario|dependente|socio)($|_)"),
    ("DADOS_BANCARIOS",  "FINANCEIRO", r"(^|_)(agencia|conta_corrente|conta_poupanca|num_conta|nr_conta|numero_conta|iban|chave_pix|account_number)($|_)|^(conta|pix)$"),
    ("RENDA",            "FINANCEIRO", r"salario|renda|remuneracao|salary|income"),
    ("SAUDE",            "SENSIVEL",   r"(^|_)cid(_?10)?($|_)|diagnostic|doenca|saude|health|prontuario|medicament|alergia|patologia|deficiencia|(^|_)exame"),
    ("BIOMETRIA",        "SENSIVEL",   r"biometri|fingerprint|impressao_digital|facial|retina|(^|_)face_|(^|_)iris($|_)"),
    ("RACA_ETNIA",       "SENSIVEL",   r"(^|_)(raca|etnia|cor_raca|race|ethnicity)($|_)"),
    ("RELIGIAO",         "SENSIVEL",   r"religi"),
    ("VIDA_SEXUAL",      "SENSIVEL",   r"orientacao_sexual|sexual_orientation|vida_sexual"),
    ("OPINIAO_POLITICA_SINDICAL", "SENSIVEL", r"partido|politic|sindica|union_member"),
    ("GENETICO",         "SENSIVEL",   r"genetic|genetico|genom|(^|_)dna($|_)"),
]
REGRAS_NOME = [(c, k, re.compile(p)) for c, k, p in REGRAS_NOME]

# Tipos detectáveis pelo conteúdo: tipo -> (categoria, classe, fração mínima da amostra)
DETECCAO_CONTEUDO = {
    "CPF":            ("CPF", "PESSOAL", 0.30),
    "CNPJ":           ("CNPJ", "EMPRESA", 0.30),
    "EMAIL":          ("EMAIL", "PESSOAL", 0.30),
    "CARTAO":         ("CARTAO_PAGAMENTO", "PCI", 0.80),   # Luhn tem ~10% de falso positivo
    "CPF_EM_TEXTO":   ("CPF", "PESSOAL", 0.05),
    "CNPJ_EM_TEXTO":  ("CNPJ", "EMPRESA", 0.05),
    "EMAIL_EM_TEXTO": ("EMAIL", "PESSOAL", 0.05),
}

# Dados de baixa entropia: hash sem chave é revertido por força bruta (CPF tem só 10^9 combinações)
BAIXA_ENTROPIA = {"CPF", "CNPJ", "TELEFONE", "DATA_NASCIMENTO", "ENDERECO", "RG", "PIS_NIS", "CNH", "TITULO_ELEITOR"}

TIPOS_TEXTO = {"STRING", "VARCHAR", "CHAR"}
TIPOS_COMPLEXOS = {"STRUCT", "ARRAY", "MAP", "VARIANT"}

RE_CPF = re.compile(r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$")
RE_CNPJ = re.compile(r"^[A-Za-z0-9]{2}\.?[A-Za-z0-9]{3}\.?[A-Za-z0-9]{3}/?[A-Za-z0-9]{4}-?\d{2}$")  # aceita CNPJ alfanumérico
RE_EMAIL = re.compile(r"^[\w.+-]+@[\w-]+(\.[\w-]+)+$")
RE_TELEFONE = re.compile(r"^(\+?55\s?)?\(?\d{2}\)?[\s-]?9?\d{4}[\s-]?\d{4}$")
RE_CEP = re.compile(r"^\d{5}-?\d{3}$")
RE_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
RE_HASH_FORTE = re.compile(r"^\$(2[abxy]?|argon2(id|i|d)?|scrypt|pbkdf2[\w-]*)\$|^(pbkdf2_sha\d+|argon2|bcrypt)\$")
RE_HASH_HEX = re.compile(r"^(?:[a-fA-F0-9]{32}|[a-fA-F0-9]{40}|[a-fA-F0-9]{64}|[a-fA-F0-9]{128})$")
RE_BASE64 = re.compile(r"^[A-Za-z0-9+/_-]{24,}={0,2}$")
RE_MASCARA = re.compile(r"[\*•#]{2,}|[xX]{3,}")
RE_CPF_NO_TEXTO = re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b")
RE_CNPJ_NO_TEXTO = re.compile(r"\b[A-Z0-9]{2}\.[A-Z0-9]{3}\.[A-Z0-9]{3}/[A-Z0-9]{4}-\d{2}\b")
RE_EMAIL_NO_TEXTO = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de detecção

# COMMAND ----------

def normalizar_nome(nome):
    n = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", nome)  # camelCase -> camel_case
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", n).strip("_")


def classificar_nome(nome):
    n = normalizar_nome(nome)
    for categoria, classe, regex in REGRAS_NOME:
        if regex.search(n):
            return categoria, classe
    return None, None


def cpf_valido(s):
    d = re.sub(r"\D", "", s)
    if len(d) != 11 or d == d[0] * 11:
        return False
    for i in (9, 10):
        soma = sum(int(d[k]) * (i + 1 - k) for k in range(i))
        if (soma * 10 % 11) % 10 != int(d[i]):
            return False
    return True


def cnpj_valido(s):
    v = re.sub(r"[^A-Za-z0-9]", "", s).upper()
    if not re.fullmatch(r"[A-Z0-9]{12}\d{2}", v) or v == v[0] * 14:
        return False
    vals = [ord(c) - 48 for c in v]  # regra oficial do CNPJ alfanumérico (vale também para o numérico)
    for n, pesos in ((12, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]),
                     (13, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])):
        r = sum(a * b for a, b in zip(vals[:n], pesos)) % 11
        if (0 if r < 2 else 11 - r) != vals[n]:
            return False
    return True


def luhn_valido(s):
    d = re.sub(r"[\s-]", "", s)
    if not d.isdigit() or not 13 <= len(d) <= 19 or d[0] not in "3456":
        return False
    total = 0
    for i, ch in enumerate(reversed(d)):
        n = int(ch)
        if i % 2 == 1:
            n = n * 2 - 9 if n * 2 > 9 else n * 2
        total += n
    return total % 10 == 0


def parece_cifrado(s):
    return (bool(RE_BASE64.fullmatch(s)) and any(c.isdigit() for c in s)
            and any(c.isupper() for c in s) and any(c.islower() for c in s))


def classificar_valor(v):
    s = str(v).strip()
    if not s:
        return None
    if RE_HASH_FORTE.match(s):
        return "HASH_FORTE"
    if RE_MASCARA.search(s):
        return "MASCARADO"
    if RE_CPF.fullmatch(s) and cpf_valido(s):
        return "CPF"
    if RE_CNPJ.fullmatch(s) and cnpj_valido(s):
        return "CNPJ"
    if luhn_valido(s):
        return "CARTAO"
    if RE_EMAIL.fullmatch(s):
        return "EMAIL"
    if RE_HASH_HEX.fullmatch(s):
        return "HASH"
    if RE_UUID.fullmatch(s):
        return "OUTRO"
    if parece_cifrado(s):
        return "CIFRADO"
    if RE_TELEFONE.fullmatch(s):
        return "TELEFONE"
    if RE_CEP.fullmatch(s):
        return "CEP"
    if len(s) > 25:
        m = RE_CPF_NO_TEXTO.search(s)
        if m and cpf_valido(m.group()):
            return "CPF_EM_TEXTO"
        m = RE_CNPJ_NO_TEXTO.search(s)
        if m and cnpj_valido(m.group()):
            return "CNPJ_EM_TEXTO"
        if RE_EMAIL_NO_TEXTO.search(s):
            return "EMAIL_EM_TEXTO"
    return "OUTRO"


STATUS_PROTEGIDO = {"HASH_FORTE": "HASH_FORTE", "HASH": "HASH",
                    "CIFRADO": "CRIPTOGRAFADO_OU_TOKEN", "MASCARADO": "MASCARADO"}


def analisar_texto(valores, categoria_nome):
    """Retorna (categoria_conteudo, classe_conteudo, n, pct_texto, pct_protegido, status)."""
    tipos = Counter(classificar_valor(v) for v in valores)
    tipos.pop(None, None)
    n = sum(tipos.values())
    if n == 0:
        return None, None, 0, None, None, "SEM_DADOS"

    pct_prot = sum(tipos[k] for k in STATUS_PROTEGIDO) / n

    cat_cont, classe_cont, pct_cat = None, None, 0.0
    for tipo, (cat, classe, minimo) in DETECCAO_CONTEUDO.items():
        r = tipos[tipo] / n
        if r >= minimo and r > pct_cat:
            cat_cont, classe_cont, pct_cat = cat, classe, r

    # Coluna com nome de PII: tudo que não está protegido conta como texto puro.
    pct_texto = (1 - pct_prot) if categoria_nome else pct_cat

    if pct_prot >= 0.8:
        status = STATUS_PROTEGIDO[max(STATUS_PROTEGIDO, key=lambda k: tipos[k])]
    elif pct_texto >= 0.05 and pct_prot >= 0.05:
        status = "MISTO"
    elif pct_texto > 0:
        status = "TEXTO_PURO"
    else:
        status = "INDETERMINADO"
    return cat_cont, classe_cont, n, round(pct_texto, 4), round(pct_prot, 4), status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras de conformidade (severidade)

# COMMAND ----------

ORDEM = ["OK", "INFO", "BAIXO", "MEDIO", "ALTO", "CRITICO"]


def avaliar(categoria, classe, status, tem_mask, tem_tag, acesso_amplo):
    achados, sev = [], "OK"

    def sobe(nivel, msg):
        nonlocal sev
        achados.append(f"[{nivel}] {msg}")
        if ORDEM.index(nivel) > ORDEM.index(sev):
            sev = nivel

    texto = status in ("TEXTO_PURO", "MISTO")
    if status == "MISTO":
        achados.append("Parte da amostra protegida e parte em claro (migração incompleta?)")

    if status == "SEM_DADOS":
        sobe("INFO", "Coluna sem dados na amostra; reavaliar quando houver carga.")
    elif status == "ERRO_LEITURA":
        sobe("MEDIO", "Não foi possível ler a tabela para validar o conteúdo; conferir permissões.")
    elif status in ("INDETERMINADO", "TIPO_COMPLEXO", "NAO_AMOSTRADO"):
        sobe("MEDIO", f"Proteção não verificável ({status}); revisar manualmente.")

    if categoria == "CVV" and status != "SEM_DADOS":
        sobe("CRITICO", "PCI DSS proíbe armazenar CVV/CVC após a autorização, mesmo criptografado. Remover a coluna.")
    elif categoria == "CARTAO_PAGAMENTO":
        if texto:
            sobe("CRITICO", "Número de cartão (PAN) legível. PCI DSS exige tokenização, criptografia forte ou truncamento.")
        elif status == "HASH":
            sobe("MEDIO", "PAN com hash simples; PCI DSS exige hash com chave (keyed hash) ou tokenização.")
    elif categoria == "SENHA":
        if texto:
            sobe("CRITICO", "Senha em texto puro. Usar hash adaptativo com salt (bcrypt, Argon2, scrypt ou PBKDF2).")
        elif status == "HASH":
            sobe("ALTO", "Senha com hash rápido (MD5/SHA). Migrar para bcrypt ou Argon2.")
        elif status == "CRIPTOGRAFADO_OU_TOKEN":
            sobe("MEDIO", "Senha parece criptografada (reversível). Senha deve ser hash, não criptografia.")
    elif categoria == "SEGREDO_TOKEN":
        if texto:
            sobe("ALTO", "Segredo/token legível. Guardar em Databricks Secrets / Key Vault ou criptografar.")
    elif classe == "SENSIVEL":
        if texto:
            if tem_mask:
                sobe("ALTO", "Dado sensível (art. 11 LGPD) em claro no armazenamento; há column mask, mas falta criptografia.")
            else:
                sobe("CRITICO", "Dado sensível (art. 11 LGPD) em claro e sem column mask.")
    elif classe in ("PESSOAL", "FINANCEIRO"):
        if texto:
            if tem_mask:
                sobe("MEDIO", "Dado pessoal em claro no armazenamento; column mask mitiga a leitura.")
            else:
                sobe("ALTO", "Dado pessoal em claro e sem column mask (art. 46 LGPD: medidas de segurança).")
        elif status == "HASH" and categoria in BAIXA_ENTROPIA:
            sobe("BAIXO", "Hash de dado de baixa entropia é reversível por força bruta se não tiver chave secreta. Confirmar que é HMAC/hash com pepper.")
    elif classe == "EMPRESA":
        if texto:
            sobe("BAIXO", "CNPJ em claro. Em geral não é dado pessoal, mas MEI/empresário individual identifica a pessoa. Avaliar.")

    if texto and acesso_amplo:
        novo = ORDEM[min(ORDEM.index(sev) + 1, len(ORDEM) - 1)]
        sobe(novo, "Tabela com SELECT para 'account users'/'users' (acesso amplo).")

    if not tem_tag and classe != "EMPRESA":
        sobe("BAIXO", "Sem tag de classificação no Unity Catalog (ex.: pii / lgpd).")

    if not achados:
        achados.append("Proteção aparente OK.")
    return sev, achados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadados (Unity Catalog)

# COMMAND ----------

def q(nome):
    return "`" + nome.replace("`", "``") + "`"


def sql_in(valores):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in valores)


filtros = []
if SCH_EXCLUIR:
    filtros.append(f"c.table_schema NOT IN ({sql_in(SCH_EXCLUIR)})")
if CAT_INCLUIR:
    filtros.append(f"c.table_catalog IN ({sql_in(CAT_INCLUIR)})")
if CAT_EXCLUIR:
    filtros.append(f"c.table_catalog NOT IN ({sql_in(CAT_EXCLUIR)})")
where = ("WHERE " + " AND ".join(filtros)) if filtros else ""

t0 = time.time()
metadados = [r.asDict() for r in spark.sql(f"""
    SELECT c.table_catalog AS catalogo, c.table_schema AS esquema, c.table_name AS tabela,
           c.column_name AS coluna, upper(c.data_type) AS tipo, t.table_type AS tipo_tabela
    FROM system.information_schema.columns c
    JOIN system.information_schema.tables t
      ON c.table_catalog = t.table_catalog
     AND c.table_schema  = t.table_schema
     AND c.table_name    = t.table_name
    {where}
""").collect()]
catalogos = sorted({m["catalogo"] for m in metadados})
print(f"Unity Catalog: {len(metadados):,} colunas em {len(catalogos)} catálogos ({time.time() - t0:.0f}s)")


def ler_info_schema(view, colunas):
    """Lê do system.information_schema; se não existir, junta o information_schema de cada catálogo."""
    try:
        return spark.sql(f"SELECT {colunas} FROM system.information_schema.{view}").collect()
    except Exception:
        linhas = []
        for cat in catalogos:
            try:
                linhas += spark.sql(f"SELECT {colunas} FROM {q(cat)}.information_schema.{view}").collect()
            except Exception:
                pass
        if not linhas:
            print(f"⚠️  Nada lido de information_schema.{view} (sem permissão ou recurso indisponível).")
        return linhas


mascaras = {
    (r.table_catalog, r.table_schema, r.table_name, r.column_name): f"{r.mask_catalog}.{r.mask_schema}.{r.mask_name}"
    for r in ler_info_schema("column_masks",
                             "table_catalog, table_schema, table_name, column_name, mask_catalog, mask_schema, mask_name")
}
com_row_filter = {
    (r.table_catalog, r.table_schema, r.table_name)
    for r in ler_info_schema("row_filters", "table_catalog, table_schema, table_name")
}
tags = defaultdict(list)
for r in ler_info_schema("column_tags", "catalog_name, schema_name, table_name, column_name, tag_name, tag_value"):
    tags[(r.catalog_name, r.schema_name, r.table_name, r.column_name)].append(
        f"{r.tag_name}={r.tag_value}" if r.tag_value else r.tag_name)
acesso_amplo = {
    (r.table_catalog, r.table_schema, r.table_name)
    for r in ler_info_schema("table_privileges", "table_catalog, table_schema, table_name, grantee, privilege_type")
    if r.grantee in ("account users", "users") and r.privilege_type in ("SELECT", "ALL PRIVILEGES")
}
print(f"Column masks: {len(mascaras)} | Tabelas com row filter: {len(com_row_filter)} | "
      f"Colunas com tag: {len(tags)} | Tabelas com acesso amplo: {len(acesso_amplo)}")

# COMMAND ----------

# Opcional: hive_metastore (fora do Unity Catalog, sem masks/tags)
if INCLUIR_HIVE and "hive_metastore" not in CAT_EXCLUIR:
    n_antes = len(metadados)
    try:
        bancos = [r[0] for r in spark.sql("SHOW SCHEMAS IN hive_metastore").collect()]
    except Exception as e:
        bancos = []
        print(f"⚠️  hive_metastore indisponível: {str(e)[:200]}")
    for db in bancos:
        if db in SCH_EXCLUIR:
            continue
        try:
            tabs = [t for t in spark.sql(f"SHOW TABLES IN hive_metastore.{q(db)}").collect() if not t.isTemporary]
        except Exception:
            continue
        for t in tabs:
            try:
                for c in spark.sql(f"DESCRIBE TABLE hive_metastore.{q(db)}.{q(t.tableName)}").collect():
                    if not c.col_name or c.col_name.startswith("#"):
                        break  # início da seção de partições
                    metadados.append({"catalogo": "hive_metastore", "esquema": db, "tabela": t.tableName,
                                      "coluna": c.col_name, "tipo": c.data_type.upper(), "tipo_tabela": "HIVE"})
            except Exception:
                pass
    print(f"hive_metastore: +{len(metadados) - n_antes:,} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Planejamento e varredura

# COMMAND ----------

def tipo_base(tipo):
    return (tipo or "").upper().split("(")[0].split("<")[0].strip()


tabelas = defaultdict(list)
tipo_tabela = {}
for m in metadados:
    tt = (m["tipo_tabela"] or "").upper()
    if "VIEW" in tt and not INCLUIR_VIEWS:
        continue
    if tt == "FOREIGN" and not INCLUIR_FOREIGN:
        continue

    base = tipo_base(m["tipo"])
    categoria, classe = classificar_nome(m["coluna"])
    if base in TIPOS_COMPLEXOS:
        modo = "COMPLEXO" if categoria else None
    elif base in TIPOS_TEXTO:
        modo = "TEXTO" if (categoria or VARRER_STRINGS) else None
    elif base == "BINARY":
        modo = "BINARIO" if categoria else None
    else:
        modo = "NATIVO" if categoria else None  # número/data com nome de PII: não tem como estar cifrado
    if modo is None or (modo == "TEXTO" and not categoria and not AMOSTRAR):
        continue

    chave = (m["catalogo"], m["esquema"], m["tabela"])
    tipo_tabela[chave] = m["tipo_tabela"]
    tabelas[chave].append({"coluna": m["coluna"], "tipo": m["tipo"], "modo": modo,
                           "categoria": categoria, "classe": classe})

print(f"Tabelas a analisar: {len(tabelas):,} | Colunas candidatas: {sum(len(v) for v in tabelas.values()):,}")


def montar_linha(chave, col, categoria, classe, origem, n_amostra, n, pct_texto, pct_prot, status, erro=None):
    cat, sch, tbl = chave
    k_col = (cat, sch, tbl, col["coluna"])
    tem_mask = k_col in mascaras
    col_tags = tags.get(k_col, [])
    amplo = chave in acesso_amplo
    sev, achados = avaliar(categoria, classe, status, tem_mask, bool(col_tags), amplo)
    return {
        "data_scan": DATA_SCAN, "catalogo": cat, "esquema": sch, "tabela": tbl, "coluna": col["coluna"],
        "tipo_dado": col["tipo"], "tipo_tabela": tipo_tabela.get(chave), "categoria": categoria,
        "classe_lgpd": classe, "origem_deteccao": origem, "linhas_amostradas": n_amostra,
        "valores_nao_nulos": n, "pct_texto_puro": pct_texto, "pct_protegido": pct_prot,
        "status_protecao": status, "tem_column_mask": tem_mask, "column_mask": mascaras.get(k_col),
        "tem_row_filter": chave in com_row_filter, "tags": ", ".join(col_tags) or None,
        "acesso_amplo": amplo, "severidade": sev, "severidade_ordem": ORDEM.index(sev),
        "achados": " | ".join(achados), "erro": erro,
    }


def escanear_tabela(chave, colunas):
    fq = ".".join(q(p) for p in chave)
    amostrar = [c for c in colunas if c["modo"] in ("TEXTO", "BINARIO", "NATIVO")] if AMOSTRAR else []
    linhas, erro = [], None
    if amostrar:
        try:
            exprs = [F.col(q(c["coluna"])).cast("string").alias(f"c{i}") for i, c in enumerate(amostrar)]
            linhas = spark.table(fq).select(*exprs).limit(LINHAS_AMOSTRA).collect()
        except Exception as e:
            erro = str(e).split("\n")[0][:300]

    valores = {c["coluna"]: [r[i] for r in linhas if r[i] is not None] for i, c in enumerate(amostrar)}
    resultado = []
    for col in colunas:
        cat_nome, classe_nome = col["categoria"], col["classe"]

        if col["modo"] == "COMPLEXO":
            resultado.append(montar_linha(chave, col, cat_nome, classe_nome, "NOME", None, None, None, None, "TIPO_COMPLEXO"))
            continue
        if not AMOSTRAR:
            status = {"NATIVO": "TEXTO_PURO", "BINARIO": "CRIPTOGRAFADO_OU_TOKEN"}.get(col["modo"], "NAO_AMOSTRADO")
            resultado.append(montar_linha(chave, col, cat_nome, classe_nome, "NOME", None, None, None, None, status))
            continue
        if erro:
            if cat_nome:
                resultado.append(montar_linha(chave, col, cat_nome, classe_nome, "NOME", None, None, None, None,
                                              "ERRO_LEITURA", erro))
            continue

        vals = valores.get(col["coluna"], [])
        if col["modo"] in ("NATIVO", "BINARIO"):
            n = len(vals)
            status = "SEM_DADOS" if n == 0 else ("TEXTO_PURO" if col["modo"] == "NATIVO" else "CRIPTOGRAFADO_OU_TOKEN")
            pct_t = None if n == 0 else (1.0 if col["modo"] == "NATIVO" else 0.0)
            resultado.append(montar_linha(chave, col, cat_nome, classe_nome, "NOME", len(linhas), n,
                                          pct_t, None if n == 0 else 1.0 - pct_t, status))
            continue

        cat_c, classe_c, n, pct_t, pct_p, status = analisar_texto(vals, cat_nome)
        if not cat_nome and not cat_c:
            continue  # coluna texto comum, nada encontrado
        categoria = cat_nome or cat_c
        classe = classe_nome or classe_c
        origem = "NOME+CONTEUDO" if (cat_nome and cat_c) else ("NOME" if cat_nome else "CONTEUDO")
        resultado.append(montar_linha(chave, col, categoria, classe, origem, len(linhas), n, pct_t, pct_p, status))
    return resultado


t0 = time.time()
resultados, feitas = [], 0
with ThreadPoolExecutor(max_workers=PARALELISMO) as pool:
    futuros = {pool.submit(escanear_tabela, k, v): k for k, v in tabelas.items()}
    for f in as_completed(futuros):
        try:
            resultados.extend(f.result())
        except Exception as e:
            print(f"⚠️  Falha em {'.'.join(futuros[f])}: {str(e)[:200]}")
        feitas += 1
        if feitas % 100 == 0 or feitas == len(futuros):
            print(f"  {feitas:,}/{len(futuros):,} tabelas ({time.time() - t0:.0f}s)")

print(f"Concluído: {len(resultados):,} colunas com achados em {time.time() - t0:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado

# COMMAND ----------

ESQUEMA = StructType([
    StructField("data_scan", TimestampType()), StructField("catalogo", StringType()),
    StructField("esquema", StringType()), StructField("tabela", StringType()),
    StructField("coluna", StringType()), StructField("tipo_dado", StringType()),
    StructField("tipo_tabela", StringType()), StructField("categoria", StringType()),
    StructField("classe_lgpd", StringType()), StructField("origem_deteccao", StringType()),
    StructField("linhas_amostradas", IntegerType()), StructField("valores_nao_nulos", IntegerType()),
    StructField("pct_texto_puro", DoubleType()), StructField("pct_protegido", DoubleType()),
    StructField("status_protecao", StringType()), StructField("tem_column_mask", BooleanType()),
    StructField("column_mask", StringType()), StructField("tem_row_filter", BooleanType()),
    StructField("tags", StringType()), StructField("acesso_amplo", BooleanType()),
    StructField("severidade", StringType()), StructField("severidade_ordem", IntegerType()),
    StructField("achados", StringType()), StructField("erro", StringType()),
])
df = spark.createDataFrame(resultados, ESQUEMA).orderBy(
    F.desc("severidade_ordem"), "catalogo", "esquema", "tabela", "coluna")
df.createOrReplaceTempView("lgpd_scan")

# COMMAND ----------

# Resumo por severidade
display(spark.sql("""
    SELECT severidade, count(*) AS colunas, count(DISTINCT catalogo, esquema, tabela) AS tabelas
    FROM lgpd_scan GROUP BY severidade, severidade_ordem ORDER BY severidade_ordem DESC
"""))

# COMMAND ----------

# Resumo por catálogo e categoria
display(spark.sql("""
    SELECT catalogo, categoria, classe_lgpd,
           count(*) AS colunas,
           sum(CASE WHEN severidade IN ('CRITICO','ALTO') THEN 1 ELSE 0 END) AS criticas_ou_altas,
           sum(CASE WHEN status_protecao IN ('TEXTO_PURO','MISTO') THEN 1 ELSE 0 END) AS em_claro,
           sum(CASE WHEN tem_column_mask THEN 1 ELSE 0 END) AS com_mask
    FROM lgpd_scan GROUP BY ALL ORDER BY criticas_ou_altas DESC, colunas DESC
"""))

# COMMAND ----------

# Detalhe: tudo que é CRITICO, ALTO ou MEDIO
display(df.filter(F.col("severidade_ordem") >= ORDEM.index("MEDIO")))

# COMMAND ----------

# Relatório completo
display(df)

# COMMAND ----------

if TABELA_SAIDA:
    (df.write.mode("append").option("mergeSchema", "true").saveAsTable(TABELA_SAIDA))
    print(f"Resultado gravado em {TABELA_SAIDA} (data_scan = {DATA_SCAN.isoformat()})")
else:
    print("Tabela de saída não informada; resultado disponível só na view temporária 'lgpd_scan'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como corrigir (exemplos)
# MAGIC
# MAGIC ```sql
# MAGIC -- 1) Column mask: só um grupo vê o CPF completo
# MAGIC CREATE OR REPLACE FUNCTION seguranca.lgpd.mascara_cpf(cpf STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('lgpd_leitura_completa') THEN cpf
# MAGIC             ELSE concat('***.', substr(regexp_replace(cpf, '[^0-9]', ''), 4, 3), '.***-**') END;
# MAGIC
# MAGIC ALTER TABLE meu_catalogo.vendas.clientes ALTER COLUMN cpf SET MASK seguranca.lgpd.mascara_cpf;
# MAGIC
# MAGIC -- 2) Tag de classificação
# MAGIC ALTER TABLE meu_catalogo.vendas.clientes ALTER COLUMN cpf SET TAGS ('pii' = 'cpf', 'lgpd' = 'pessoal');
# MAGIC
# MAGIC -- 3) Criptografia em coluna + chave de busca com segredo (pepper), chaves no Databricks Secrets
# MAGIC SELECT base64(aes_encrypt(cpf, secret('lgpd', 'chave_aes_32_bytes'))) AS cpf_cifrado,
# MAGIC        sha2(concat(secret('lgpd', 'pepper'), regexp_replace(cpf, '[^0-9]', '')), 256) AS cpf_busca
# MAGIC FROM meu_catalogo.vendas.clientes_raw;
# MAGIC
# MAGIC -- 4) Tirar acesso amplo
# MAGIC REVOKE SELECT ON TABLE meu_catalogo.vendas.clientes FROM `account users`;
# MAGIC ```
# MAGIC
# MAGIC Senhas: não use `sha2` nem `aes_encrypt`. Faça hash com bcrypt/Argon2 na aplicação antes de gravar. CVV: apague a coluna.
