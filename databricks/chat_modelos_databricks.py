# Databricks notebook source
# MAGIC %md
# MAGIC # Chat com os modelos do Databricks
# MAGIC Lista os endpoints de **LLM de chat** do Model Serving que você tem permissão para usar
# MAGIC (Foundation Models `databricks-*`, modelos externos e endpoints custom) e abre um chat no próprio notebook.
# MAGIC
# MAGIC - A descoberta usa a mesma lógica do chat-especialistas: `serving_endpoints.list()` e filtro pela `task` do endpoint.
# MAGIC - As chamadas vão por HTTP direto em `/serving-endpoints/<nome>/invocations` com o **seu** token (capturado ao rodar a célula).
# MAGIC - Ao rodar, testa um modelo padrão e já deixa selecionado o primeiro que responder.
# MAGIC - O histórico fica em memória: trocar de modelo no meio da conversa continua a mesma conversa (útil para comparar).
# MAGIC - Sem interface? Use `perguntar("...")` numa célula qualquer (última célula).
# MAGIC
# MAGIC **Use "Run all"**.

# COMMAND ----------

# Descomente se o runtime não tiver ipywidgets / databricks-sdk recente
# %pip install -U databricks-sdk ipywidgets
# dbutils.library.restartPython()

# COMMAND ----------

# ---------------- Descoberta dos modelos de chat ----------------
import time
from dataclasses import dataclass

import requests
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # só para listar os endpoints (roda dentro da célula, então a auth automática funciona)

# Os botões do ipywidgets rodam FORA da execução de uma célula: lá a auth automática do notebook falha com
# "Missing api_token from command execution". Por isso o host/token são capturados AGORA e as chamadas ao
# modelo são feitas por HTTP direto (requests), sem depender do contexto do notebook.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = _ctx.apiUrl().get().rstrip("/")
TOKEN = _ctx.apiToken().get()


@dataclass
class ModeloIA:
    nome: str
    origem: str  # "foundation" | "externo" | "custom"
    pronto: bool
    detalhe: str = ""

    @property
    def rotulo(self) -> str:
        status = "" if self.pronto else " (não pronto)"
        extra = f" · {self.detalhe}" if self.detalhe else ""
        return f"{self.nome} [{self.origem}{extra}]{status}"


def _valor(enum_ou_str) -> str:
    return str(getattr(enum_ou_str, "value", enum_ou_str) or "")


def _eh_chat(endpoint) -> bool:
    t = (endpoint.task or "").lower()
    return "chat" in t or "completions" in t


def _origem(endpoint) -> tuple[str, str]:
    entidades = (endpoint.config.served_entities if endpoint.config else None) or []
    for e in entidades:
        if e.external_model is not None:
            prov = _valor(getattr(e.external_model, "provider", "")) or "externo"
            return "externo", f"{prov}: {getattr(e.external_model, 'name', '')}"
        if e.foundation_model is not None:
            return "foundation", getattr(e.foundation_model, "display_name", "") or ""
        if e.entity_name:
            if e.entity_name.startswith("system.ai."):
                return "foundation", e.entity_name
            return "custom", e.entity_name
    if (endpoint.name or "").startswith("databricks-"):
        return "foundation", ""
    return "custom", ""


def listar_modelos_chat() -> list[ModeloIA]:
    modelos = []
    for ep in w.serving_endpoints.list():
        if not _eh_chat(ep):
            continue
        origem, detalhe = _origem(ep)
        pronto = _valor(ep.state.ready if ep.state else "") == "READY"
        modelos.append(ModeloIA(ep.name, origem, pronto, detalhe))
    # prontos primeiro, depois foundation, depois por nome
    return sorted(modelos, key=lambda m: (not m.pronto, m.origem != "foundation", m.nome))


# ---------------- Chamada ao modelo (HTTP direto) ----------------
def _texto(conteudo) -> str:
    """Alguns modelos (ex.: Claude, reasoning) devolvem lista de blocos em vez de string."""
    if isinstance(conteudo, list):
        partes = []
        for p in conteudo:
            if isinstance(p, dict):
                if p.get("type") in ("reasoning", "thinking"):
                    continue  # não mostra o raciocínio interno
                partes.append(p.get("text", ""))
            else:
                partes.append(str(p))
        return "".join(partes)
    return conteudo or ""


def _post(endpoint: str, corpo: dict, timeout: int) -> requests.Response:
    return requests.post(f"{HOST}/serving-endpoints/{endpoint}/invocations",
                         headers={"Authorization": f"Bearer {TOKEN}"}, json=corpo, timeout=timeout)


def chamar_modelo(endpoint: str, historico: list[dict], temperatura: float | None, max_tokens: int,
                  timeout: int = 300) -> dict:
    """historico = [{"role": "system"|"user"|"assistant", "content": str}, ...]"""
    corpo = {"messages": [{"role": m["role"], "content": m["content"]} for m in historico], "max_tokens": max_tokens}
    if temperatura is not None:
        corpo["temperature"] = temperatura
    inicio = time.time()
    r = _post(endpoint, corpo, timeout)
    if r.status_code == 400 and "temperature" in r.text.lower():
        corpo.pop("temperature")  # modelos de raciocínio costumam recusar temperature
        r = _post(endpoint, corpo, timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
    dados = r.json()
    escolhas = dados.get("choices") or []
    conteudo = escolhas[0].get("message", {}).get("content") if escolhas else ""
    uso = dados.get("usage") or {}
    return {
        "texto": _texto(conteudo),
        "segundos": time.time() - inicio,
        "tokens_in": uso.get("prompt_tokens"),
        "tokens_out": uso.get("completion_tokens"),
    }


# ---------------- Lista e escolhe um padrão que responde ----------------
MODELOS = listar_modelos_chat()
assert MODELOS, "Nenhum endpoint de LLM de chat disponível para você. Verifique as permissões em Serving."

print(f"{len(MODELOS)} modelos de chat encontrados:")
for m in MODELOS:
    print(" -", m.rotulo)

# Preferências para o padrão; depois qualquer foundation pronto. Testa com uma chamada mínima e fica com o 1º que responde.
PREFERIDOS = ["llama-3-3-70b", "claude-sonnet", "gpt-oss-120b", "llama-4-maverick", "gemma"]
candidatos = [m.nome for p in PREFERIDOS for m in MODELOS if p in m.nome and m.pronto]
candidatos += [m.nome for m in MODELOS if m.pronto and m.origem == "foundation" and m.nome not in candidatos]
candidatos += [m.nome for m in MODELOS if m.nome not in candidatos]

PADRAO = None
for nome in candidatos[:8]:
    try:
        chamar_modelo(nome, [{"role": "user", "content": "Responda só: ok"}], None, 16, timeout=60)
        PADRAO = nome
        break
    except Exception as e:  # noqa: BLE001
        print(f"   ✗ {nome}: {str(e)[:150]}")
PADRAO = PADRAO or MODELOS[0].nome
print(f"\nModelo padrão (testado): {PADRAO}")

# COMMAND ----------

# ---------------- Interface de chat (ipywidgets) ----------------
import html

import ipywidgets as widgets
from IPython.display import display

try:
    import markdown as _md

    def para_html(txt: str) -> str:
        return _md.markdown(txt, extensions=["fenced_code", "tables"])
except ImportError:
    def para_html(txt: str) -> str:
        return f"<pre style='white-space:pre-wrap;font-family:inherit;margin:0'>{html.escape(txt)}</pre>"


SISTEMA_PADRAO = "Você é um assistente útil. Responda em português do Brasil, de forma clara e objetiva."
estado = {"historico": [], "tokens_in": 0, "tokens_out": 0}

# --- controles
dd_modelo = widgets.Dropdown(
    options=[(m.rotulo, m.nome) for m in MODELOS], value=PADRAO, description="Modelo",
    layout=widgets.Layout(width="70%"), style={"description_width": "70px"},
)
btn_atualizar = widgets.Button(description="🔄 Atualizar modelos", layout=widgets.Layout(width="180px"))
sl_temp = widgets.FloatSlider(value=0.2, min=0.0, max=1.5, step=0.1, description="Temperatura",
                              style={"description_width": "90px"})
it_max = widgets.BoundedIntText(value=2000, min=64, max=32000, step=256, description="Max tokens",
                                style={"description_width": "90px"}, layout=widgets.Layout(width="220px"))
ta_sistema = widgets.Textarea(value=SISTEMA_PADRAO, description="System",
                              layout=widgets.Layout(width="100%", height="60px"), style={"description_width": "70px"})

# --- conversa
caixa_chat = widgets.HTML(layout=widgets.Layout(
    width="100%", height="480px", overflow_y="auto", border="1px solid #ccc", padding="8px"))
ta_pergunta = widgets.Textarea(placeholder="Digite sua mensagem…", layout=widgets.Layout(width="100%", height="80px"))
btn_enviar = widgets.Button(description="Enviar", button_style="primary", icon="paper-plane")
btn_limpar = widgets.Button(description="Limpar conversa", icon="trash")
lbl_status = widgets.HTML()

ESTILO = {
    "user": "background:#e8f0fe;margin-left:15%;",
    "assistant": "background:#f4f4f4;margin-right:15%;",
    "erro": "background:#fdecea;color:#a12;margin-right:15%;",
}


def renderizar(pendente: bool = False):
    blocos = []
    for m in estado["historico"]:
        if m["role"] == "system":
            continue
        tipo = m.get("tipo", m["role"])
        quem = "Você" if m["role"] == "user" else f"🤖 {m.get('modelo', '')}"
        rodape = f"<div style='font-size:11px;color:#888;margin-top:4px'>{m['meta']}</div>" if m.get("meta") else ""
        blocos.append(
            f"<div style='{ESTILO[tipo]}border-radius:10px;padding:8px 12px;margin:6px 0'>"
            f"<div style='font-size:12px;font-weight:bold;color:#555'>{html.escape(quem)}</div>"
            f"{para_html(m['content'])}{rodape}</div>"
        )
    if pendente:
        blocos.append(f"<div style='color:#888;font-style:italic'>🤖 {html.escape(dd_modelo.value)} está pensando…</div>")
    if not blocos:
        blocos.append("<div style='color:#999'>Escolha um modelo e mande a primeira mensagem.</div>")
    # âncora no fim + column-reverse mantém a rolagem no final
    caixa_chat.value = (
        "<div style='display:flex;flex-direction:column-reverse'><div>" + "".join(blocos) + "</div></div>"
    )
    lbl_status.value = (
        f"<span style='color:#666;font-size:12px'>Tokens na sessão: entrada {estado['tokens_in']:,} · "
        f"saída {estado['tokens_out']:,}</span>"
    )


def enviar(_=None):
    pergunta = ta_pergunta.value.strip()
    if not pergunta:
        return
    ta_pergunta.value = ""
    btn_enviar.disabled = True
    modelo = dd_modelo.value
    estado["historico"].append({"role": "user", "content": pergunta})
    renderizar(pendente=True)

    # monta o que vai para o modelo: system atual + mensagens (sem as de erro)
    mensagens = [{"role": "system", "content": ta_sistema.value.strip()}] if ta_sistema.value.strip() else []
    mensagens += [{"role": m["role"], "content": m["content"]}
                  for m in estado["historico"] if m.get("tipo") != "erro"]
    try:
        r = chamar_modelo(modelo, mensagens, sl_temp.value, it_max.value)
        estado["tokens_in"] += r["tokens_in"] or 0
        estado["tokens_out"] += r["tokens_out"] or 0
        meta = f"{r['segundos']:.1f}s"
        if r["tokens_in"] is not None:
            meta += f" · {r['tokens_in']} tokens in / {r['tokens_out']} out"
        estado["historico"].append({"role": "assistant", "content": r["texto"] or "_(resposta vazia)_",
                                    "modelo": modelo, "meta": meta})
    except Exception as e:  # noqa: BLE001
        msg = str(e).split(" Config: ")[0][:500]
        if any(s in msg.lower() for s in ("api_token", "invalid access token", "token expired", "401")):
            msg += "\n\nO token do notebook expirou: rode de novo a célula 'Descoberta dos modelos' e depois a da interface."
        estado["historico"].append({"role": "assistant", "tipo": "erro", "modelo": modelo,
                                    "content": f"⚠️ Erro ao chamar `{modelo}`:\n\n```\n{msg}\n```"})
    finally:
        btn_enviar.disabled = False
        renderizar()


def limpar(_=None):
    estado.update(historico=[], tokens_in=0, tokens_out=0)
    renderizar()


def atualizar_modelos(_=None):
    global MODELOS
    atual = dd_modelo.value
    MODELOS = listar_modelos_chat()
    dd_modelo.options = [(m.rotulo, m.nome) for m in MODELOS]
    if atual in [m.nome for m in MODELOS]:
        dd_modelo.value = atual


btn_enviar.on_click(enviar)
btn_limpar.on_click(limpar)
btn_atualizar.on_click(atualizar_modelos)

renderizar()
display(widgets.VBox([
    widgets.HBox([dd_modelo, btn_atualizar]),
    widgets.HBox([sl_temp, it_max]),
    ta_sistema,
    caixa_chat,
    lbl_status,
    ta_pergunta,
    widgets.HBox([btn_enviar, btn_limpar]),
]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternativa sem interface
# MAGIC Usa o mesmo histórico, modelo e system prompt dos widgets acima. Ex.: `perguntar("Explique Delta Lake em 3 linhas")`
# MAGIC ou `perguntar("...", modelo="databricks-claude-sonnet-4")`.

# COMMAND ----------

def perguntar(texto: str, modelo: str | None = None):
    if modelo:
        dd_modelo.value = modelo
    ta_pergunta.value = texto
    enviar()
    ultima = estado["historico"][-1]
    displayHTML(para_html(ultima["content"]) +
                f"<div style='font-size:11px;color:#888'>{html.escape(ultima.get('modelo', ''))} · {ultima.get('meta', '')}</div>")


# perguntar("Olá! Em uma frase, quem é você?")
