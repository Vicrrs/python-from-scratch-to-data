# Databricks notebook source
# MAGIC %md
# MAGIC # Chat com os modelos do Databricks
# MAGIC Lista os endpoints de **LLM de chat** do Model Serving que você tem permissão para usar
# MAGIC (Foundation Models `databricks-*`, modelos externos e endpoints custom) e abre um chat no próprio notebook.
# MAGIC
# MAGIC - A descoberta usa a mesma lógica do chat-especialistas: `serving_endpoints.list()` e filtro pela `task` do endpoint.
# MAGIC - As chamadas usam `serving_endpoints.query(...)` com as **suas** credenciais.
# MAGIC - O histórico fica em memória: trocar de modelo no meio da conversa continua a mesma conversa (útil para comparar).
# MAGIC - Sem interface? Use `perguntar("...")` numa célula qualquer (última célula).
# MAGIC
# MAGIC **Use "Run all"**.

# COMMAND ----------

# Descomente se o runtime tiver um databricks-sdk antigo (sem serving_endpoints.query / ChatMessage)
# %pip install -U databricks-sdk ipywidgets
# dbutils.library.restartPython()

# COMMAND ----------

# ---------------- Descoberta dos modelos de chat ----------------
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient


def criar_cliente() -> WorkspaceClient:
    """Cliente com host/token explícitos.

    Os callbacks dos botões (ipywidgets) rodam fora da execução de uma célula, e aí a autenticação
    automática do notebook falha com "Missing api_token from command execution". Por isso o token é
    capturado agora, enquanto a célula executa, e passado direto ao cliente.
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return WorkspaceClient(host=ctx.apiUrl().get(), token=ctx.apiToken().get(), auth_type="pat")


w = criar_cliente()


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


MODELOS = listar_modelos_chat()
assert MODELOS, "Nenhum endpoint de LLM de chat disponível para você. Verifique as permissões em Serving."
PADRAO = next((m.nome for m in MODELOS if "llama-3-3-70b" in m.nome), MODELOS[0].nome)

print(f"{len(MODELOS)} modelos de chat encontrados:")
for m in MODELOS:
    print(" -", m.rotulo)

# COMMAND ----------

# ---------------- Cliente de chat ----------------
import time

from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

PAPEIS = {"system": ChatMessageRole.SYSTEM, "user": ChatMessageRole.USER, "assistant": ChatMessageRole.ASSISTANT}


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


def chamar_modelo(endpoint: str, historico: list[dict], temperatura: float, max_tokens: int) -> dict:
    """historico = [{"role": "system"|"user"|"assistant", "content": str}, ...]"""
    msgs = [ChatMessage(role=PAPEIS[m["role"]], content=m["content"]) for m in historico]
    inicio = time.time()
    try:
        r = w.serving_endpoints.query(endpoint, messages=msgs, temperature=temperatura, max_tokens=max_tokens)
    except Exception as e:  # noqa: BLE001
        # modelos de raciocínio costumam recusar temperature: tenta de novo sem
        if "temperature" not in str(e).lower():
            raise
        r = w.serving_endpoints.query(endpoint, messages=msgs, max_tokens=max_tokens)
    conteudo = r.choices[0].message.content if r.choices else ""
    uso = r.usage
    return {
        "texto": _texto(conteudo),
        "segundos": time.time() - inicio,
        "tokens_in": getattr(uso, "prompt_tokens", None) if uso else None,
        "tokens_out": getattr(uso, "completion_tokens", None) if uso else None,
    }

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
