# Resumo Executivo Matinal: guia de preenchimento

Define o que entra em cada campo de `resumo_executivo_template.html`. Use estas regras nos prompts do Genie, para que o texto tenha o mesmo padrão todos os dias.

## Regras de escrita

1. **Comece pela conclusão.** Títulos dizem o que os dados mostram, não o assunto. Use "Receita abaixo da meta há 12 dias", não "Evolução da receita".
2. **Todo número tem comparação.** Sempre contra a meta, o período anterior ou um limite.
3. **Traduza em impacto.** Prefira "R$ 0,6 mi abaixo da meta" a "desvio relevante".
4. **Não invente causa nem ação.** Se os dados não sustentam a explicação, escreva "causa em apuração".
5. **Sem adjetivos vazios.** Evite "expressivo", "robusto" e "significativo". Mostre o número.
6. **Formato brasileiro.** Use `R$ 48,2 mi`, `3,1%`, `0,4 p.p.` e `06/11`, com uma casa decimal.

## Campos

| Campo | O que colocar | Limite |
|---|---|---|
| `PREHEADER` | A conclusão do dia, para a prévia na caixa de entrada | 90 caracteres |
| `DATA_EXTENSO` | Quarta-feira, 4 de novembro de 2026 | |
| `DADOS_ATE` | Data e hora da última carga: 03/11, 23h59 | |
| `STATUS_ROTULO` / `STATUS_COR` | Situação geral: No plano, Atenção ou Crítico | ver cores |
| `MANCHETE` | O fato mais importante, com número e causa | 100 caracteres |
| `RESUMO_ACONTECEU` | O fato principal e sua variação | 1 frase, 140 caracteres |
| `RESUMO_IMPORTA` | A consequência para o negócio, em R$ ou % | 1 frase, 140 caracteres |
| `RESUMO_FAZER` | A decisão ou ação esperada do leitor, com prazo | 1 frase, 140 caracteres |
| `PERIODO_REFERENCIA` | Janela de todos os indicadores: Últimos 30 dias, até 03/11 | |
| `KPI_n_NOME` | Nome curto do indicador | 18 caracteres |
| `KPI_n_VALOR` | Valor atual: R$ 48,2 mi | 10 caracteres |
| `KPI_n_VAR` / `KPI_n_VAR_COR` | ▲ ou ▼ com a variação contra a meta: ▼ 3,1% vs. meta | 18 caracteres |
| `KPI_n_META` | Referência: Meta: R$ 49,7 mi | 20 caracteres |
| `ALERTA_n_SEVERIDADE` / `ALERTA_n_COR` | Crítico, Atenção ou Monitorar, do mais grave ao mais leve | ver cores |
| `ALERTA_n_TITULO` | O problema, com número | 80 caracteres |
| `ALERTA_n_IMPACTO` | Quanto custa ou pode custar | 1 frase |
| `ALERTA_n_ACAO` | Responsável · ação · prazo | 1 linha |
| `GRAFICO_n_TITULO` | A conclusão do gráfico | 70 caracteres |
| `GRAFICO_n_SUBTITULO` | Métrica, unidade e período | 1 linha |
| `GRAFICO_n_SRC` | Origem da imagem: `cid:grafico_n` | |
| `GRAFICO_n_ALT` | A conclusão em texto, para quando a imagem for bloqueada | 1 frase |
| `GRAFICO_n_LEITURA` | O que o gráfico não mostra sozinho: causa, contexto ou próximo passo | 1 a 2 frases |
| `TABELA_TITULO` | A conclusão da tabela | 70 caracteres |
| `TABELA_DIMENSAO` | Nome da primeira coluna: Região, Produto... | |
| `TAB_n_*` | Até 5 linhas, ordenadas da maior para a menor por realizado | |
| `TAB_TOTAL_*` | Total, calculado sobre todas as linhas da consulta | |
| `RADAR_n_QUANDO` | Hoje, 14h / Sex, 06/11 | 12 caracteres |
| `RADAR_n_ITEM` | Evento ou entrega que pode mudar os números | 1 frase |
| `LINK_PAINEL` / `LINK_GENIE` | URLs do painel e do espaço Genie | |
| `FONTES` | Tabelas do Unity Catalog usadas (`catalogo.schema.tabela`) | |
| `CONTATO_EMAIL` | Caixa do time responsável | |

## Cores

Use exatamente estes valores nos campos `*_COR`:

| Uso | Hex | Quando |
|---|---|---|
| Bom | `#0F7B4F` | Variação favorável, Status "No plano" |
| Atenção | `#A15C00` | Status e alerta "Atenção" |
| Ruim / Crítico | `#B42318` | Variação desfavorável, Status e alerta "Crítico" |
| Neutro | `#4A5563` | Alerta "Monitorar", variação dentro de ±0,5% |

**A cor indica se o resultado é bom ou ruim, não se subiu ou desceu.** Churn ou custo em alta usa ▲ em vermelho.

## Blocos opcionais

Trechos entre `<!-- INICIO:X -->` e `<!-- FIM:X -->` são removidos inteiros quando não há conteúdo: `ALERTA_1` a `ALERTA_3`, `ALERTAS`, `GRAFICO_2`, `RADAR_1` a `RADAR_3`, `RADAR`.

- **Sem alertas no dia:** remova `ALERTAS`. Não preencha com itens de baixa relevância.
- **Sem eventos:** remova `RADAR`.
- **Não sobra campo:** nenhum `{{...}}` pode chegar ao e-mail. Se sobrar algum, não envie.

## Gráficos

- **Imagem:** PNG de 1200 px de largura, fundo branco, anexado ao e-mail com `Content-ID` e referenciado como `cid:grafico_1`. O Gmail e o Outlook não exibem imagens embutidas em base64 de forma confiável.
- **Fonte:** no mínimo 11 pt numa figura de 5 polegadas de largura (240 dpi), para continuar legível no celular.
- **Título:** fica no HTML, nunca dentro da imagem.
- **Principal:** tendência com a referência de meta.
- **Causas:** barras horizontais com o desvio por causa, ordenadas por tamanho e com rótulo de valor.

## Validações antes de enviar

- Nenhum `{{` restante no HTML.
- `DADOS_ATE` é da última carga esperada. Se a carga atrasou, não envie sem avisar.
- O total da tabela confere com o KPI correspondente.
- Os números da manchete e dos alertas aparecem nos resultados das consultas.
