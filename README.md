# Visagio Rocket Lab 2026.2 — Engenharia de Dados

**Autor: Gabriel Victalino**

Projeto desenvolvido para o desafio **Rocket Lab 2026.2 — Engenharia de Dados**, com o objetivo de construir um pipeline completo no Databricks, desde a ingestão dos dados brutos até a modelagem analítica e a orquestração.

A solução utiliza arquitetura **Medallion** com camadas **Bronze, Silver e Gold**, integra arquivos CSV com a API PTAX do Banco Central do Brasil, trata problemas de qualidade de dados, cria um modelo dimensional e gera uma tabela textual preparada para uso futuro com IA Generativa/RAG.

---

## Objetivo

O pipeline foi construído para atender quatro necessidades principais:

1. **Ingestão confiável**
   - carregar os arquivos de origem sem alterar seu conteúdo;
   - registrar o momento da ingestão;
   - consumir a cotação oficial do dólar via API;
   - preservar o histórico de cargas.

2. **Qualidade e preparação**
   - padronizar tipos e nomes de colunas;
   - tratar datas em múltiplos formatos;
   - limpar valores financeiros inconsistentes;
   - tratar problemas de Column Shift;
   - manter unicidade lógica;
   - construir uma série temporal contínua da cotação.

3. **Modelagem analítica**
   - construir dimensões, tabela fato e bridges;
   - garantir uma linha única por filme na fato;
   - criar surrogate keys;
   - preservar relacionamentos muitos-para-muitos sem duplicar métricas.

4. **Orquestração e consumo**
   - automatizar o pipeline em um Databricks Workflow;
   - responder às perguntas analíticas do desafio;
   - construir um documento textual por filme para uso futuro com LLMs/RAG.

---

# Arquitetura

```text
Landing / Volume
       ↓
     Bronze
       ↓
     Silver
       ↓
      Gold
       ↓
Analytics / BI / GenAI
```

A responsabilidade de cada camada foi mantida separada de propósito:

- **Bronze**: preserva a origem e o histórico de ingestão.
- **Silver**: aplica qualidade, padronização, tipagem e regras de negócio.
- **Gold**: organiza os dados em modelo dimensional e estruturas voltadas para análise.

Essa separação evita misturar ingestão com regras analíticas e facilita reprocessamento, validação e manutenção.

---

# Tecnologias utilizadas

- Databricks
- Apache Spark
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Databricks Volumes
- Databricks Workflows
- Python
- API REST
- API PTAX do Banco Central do Brasil
- Git
- GitHub

---

# Estrutura do repositório

```text
RocketLab-2026.2-EngenhariaDeDados/
│
├── Notebooks/
│   ├── Landing_to_Bronze.ipynb
│   ├── Bronze_to_Silver.ipynb
│   └── Silver_to_Gold.ipynb
│
├── Workflow/
│   └── job.yaml
│
├── Images/
│   └── workflow_success.png
│
└── README.md
```

Também foi utilizada a API PTAX do Banco Central para obter a cotação do dólar.


# Camada Bronze

Notebook:

```text
Landing_to_Bronze.ipynb
```

Tabelas:

```text
workspace.bronze.tb_movies_info
workspace.bronze.tb_movies_financials
workspace.bronze.tb_movies_metrics
workspace.bronze.tb_credits_and_tags
workspace.bronze.tb_movies_reviews
workspace.bronze.tb_cotacao_dolar
```

## Decisão 1 — não inferir schema nos CSVs

Os CSVs são lidos com:

```python
.option("inferSchema", "false")
```

### Por quê?

A Bronze deve preservar os dados da forma mais próxima possível da origem. Se o Spark inferisse tipos nessa etapa, valores inconsistentes poderiam ser convertidos ou invalidados antes que a Silver tivesse a oportunidade de tratá-los.

A tipagem foi, portanto, deliberadamente adiada para a Silver.

### Por que não tipar diretamente na Bronze?

Porque a Bronze não é a camada de qualidade. Valores como `34.0M`, `Unknown`, datas em formatos distintos ou textos deslocados precisam permanecer rastreáveis até o momento da limpeza.

---

## Decisão 2 — adicionar somente `ingestion_datetime`

Cada tabela Bronze recebe:

```text
ingestion_datetime
```

Esse campo registra quando cada batch entrou no Lakehouse.

### Motivos

Ele permite:

- rastrear a origem temporal da carga;
- identificar diferentes batches;
- deduplicar na Silver mantendo a versão mais recente;
- validar a última ingestão sem depender do total acumulado.

Nenhuma outra transformação estrutural ou de conteúdo é feita nos cinco CSVs.

---

## Decisão 3 — utilizar Delta Lake

As tabelas são persistidas em:

```text
format = delta
```

### Motivo

Delta Lake oferece uma estrutura adequada ao Lakehouse, facilita leitura consistente das tabelas e é a forma escolhida para persistência entre as camadas.

---

## Decisão 4 — utilizar `Append` na Bronze

A escrita utiliza:

```python
.mode("append")
```

### Por quê?

A Bronze deve manter histórico. Quando o pipeline roda novamente, o novo batch é acrescentado em vez de substituir o anterior.

Exemplo validado:

```text
tb_movies_info
Origem:       106930
Último batch: 106930
Total Bronze: 213860
```

A segunda execução adicionou uma nova carga completa.

### Por que não `overwrite`?

`overwrite` destruiria o histórico da ingestão anterior e eliminaria a utilidade de `ingestion_datetime` para distinguir versões.

---

## Decisão 5 — validar o último batch, e não o total acumulado

Com `Append`, esta comparação seria incorreta:

```text
total Bronze == total CSV
```

Depois da segunda execução:

```text
CSV:          106930
1ª execução:  106930
2ª execução:  106930
Total Bronze: 213860
```

Isso é um estado válido.

Por isso, a validação localiza o `MAX(ingestion_datetime)` e compara o batch mais recente com a origem.

---

# Ingestão da cotação do dólar

O notebook utiliza widgets:

```text
data_inicio
data_fim
```

no formato:

```text
MM-DD-YYYY
```

A resposta da API é armazenada em:

```text
workspace.bronze.tb_cotacao_dolar
```

## Decisão 6 — parametrizar o período por widgets

### Por quê?

O período da consulta pode ser alterado sem modificar o código e o Databricks Workflow consegue passar esses valores diretamente para o notebook.

Na configuração final do Job:

```text
data_inicio = 09-15-2026
data_fim    = 09-21-2026
```

---

## Decisão 7 — usar `Append` também na cotação

A API segue a mesma estratégia histórica da Bronze.

Teste realizado:

```text
Registros retornados pela API: 5
Total Bronze após nova execução: 10
```

### Por que não deduplicar na Bronze?

Porque a Bronze preserva cada execução. A escolha de uma cotação única por data é feita posteriormente na Silver.

---

# Camada Silver

Notebook:

```text
Bronze_to_Silver.ipynb
```

Tabelas:

```text
workspace.silver.tb_info_filmes
workspace.silver.tb_financeiro_filmes
workspace.silver.tb_metricas_engajamento
workspace.silver.tb_avaliacoes_usuarios
workspace.silver.tb_generos
workspace.silver.tb_pessoas_empresas
workspace.silver.tb_cotacao_dolar
```

---

# `silver.tb_info_filmes`

Resultado validado:

```text
97.879 filmes
97.879 IDs distintos
0 IDs nulos
```

## Decisão 8 — normalizar o status antes de traduzi-lo

A origem continha variações como:

```text
Released
released
RELEASED
```

O texto é primeiro normalizado e depois mapeado para português.

Mapeamento:

```text
Released        → Lançado
Post Production → Pós-Produção
In Production   → Em Produção
Planned         → Planejado
Rumored         → Rumores
Canceled        → Cancelado
```

Valores não mapeáveis recebem:

```text
Não Informado
```

### Por que normalizar antes?

Porque traduzir diretamente exigiria tratar muitas grafias equivalentes como categorias distintas. A normalização reduz esse problema antes do mapeamento.

---

## Decisão 9 — manter somente a versão mais recente de cada filme

A Bronze é histórica e pode conter várias execuções do mesmo arquivo. A Silver precisa ter uma linha por filme.

Por isso, quando `id_filme` se repete, é mantido o registro com maior:

```text
ingestion_datetime
```

### Por que não manter todas as versões na Silver?

Porque isso quebraria a unicidade e multiplicaria linhas na Gold. O histórico permanece disponível na Bronze.

---

## Decisão 10 — tratar múltiplos formatos de data

Foram considerados os formatos encontrados na origem:

```text
yyyy-MM-dd
MM-dd-yyyy
dd/MM/yyyy
```

A transformação tenta os padrões e mantém a primeira conversão válida.

### Por que não escolher um único formato?

Porque isso transformaria datas válidas dos outros padrões em `NULL`.

### Por que valores impossíveis viram `NULL`?

Porque representar ausência é preferível a fabricar uma data incorreta.

---

## Decisão 11 — criar `ano_lancamento`

O campo é derivado de:

```text
data_lancamento
```

### Motivo

Ele é recorrente em filtros e análises temporais e evita recalcular o ano em várias consultas.

---

# `silver.tb_financeiro_filmes`

Resultado validado:

```text
99.006 IDs únicos
0 valores financeiros finais <= 0
```

Campos:

```text
orcamento_usd
receita_usd
lucro_usd
margem_lucro
orcamento_brl
receita_brl
lucro_brl
```

## Decisão 12 — transformar textos de ausência em `NULL`

Valores como:

```text
Unknown
Não Informado
N/A
NULL
```

não são transformados em zero.

### Por que `NULL`?

Zero significaria um valor financeiro real de zero. `NULL` representa corretamente informação ausente ou inválida.

---

## Decisão 13 — suportar formatos financeiros heterogêneos

Foram tratados valores como:

```text
$97000000
USD 150000000
34.0M
200.0M
```

O sufixo `M` é interpretado como milhões.

### Por que não usar apenas `cast`?

Porque símbolos e textos fariam o cast falhar ou gerar nulos silenciosamente. A higienização ocorre antes da tipagem.

---

## Decisão 14 — valores `<= 0` tornam-se `NULL`

Orçamentos e receitas iguais ou menores que zero são tratados como inválidos.

### Por quê?

No contexto da base, esses valores não representam métricas financeiras utilizáveis e contaminariam lucro e margem.

---

# `silver.tb_cotacao_dolar`

Validação final:

```text
Data inicial:       2026-09-14
Data final:         2026-09-21
Quantidade de dias: 8
Datas distintas:    8
Datas duplicadas:   0
Dias sem cotação:   0
```

## Decisão 15 — deduplicar cotação somente na Silver

Como a Bronze usa `Append`, uma data pode reaparecer em diferentes batches.

Na Silver é mantida uma única linha por `data_cotacao`, priorizando:

```text
ingestion_datetime DESC
dataHoraCotacao DESC
```

### Por que não deduplicar na Bronze?

Porque isso apagaria histórico. A Silver é a camada destinada a transformar histórico bruto em estado analítico.

---

## Decisão 16 — construir calendário até o dia atual

O calendário é criado desde a primeira data disponível até:

```text
CURRENT_DATE()
```

### Por quê?

A API não retorna necessariamente dados em finais de semana ou feriados. O calendário garante continuidade diária.

---

## Decisão 17 — utilizar Forward Fill

Dias sem nova cotação recebem a última taxa disponível.

Exemplo:

```text
2026-09-18 → 5.1569
2026-09-19 → 5.1569
2026-09-20 → 5.1569
2026-09-21 → 5.1569
```

### Por que Forward Fill?

Ele reutiliza a última informação oficial disponível.

### Por que não zero?

Zero criaria uma taxa inexistente.

### Por que não interpolar?

Interpolar inventaria uma cotação que não foi publicada pelo Banco Central.

---

## Decisão 18 — usar a cotação mais recente da série na conversão

Na execução validada:

```text
Data de referência: 2026-09-21
Cotação USD/BRL:    5.1569
```

### Por que uma única taxa?

O desafio pede valores equivalentes em BRL, mas não define uma regra histórica por data de lançamento. Usar uma única taxa torna os valores comparáveis dentro da execução.

### Por que não usar câmbio histórico por filme?

Isso exigiria uma regra não fornecida e criaria uma precisão que a origem não suporta.

---

## Decisão 19 — calcular lucro e margem com proteção

Lucro:

```text
receita - orçamento
```

Margem:

```text
(lucro / receita) * 100
```

Operações são feitas somente quando os valores necessários são válidos.

### Por que proteger divisão por zero?

Porque não existe interpretação válida para uma margem calculada sobre receita igual a zero.

---

# `silver.tb_metricas_engajamento`

Resultado:

```text
99.013 registros
99.013 IDs distintos
0 IDs nulos
0 métricas inválidas restantes
0 casos suspeitos de Column Shift restantes
```

## Decisão 20 — utilizar conversão segura

A origem apresenta Column Shift e textos podem aparecer em campos numéricos.

### Por que não `cast()` simples?

Uma conversão segura permite que valores incompatíveis virem `NULL` sem interromper o pipeline e sem mascarar o problema.

---

## Decisão 21 — não impor teto artificial à popularidade

São aceitos todos os valores não negativos que não apresentem evidência de corrupção.

Exemplos altos e legítimos:

```text
blue beetle  → 2994.357
Gran Turismo → 2680.593
```

### Por que não limitar a 100 ou 1000?

Porque a regra do domínio não define esse teto. Um limite arbitrário apagaria dados válidos.

---

## Decisão 22 — detectar anos deslocados usando contexto da linha

Foram encontrados valores como:

```text
2020
2019
2018
1969
```

em `popularidade`.

Esses valores não foram removidos apenas porque parecem anos.

A correção considera também evidências de Column Shift na mesma linha, como texto em campos que deveriam ser numéricos.

### Por quê?

Isso reduz falsos positivos. A regra escolhida usa o contexto do registro, e não apenas um valor isolado.

---

## Decisão 23 — notas TMDB/IMDb apenas entre 0 e 10

Valores fora do intervalo:

```text
0 <= nota <= 10
```

viram `NULL`.

### Motivo

Valores fora dessa escala representam erro de escala, corrupção ou deslocamento.

---

## Decisão 24 — votos precisam ser inteiros e não negativos

São aceitos:

```text
150
150.0
150.000
```

quando representam um número inteiro.

São rejeitados:

```text
-4
15.7
texto
```

### Por quê?

Contagem de votos é uma quantidade discreta e não pode ser negativa ou fracionária.

---

# `silver.tb_avaliacoes_usuarios`

Resultado:

```text
32.412 avaliações
0 notas inválidas
0 duplicatas
```

## Decisão 25 — nota inválida vira `NULL`, sem excluir o registro

### Por quê?

Mesmo com uma nota incorreta, o comentário ou a identificação da avaliação ainda podem ser úteis. Apenas o atributo inválido é anulado.

---

## Decisão 26 — comentário vazio vira `"Sem comentário"`

### Por quê?

A ausência fica explícita e o campo se torna mais simples de consumir em relatórios e textos.

---

## Decisão 27 — deduplicar pela combinação completa

A combinação usada é:

```text
id_filme
nome_usuario
nota_usuario
comentario_usuario
```

### Por que não apenas filme + usuário?

Porque avaliações distintas do mesmo usuário não devem ser eliminadas automaticamente. Apenas registros integralmente iguais são considerados duplicados.

---

# `silver.tb_generos`

Resultado:

```text
132.775 relações filme-gênero
0 gêneros inválidos
0 duplicatas
```

## Decisão 28 — normalizar separadores e aplicar `split + explode`

A origem usa `,` e `;`.

### Por quê?

Um filme pode possuir vários gêneros dentro de uma única string. A Silver precisa transformar isso em uma relação filme-gênero por linha.

---

## Decisão 29 — usar whitelist de gêneros válidos

Domínio:

```text
Action
Adventure
Animation
Comedy
Crime
Documentary
Drama
Family
Fantasy
History
Horror
Music
Mystery
Romance
Science Fiction
TV Movie
Thriller
War
Western
```

### Por quê?

Para gêneros existe um domínio pequeno e conhecido. Isso permite remover de forma segura resíduos de Column Shift sem depender de heurísticas genéricas.

---

# `silver.tb_pessoas_empresas`

Resultado:

```text
914.822 relações
0 IDs nulos
0 nomes nulos/vazios
0 tipos inválidos
0 duplicatas
0 resíduos longos
```

Tipos:

```text
Ator
Diretor
Roteirista
Produtora
```

## Decisão 30 — consolidar quatro origens em uma estrutura unificada

Mapeamento:

```text
cast                 → Ator
directors            → Diretor
writers              → Roteirista
production_companies → Produtora
```

### Por quê?

As quatro colunas representam a mesma estrutura lógica:

```text
filme + entidade + tipo
```

A unificação reduz duplicação de código e facilita a criação das dimensões Gold.

---

## Decisão 31 — normalizar capitalização de forma conservadora

Espaços e inconsistências de caixa são tratados, mas evita-se aplicar `initcap()` cegamente.

### Por quê?

Nomes válidos podem conter capitalização específica, como:

```text
T.J. Miller
McG
MGM
```

Uma normalização agressiva poderia degradar dados corretos.

---

## Decisão 32 — remover resíduos de Column Shift por tamanho após diagnóstico

A inspeção mostrou que valores muito longos em campos de entidades eram trechos de sinopses e frases deslocadas.

Foi utilizado:

```text
limite = 60 caracteres
```

Resultado:

```text
Registros antes:     915.393
Registros depois:    914.822
Registros removidos: 571
Percentual removido: 0,0624%
```

### Por que 60?

O limite foi definido a partir da inspeção da distribuição real e dos valores extremos do dataset.

### Por que não usar uma regex muito agressiva para frases?

Porque palavras comuns também podem aparecer em nomes legítimos de organizações. Foi preferida uma regra simples, mensurável e fundamentada nos dados observados.

---

# Camada Gold

Notebook:

```text
Silver_to_Gold.ipynb
```

Tabelas:

```text
gold.dim_movies
gold.dim_genres
gold.dim_people
gold.dim_companies
gold.dim_reviews
gold.bridge_movie_genre
gold.bridge_movie_person
gold.bridge_movie_company
gold.fact_movies_performance
gold.gold_genai_movies_context
```

---

# Decisão 33 — utilizar surrogate keys

Foram criadas:

```text
sk_movie_id
sk_genre_id
sk_person_id
sk_company_id
sk_review_id
```

com `row_number()`.

### Por quê?

Surrogate keys desacoplam o modelo analítico das chaves externas e fornecem relações consistentes entre dimensões, fato e bridges.

### Por que `row_number()`?

Era uma estratégia adequada ao escopo do desafio e gera chaves numéricas `BIGINT` simples.

### Sobre o warning de Window

O Spark pode exibir:

```text
WindowExpression: No Partition Defined
```

ao gerar uma sequência global.

Esse aviso está relacionado a performance, e não à correção lógica da chave.

---

# `gold.dim_movies`

Resultado:

```text
97.879 filmes
97.879 IDs naturais
97.879 SKs
```

## Decisão 34 — manter `id_filme` junto da surrogate key

### Por quê?

A SK é usada no modelo dimensional, enquanto a chave natural preserva rastreabilidade até a Silver e a origem.

---

# `gold.dim_genres`

Foi criada como catálogo único de gêneros.

### Por que não guardar os relacionamentos aqui?

Porque uma dimensão deve ter uma linha por entidade. Relações filme-gênero pertencem à bridge.

---

# `gold.dim_people`

Inclui:

```text
Ator
Diretor
Roteirista
```

### Por que excluir `Produtora`?

Produtora é empresa, não pessoa física. Separá-la em `dim_companies` preserva semântica e facilita análise por estúdio.

---

# `gold.dim_companies`

Catálogo único de produtoras/estúdios.

Essa separação impede que pessoas e empresas sejam tratadas como a mesma categoria analítica.

---

# `gold.dim_reviews`

Campos:

```text
sk_review_id
sk_movie_id
qtd_avaliacoes_usuarios
nota_media_usuarios
```

## Decisão 35 — agregar reviews por filme

### Por quê?

A Gold precisa de uma visão resumida. As avaliações individuais permanecem disponíveis na Silver, enquanto a Gold fornece quantidade e média por filme.

---

# Bridge Tables

```text
bridge_movie_genre
bridge_movie_person
bridge_movie_company
```

## Decisão 36 — utilizar bridges para relações N:N

Um filme pode possuir múltiplos gêneros, pessoas e produtoras.

### Por que não juntar tudo diretamente na fato?

Um cenário como:

```text
1 filme × 5 atores × 3 gêneros × 2 produtoras
```

criaria diversas linhas do mesmo filme e duplicaria receita, orçamento, lucro e métricas.

As bridges preservam os relacionamentos sem alterar o grão da fato.

---

# `gold.fact_movies_performance`

Resultado:

```text
96.463 registros
96.463 sk_movie_id distintas
0 SKs nulas
```

## Decisão 37 — incluir apenas filmes `Lançado`

### Por quê?

Métricas de desempenho financeiro e engajamento são analisadas para obras efetivamente lançadas. Filmes planejados, cancelados ou ainda em produção possuem outra interpretação.

---

## Decisão 38 — manter o grão de uma linha por filme

A fato parte dos filmes lançados e recebe apenas joins compatíveis com esse grão.

Pessoas, gêneros e empresas ficam nas bridges.

### Validação

```text
COUNT(*) == COUNT(DISTINCT sk_movie_id)
```

A igualdade confirma que os joins não duplicaram a fato.

---

# `gold.gold_genai_movies_context`

Resultado:

```text
96.463 documentos
96.463 filmes
0 documentos nulos/vazios
```

Estrutura:

```text
movie_id
title
llm_context_document
```

## Decisão 39 — gerar uma frase corrida

O documento reúne título, ano, receita, orçamento, atores, diretor e sinopse.

### Por quê?

O objetivo é criar uma unidade textual semanticamente útil para futura vetorização em Vector Search/RAG, em vez de apenas concatenar campos sem contexto linguístico.

---

## Decisão 40 — agregar atores antes de criar o documento

### Por quê?

Sem agregação, um filme com vários atores geraria vários registros e vários documentos.

A regra preserva:

```text
1 filme = 1 documento
```

---

## Decisão 41 — usar fallback para valores nulos

### Por quê?

Uma concatenação contendo campo nulo pode resultar em documento nulo.

Os fallbacks impedem que um filme desapareça da tabela de contexto apenas por possuir diretor, sinopse ou valor financeiro ausente.

---

# Resultados analíticos

## 1. Receita total em BRL

```text
R$ 834.732.290.730,20
```

---

## 2. Top 5 filmes por popularidade

| Posição | Filme | Popularidade |
|---:|---|---:|
| 1 | blue beetle | 2994.357 |
| 2 | Gran Turismo | 2680.593 |
| 3 | The Nun II | 1692.778 |
| 4 | Meg 2: The Trench | 1567.273 |
| 5 | retribution | 1547.220 |

### Decisão relevante

Valores `2020`, `2019` e `2018` chegaram a aparecer como popularidade, mas foram identificados como Column Shift após inspeção da linha completa.

A correção não aplicou teto artificial de popularidade.

---

## 3. Filmes por gênero

| Gênero | Filmes |
|---|---:|
| Drama | 30457 |
| Documentary | 18612 |
| Comedy | 17348 |
| Thriller | 9424 |
| Horror | 9163 |
| Romance | 6996 |
| Action | 5541 |
| Crime | 4320 |
| Animation | 4159 |
| TV Movie | 3676 |
| Science Fiction | 3490 |
| Family | 3371 |
| Mystery | 3019 |
| Fantasy | 2982 |
| Adventure | 2597 |
| Music | 2573 |
| History | 2181 |
| War | 869 |
| Western | 383 |

---

## 4. Top 10 por receita com `RANK()`

| Rank | Filme | Receita USD | Receita BRL |
|---:|---|---:|---:|
| 1 | Avengers: Endgame | 2,800,000,000.00 | 14,439,320,000.00 |
| 2 | Avatar: The Way of Water | 2,320,250,281.00 | 11,965,298,674.09 |
| 3 | AVENGERS: INFINITY WAR | 2,052,415,039.00 | 10,584,099,114.62 |
| 4 | spider-man: no way home | 1,921,847,111.00 | 9,910,773,366.72 |
| 5 | The Lion King | 1,663,075,401.00 | 8,576,313,535.42 |
| 6 | Top Gun: Maverick | 1,488,732,821.00 | 7,677,246,284.61 |
| 7 | Barbie | 1,428,545,028.00 | 7,366,863,854.89 |
| 8 | The Super Mario Bros. Movie | 1,355,725,263.00 | 6,991,339,608.76 |
| 9 | Black Panther | 1,349,926,083.00 | 6,961,433,817.42 |
| 10 | Star Wars: The Last Jedi | 1,332,698,830.00 | 6,872,594,596.43 |

### Por que `RANK()`?

Porque ele preserva corretamente a posição analítica em caso de empate.

---

# Decisão 42 — data de referência das janelas de 2 e 5 anos

A data é calculada por:

```sql
MAX(data_lancamento)
```

considerando apenas:

```text
status_filme = 'Lançado'
data_lancamento IS NOT NULL
data_lancamento <= CURRENT_DATE()
```

Resultado:

```text
2026-02-19
```

### Por que não usar diretamente `CURRENT_DATE()`?

Porque o limite superior da regra é a data de lançamento válida mais recente existente na base.

`CURRENT_DATE()` serve somente para excluir datas futuras.

---

## 5. Ator com mais participações nos últimos 2 anos

```text
Kevin Hart
64 filmes
```

## Decisão 43 — contar filmes distintos

### Por quê?

A pergunta mede participações em filmes, não o número de linhas produzido por joins intermediários.

---

## 6. Produtora com maior lucro nos últimos 5 anos

```text
Universal Pictures
Lucro total USD: 5.772.329.679,00
Lucro total BRL: 29.767.326.921,64
```

## Decisão 44 — não ratear lucro entre produtoras

A base informa associações filme-produtora, mas não informa percentuais financeiros.

### Por que não dividir igualmente?

Isso inventaria uma regra não suportada pela fonte.

### Por que não estimar percentuais?

Também produziria informação sem fundamento.

Por isso, o lucro total do filme é associado às produtoras relacionadas e essa premissa é explicitada.

---

# Databricks Workflow

Job:

```text
CineData_ETL_Workflow
```

Fluxo:

```text
to_Bronze
    ↓
to_Silver
    ↓
to_Gold
```

## Decisão 45 — dependências explícitas

```text
to_Silver depende de to_Bronze
to_Gold depende de to_Silver
```

### Por quê?

A Silver depende das tabelas Bronze já atualizadas, e a Gold depende da Silver já tratada. Execução paralela poderia fazer uma camada consumir dados antigos ou incompletos.

---

# Schedule

Configuração final:

```yaml
schedule:
  quartz_cron_expression: 7 0 6 * * ?
  timezone_id: America/Fortaleza
  pause_status: UNPAUSED
```

Isso representa execução diária às:

```text
06:00:07
```

no fuso:

```text
America/Fortaleza
```

### Por que diário?

Simula uma rotina real de atualização sem criar batches excessivamente frequentes.

Uma configuração inicial que executaria a cada 10 minutos foi descartada porque, com Bronze em `Append`, isso geraria diversas cargas desnecessárias ao longo do dia.

---

# `job.yaml`

Salvar em:

```text
Workflow/job.yaml
```

Configuração:

```yaml
resources:
  jobs:
    CineData_ETL_Workflow:
      name: CineData_ETL_Workflow

      schedule:
        quartz_cron_expression: 7 0 6 * * ?
        timezone_id: America/Fortaleza
        pause_status: UNPAUSED

      tasks:
        - task_key: to_Bronze
          notebook_task:
            notebook_path: /Workspace/Users/gabrielvictalino@gmail.com/RocketLab-2026.2-EngenhariaDeDados/Notebooks/Landing_to_Bronze
            base_parameters:
              data_inicio: 09-15-2026
              data_fim: 09-21-2026
            source: WORKSPACE

        - task_key: to_Silver
          depends_on:
            - task_key: to_Bronze
          notebook_task:
            notebook_path: /Workspace/Users/gabrielvictalino@gmail.com/RocketLab-2026.2-EngenhariaDeDados/Notebooks/Bronze_to_Silver
            source: WORKSPACE

        - task_key: to_Gold
          depends_on:
            - task_key: to_Silver
          notebook_task:
            notebook_path: /Workspace/Users/gabrielvictalino@gmail.com/RocketLab-2026.2-EngenhariaDeDados/Notebooks/Silver_to_Gold
            source: WORKSPACE

      queue:
        enabled: true

      performance_target: PERFORMANCE_OPTIMIZED
```

---

# Evidência do Workflow

A execução deve mostrar:

```text
to_Bronze  → Succeeded
to_Silver  → Succeeded
to_Gold    → Succeeded
```

Imagem sugerida:

```text
Images/workflow_success.png
```

Referência no README:

```markdown
![Execução bem-sucedida do Databricks Workflow](Images/workflow_success.png)
```

---

# Estratégia de reprocessamento

A arquitetura segue:

```text
Bronze = histórico
Silver = estado tratado
Gold   = estado analítico
```

## Decisão 46 — permitir crescimento da Bronze sem duplicar Silver/Gold

Durante os testes, a Bronze foi carregada mais de uma vez.

Mesmo assim:

```text
dim_movies                  97.879
fact_movies_performance     96.463
gold_genai_movies_context   96.463
```

permaneceram estáveis.

Isso demonstra que o histórico bruto não está contaminando a granularidade analítica.

---

# Validações implementadas

Foram validados:

- quantidade de registros do último batch;
- presença de `ingestion_datetime`;
- IDs nulos;
- IDs duplicados;
- unicidade por filme;
- datas inválidas;
- status não reconhecidos;
- valores financeiros inválidos;
- divisão por zero;
- continuidade da cotação;
- datas duplicadas na cotação;
- notas fora da escala;
- votos negativos ou fracionários;
- Column Shift;
- gêneros fora do domínio;
- entidades inválidas;
- duplicatas de relações;
- surrogate keys;
- granularidade da fato;
- documentos GenAI nulos ou vazios.

---

# Resumo das contagens validadas

## Bronze após duas cargas

```text
tb_movies_info
Origem:       106930
Último batch: 106930
Total Bronze: 213860

tb_movies_financials
Origem:       106165
Último batch: 106165
Total Bronze: 212330

tb_movies_metrics
Origem:       107364
Último batch: 107364
Total Bronze: 214728

tb_credits_and_tags
Origem:       106320
Último batch: 106320
Total Bronze: 212640

tb_movies_reviews
Origem:       32412
Último batch: 32412
Total Bronze: 64824
```

## Silver

```text
tb_info_filmes             97.879
tb_financeiro_filmes       99.006
tb_metricas_engajamento    99.013
tb_avaliacoes_usuarios     32.412
tb_generos                 132.775
tb_pessoas_empresas        914.822
```

## Gold

```text
dim_movies                  97.879
fact_movies_performance     96.463
gold_genai_movies_context   96.463
```

---

# Princípios usados nas decisões

## 1. Não inventar informação ausente

Quando um valor não era confiável, a preferência foi por:

```text
NULL
```

em vez de `0` ou outro valor artificial.

Aplicado a:

- orçamento;
- receita;
- notas;
- votos;
- popularidade corrompida;
- datas impossíveis.

---

## 2. Evitar regras arbitrárias

O tratamento de popularidade não utiliza teto fixo.

As regras foram baseadas em domínio e em evidências da própria linha/dataset.

---

## 3. Preservar histórico onde faz sentido

A Bronze mantém todos os batches.

A deduplicação ocorre apenas quando os dados passam a representar estado tratado ou analítico.

---

## 4. Não criar precisão inexistente

Dois exemplos:

- Forward Fill reutiliza a última cotação oficial em vez de interpolar uma taxa fictícia.
- Lucro de filmes com múltiplas produtoras não é dividido em percentuais inventados.

---

# Premissas e limitações

## Conversão USD → BRL

A conversão utiliza a cotação mais recente da série da execução.

Ela oferece consistência analítica, mas não representa reconstrução histórica do câmbio na data de lançamento de cada filme.

## Lucro por produtora

Não há percentual de participação financeira por produtora na fonte.

A análise considera o lucro dos filmes associados, sem rateio.

## Limpeza de pessoas/empresas

O limite de 60 caracteres foi derivado da inspeção do próprio dataset e não é uma regra universal para nomes.

## Surrogate keys com `row_number()`

A solução é adequada ao escopo do desafio. Em um cenário incremental com Slowly Changing Dimensions, poderia ser necessária uma estratégia persistente de geração de chaves.

---

# Como executar

## Manualmente

```text
Landing_to_Bronze
        ↓
Bronze_to_Silver
        ↓
Silver_to_Gold
```

## Pelo Workflow

```text
Jobs & Pipelines
→ CineData_ETL_Workflow
→ Run now
```

O próprio Job controla as dependências.

---


# ✅ Checklist final

- [x] `Landing_to_Bronze.ipynb`
- [x] `Bronze_to_Silver.ipynb`
- [x] `Silver_to_Gold.ipynb`
- [x] Bronze em Delta + Append
- [x] API do Banco Central
- [x] Widgets `data_inicio` e `data_fim`
- [x] Camada Silver tratada
- [x] Forward Fill
- [x] Tratamento de Column Shift
- [x] Modelo dimensional Gold
- [x] Bridge tables
- [x] `fact_movies_performance`
- [x] `gold_genai_movies_context`
- [x] Seis consultas analíticas
- [x] Databricks Workflow
- [x] Dependências `Bronze → Silver → Gold`
- [x] Schedule
- [x] `job.yaml`
- [x] Screenshot da execução bem-sucedida
- [x] README com justificativas das decisões
- [x] Repositório GitHub público
