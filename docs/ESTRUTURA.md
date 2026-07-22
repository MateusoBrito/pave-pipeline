# 📂 Guia de Arquitetura e Estrutura do Projeto (`pave-pipeline`)

Este documento define os padrões de organização de código e responsabilidades no repositório do **Pipeline de Dados do Panorama Eleitoral 2026**.

---

## 🏗️ Visão Geral da Estrutura de Pastas

```text
pave-pipeline/
├── configs/             # Arquivos de configuração YAML (entidades, termos de busca)
├── dags/                # [AIRFLOW] DAGs de orquestração e agendamento
├── data/                # Dados temporários / caches locais (ignorado no git)
├── docs/                # Documentações do projeto, especificações e relatórios
├── notebook/            # Notebooks Jupyter para EDA e experimentos de NLP
├── pipelines/           # Scripts executáveis do fluxo principal (Coleta & NLP)
│   ├── collectors/      # Scripts de coleta por rede social
│   └── nlp/             # Scripts de pré-processamento, tópicos e sentimento
├── reports/             # Relatórios gerados (PDFs, plots, análises pontuais)
├── src/                 # Módulos Python reutilizáveis (banco, utilitários, configs)
│   ├── database/        # Conectores para MongoDB e PostgreSQL
│   └── utils/           # Funções auxiliares (limpeza de texto, parsing, etc.)
├── .env.example         # Template de variáveis de ambiente
├── docker-compose.yml   # Infraestrutura de bancos (Mongo, Postgres, Redis, Airflow)
└── ESTRUTURA.md         # Este guia

```

---

## Divisão de Responsabilidades

### 1. Pasta `src/` (Módulos Base Reutilizáveis)

* **Quem mexe:** Todo o time.
* **O que vai aqui:** Classes e funções **genéricas** que não executam tarefas sozinhas, mas servem de apoio aos scripts.
* `src/database/mongo.py`: Função que retorna a conexão pronta com o MongoDB.
* `src/database/postgres.py`: Função que conecta no PostgreSQL.
* `src/utils/config.py`: Leitor do arquivo `configs/entities.yaml`.
---

### 2. Pasta `pipelines/` (Scripts Executáveis)

* **Quem mexe:** Time de **Coleta de Dados** e Time de **NLP/IA**.
* **O que vai aqui:** Scripts de execução final. Eles importam o que precisam da `src/` e realizam a tarefa.
* **Regras para o time de Coleta/NLP:**
1. **Python Puro:** Não escreva código específico do Airflow aqui.
2. **Execução Direta:** Seu script deve poder ser executado isoladamente pelo terminal (ex: `python3 pipelines/collectors/youtube.py`).
3. **Modularidade:** Encapsule a lógica principal em uma função (`def run()`).
4. **Armazenamento:** Salve os dados brutos no MongoDB usando os conectores da `src/database/`.
---

### 3. Pasta `dags/` (Orquestração & Agendamento)

* **Quem mexe:** Responsável por **Infra / Airflow**.
* **O que vai aqui:** As DAGs do Apache Airflow.
* **Como funciona:** O Airflow apenas **importa e executa** as funções criadas pelo time na pasta `pipelines/`.
* *Exemplo:* A DAG `dag_coleta_diaria.py` vai chamar o script `pipelines/collectors/youtube.py` todos os dias às 23:00.

---

### 4. Pasta `configs/` (Configurações do Projeto)

* **O que vai aqui:** O arquivo `entities.yaml` com o cadastro de políticos, apelidos e termos de busca.
* **Regra de Ouro:** **Nenhum termo de busca ou nome de político deve estar fixo (hardcoded) no código Python.** Sempre leia as entidades a partir da pasta `configs/`.

---

## Como testar seu código localmente

1. **Suba os bancos de dados com o Docker:**
```bash
docker-compose up -d

```


2. **Certifique-se de ter as variáveis no seu `.env`:**
```bash
cp .env.example .env

```


3. **Execute seu script diretamente pelo terminal:**
```bash
python3 pipelines/collectors/nome_do_seu_script.py

```