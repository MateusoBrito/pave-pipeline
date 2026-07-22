# 🐳 Guia da Infraestrutura Docker (`pave-pipeline`)

Este documento contém todas as instruções necessárias para subir, gerenciar e utilizar o ambiente de bancos de dados e orquestração do projeto **Panorama Eleitoral 2026**.

---

## Serviços Disponíveis

A nossa infraestrutura sobe automaticamente os seguintes contêineres via `docker-compose`:

| Serviço | Imagem | Porta Local | Finalidade |
| :--- | :--- | :--- | :--- |
| **MongoDB** | `mongo:latest` | `27017` | Data Lake Bruto (Armazena os JSONs das coletas) |
| **PostgreSQL** | `postgres:16-alpine` | `5432` | Banco Analítico (Consumido pelo time de Dev e Prisma) |
| **Redis** | `redis:alpine` | `6379` | Cache de performance para agregação de dados |
| **Airflow** | `apache/airflow:2.8.1` | `8080` | Painel do Orquestrador de Tarefas/DAGs |

---

## Como Subir o Ambiente (Primeiro Uso)

### 1. Copie o arquivo de variáveis de ambiente
Antes de subir os contêineres, garanta que possui o arquivo `.env` configurado:
```bash
cp .env.example .env

```

### 2. Inicie os serviços

Na raiz do repositório, execute o comando para baixar as imagens e iniciar os contêineres em segundo plano (`-d`):

```bash
docker-compose up -d

```

### 3. Verifique os contêineres ativos

Para confirmar que todos os 4 serviços estão rodando corretamente:

```bash
docker ps

```

---

## Endereços e Acessos

* **Painel Web do Airflow:** [http://localhost:8080](http://localhost:8080)
* **MongoDB:** `mongodb://root:rootpassword@localhost:27017/`
* **PostgreSQL:** `postgresql://postgres:postgrespassword@localhost:5432/panorama_db`
* **Redis:** `localhost:6379`

---

## Comandos Mais Utilizados no Dia a Dia

### Parar o ambiente (fim do dia)

Para desligar os contêineres sem perder os dados dos bancos:

```bash
docker-compose stop

```

### Ligar o ambiente novamente

Para reiniciar os contêineres que estavam parados:

```bash
docker-compose start

```

### Desligar e remover os contêineres

Caso precise desligar completamente a infraestrutura:

```bash
docker-compose down

```

### Ver os logs dos serviços

Se algum banco ou o Airflow apresentar erro, veja os logs com:

```bash
# Ver os logs de todos os serviços
docker-compose logs -f

# Ver os logs apenas do Airflow
docker-compose logs -f airflow

```

---

## Solução de Problemas Comuns

### 1. Erro `Permission denied` ao rodar comandos docker

Sua sessão precisa atualizar o grupo do Docker. Rode:

```bash
newgrp docker

```

### 2. Erro de porta já em uso (`port is already allocated`)

Se você já possui um PostgreSQL ou MongoDB instalado na sua máquina física utilizando a porta 5432 ou 27017, pare o serviço local antes de subir o Docker:

```bash
sudo systemctl stop postgresql

```

### 3. Resetar o banco de dados do zero

Se precisar limpar todos os dados brutos e analíticos para recomeçar do zero:

```bash
docker-compose down -v
docker-compose up -d

```

*(Atenção: A flag `-v` remove os volumes persistentes de dados).*