# Alvify Worker

Worker remoto de scraping para a plataforma [Alvify](https://alvify.com.br).

Este repositório contém tudo o que é necessário para instalar e executar um worker Alvify em qualquer VPS Linux. O worker se conecta à API Alvify, recebe tarefas de scraping (Google Maps), coleta os leads usando Playwright + Chromium e reporta os resultados em tempo real.

---

## O que é o Worker?

A plataforma Alvify distribui o trabalho de coleta de leads entre um ou mais workers remotos. Cada worker:

- Faz **long-polling** na API para receber jobs de scraping
- Executa o **Google Maps Scraper** com Playwright + Chromium (headless)
- Envia os leads coletados de volta para a API em tempo real
- Reporta **heartbeat** (CPU, RAM, jobs ativos) a cada 15 segundos
- Expõe um endpoint `/health` para que o painel Admin possa testar a conectividade

---

## Requisitos

| Item | Mínimo |
|------|--------|
| Sistema operacional | Ubuntu 20.04 / 22.04 / 24.04 · Debian 11 / 12 |
| RAM | 2 GB (4 GB recomendado) |
| CPU | 2 vCPUs |
| Disco | 5 GB livres |
| Acesso à internet | Saída HTTPS obrigatória |
| Porta de entrada | 8001/TCP (health check) |

> Python 3.11+ é instalado automaticamente se não estiver presente.

---

## Instalação

```bash
git clone https://github.com/kallyd/alvify-worker.git
cd alvify-worker
chmod +x install.sh
sudo ./install.sh
```

O instalador:
1. Detecta o SO e instala dependências do sistema
2. Instala Python 3.11+ (via apt, PPA deadsnakes ou compilação — automático)
3. Cria o usuário de sistema `alvify`
4. Copia os arquivos para `/opt/alvify-worker/`
5. Cria o ambiente virtual Python e instala as dependências
6. Instala o Chromium via Playwright
7. Instala e habilita o serviço systemd `alvify-worker`
8. **Abre o assistente de configuração** para inserir as credenciais

---

## Configuração

Após a instalação, o assistente exibe o **IP público** do servidor e guia você pelo processo:

```
╔══════════════════════════════════════════════════╗
║        Alvify Worker — Configuração Inicial       ║
╚══════════════════════════════════════════════════╝

  IP deste servidor:  203.0.113.42
  Porta de saúde    :  :8001
  API URL           :  https://workers.alvify.com.br

Como criar este worker no painel Alvify:

  1. Acesse https://alvify.com.br → Admin → Workers
  2. Clique em "+ Novo Worker"
  3. Preencha os campos:
       Nome  : qualquer nome (ex: vps-01)
       Host  : 203.0.113.42
       Porta : 8001
  4. Clique em Criar — copie o Worker ID e a API Key gerados
       (a API Key é mostrada uma única vez)

  Worker ID (UUID): ________________________________
  API Key          : ________________________________

  ✔ Conexão bem-sucedida! (HTTP 200)
  ✔ Serviço alvify-worker está rodando!
```

Para re-executar o assistente a qualquer momento:

```bash
sudo /opt/alvify-worker/setup.sh
```

---

## Estrutura do repositório

```
alvify-worker/
├── install.sh          # Instalador principal
├── setup.sh            # Assistente de configuração (credenciais + teste)
├── main.py             # Processo principal do worker
├── requirements.txt    # Dependências Python
├── .env.example        # Modelo de arquivo de configuração
└── core/
    ├── __init__.py
    ├── browser_pool.py  # Pool de contextos Playwright
    ├── scraper.py       # Google Maps scraper
    └── result_cache.py  # Helpers de cache de resultados
```

---

## Gerenciamento do serviço

```bash
# Status
sudo systemctl status alvify-worker

# Logs em tempo real
sudo journalctl -u alvify-worker -f

# Reiniciar
sudo systemctl restart alvify-worker

# Parar
sudo systemctl stop alvify-worker
```

---

## Configuração manual

O arquivo de configuração fica em `/opt/alvify-worker/.env`:

```env
API_URL=https://workers.alvify.com.br
WORKER_ID=<uuid do worker>
WORKER_API_KEY=<chave gerada no painel>
MAX_CONCURRENCY=2
HEALTH_PORT=8001
VERSION=1.0.0
LOG_LEVEL=INFO
```

Após editar, reinicie o serviço:

```bash
sudo systemctl restart alvify-worker
```

---

## Atualização

```bash
cd alvify-worker
git pull
sudo ./install.sh
```

O instalador preserva o `.env` existente (não sobrescreve credenciais).

---

## Licença

Uso restrito — © Alvify. Todos os direitos reservados.
