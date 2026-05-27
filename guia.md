# Guia de Instalação — Alvify Worker

Este guia explica como instalar e configurar um **worker remoto** do Alvify em um VPS Linux. O worker se conecta à API principal via HTTPS, recebe jobs de scraping da fila e devolve os leads coletados — sem acesso direto ao banco de dados.

---

## Requisitos mínimos

| Item | Mínimo recomendado |
|---|---|
| OS | Ubuntu 22.04 / Debian 12 (ou superior) |
| CPU | 2 vCPUs |
| RAM | 2 GB |
| Disco | 10 GB livres |
| Python | 3.11+ (instalado automaticamente se ausente) |
| Acesso | SSH como root ou usuário com sudo |
| Rede | Acesso de saída a `workers.alvify.com.br` na porta 443 |

---

## 1. Registrar o worker no painel admin

Antes de instalar qualquer coisa no VPS, crie o registro do worker na plataforma:

1. Acesse **Admin → Workers** no painel Alvify.
2. Clique em **Novo worker**.
3. Preencha nome, host (IP ou hostname do VPS) e concorrência máxima.
4. Clique em **Criar** — o sistema gera uma **API Key** que é exibida **uma única vez**.
5. Anote:
   - **Worker ID** (UUID)
   - **API Key** gerada

> Guarde a API Key em local seguro. Ela não poderá ser recuperada depois.

---

## 2. Copiar os arquivos para o VPS

O worker precisa de dois diretórios do repositório: `worker/` e `backend/app/`.

> **Atenção:** copiar apenas `worker/` **não é suficiente**. O scraper, o browser pool e o cache estão em `backend/app/core/` e precisam estar presentes para o worker funcionar.

### Opção A — Via SCP (recomendado)

Execute no seu **computador local**, a partir da raiz do repositório:

```bash
# Cria um diretório temporário no VPS e copia os dois diretórios de uma vez
ssh root@<IP-DO-VPS> "mkdir -p /root/alvify-worker-src"
scp -r worker/   root@<IP-DO-VPS>:/root/alvify-worker-src/
scp -r backend/  root@<IP-DO-VPS>:/root/alvify-worker-src/
```

Estrutura esperada no VPS antes de rodar o install:
```
/root/alvify-worker-src/
├── worker/
│   ├── install.sh
│   ├── main.py
│   ├── requirements.txt
│   └── .env.example
└── backend/
    └── app/
        └── core/
            ├── scraper.py
            ├── browser_pool.py
            └── ...
```

### Opção B — Clonar o repositório direto no VPS

```bash
ssh root@<IP-DO-VPS>
git clone --depth=1 <URL-DO-REPO> /root/alvify-worker-src
```

O repositório já contém ambos os diretórios, então nenhuma cópia extra é necessária.

---

## 3. Executar o script de instalação

```bash
ssh root@<IP-DO-VPS>
cd /root/alvify-worker-src/worker
chmod +x install.sh
sudo ./install.sh
```

O script executa automaticamente:

1. Detecção de OS (Ubuntu / Debian / CentOS / Fedora)
2. Instalação de dependências do sistema (Chromium, libs gráficas, Python)
3. Criação do usuário de sistema `alvify` (sem login)
4. Criação do diretório `/opt/alvify-worker/`
5. Criação de virtualenv Python + `pip install`
6. Download e instalação do browser Chromium via Playwright
7. Geração do arquivo `/opt/alvify-worker/.env` (template)
8. Instalação e ativação do serviço systemd `alvify-worker`

---

## 4. Configurar as credenciais

Edite o arquivo `.env` gerado pelo instalador:

```bash
sudo nano /opt/alvify-worker/.env
```

Preencha os campos obrigatórios:

```dotenv
API_URL=https://workers.alvify.com.br

WORKER_ID=<cole-o-UUID-aqui>
WORKER_API_KEY=<cole-a-api-key-aqui>

MAX_CONCURRENCY=2   # jobs paralelos (ajuste conforme RAM disponível)
HEALTH_PORT=8001
VERSION=1.0.0
LOG_LEVEL=INFO

# Opcional: acesso direto ao Redis para cache-refresh jobs
# REDIS_URL=redis://<host>:6379
```

Salve o arquivo (`Ctrl+O`, `Enter`, `Ctrl+X`).

---

## 5. Iniciar o serviço

```bash
sudo systemctl start alvify-worker
sudo systemctl status alvify-worker
```

Saída esperada:

```
● alvify-worker.service - Alvify Remote Scraping Worker
     Active: active (running) since ...
```

Para acompanhar os logs em tempo real:

```bash
sudo journalctl -u alvify-worker -f
```

Linhas normais de startup:

```
INFO  Connected to API as worker <WORKER_ID>
INFO  Polling for jobs (concurrency=2)…
INFO  Heartbeat sent
```

---

## 6. Verificar no painel admin

1. Volte ao **Admin → Workers** no painel Alvify.
2. O worker deve aparecer com status **online** (indicador verde pulsante) em até 30 segundos.
3. O campo **Último heartbeat** é atualizado a cada 15 segundos.

Se o worker aparecer como **offline** após 1 minuto, veja a seção [Resolução de problemas](#resolução-de-problemas).

---

## 7. Ajuste de concorrência

O parâmetro `MAX_CONCURRENCY` controla quantos jobs o worker processa em paralelo. Cada job abre uma janela Chromium. Guia de referência:

| RAM disponível | `MAX_CONCURRENCY` sugerido |
|---|---|
| 1 GB | 1 |
| 2 GB | 2 |
| 4 GB | 3–4 |
| 8 GB+ | 5–8 |

Após alterar, reinicie o serviço:

```bash
sudo systemctl restart alvify-worker
```

---

## 8. Atualizar o worker

Para atualizar para uma nova versão:

```bash
# Copie os novos arquivos para o VPS
scp -r worker/main.py root@<IP>:/opt/alvify-worker/main.py
scp -r backend/app/ root@<IP>:/opt/alvify-worker/app/

# Atualize dependências se necessário
sudo /opt/alvify-worker/venv/bin/pip install -r /opt/alvify-worker/requirements.txt -q

# Reinicie
sudo systemctl restart alvify-worker
```

---

## 9. Desinstalar

```bash
sudo systemctl stop alvify-worker
sudo systemctl disable alvify-worker
sudo rm /etc/systemd/system/alvify-worker.service
sudo systemctl daemon-reload
sudo rm -rf /opt/alvify-worker
sudo userdel alvify
```

---

## Resolução de problemas

### Worker aparece offline no painel

```bash
# Verifique se o serviço está rodando
sudo systemctl status alvify-worker

# Veja os últimos erros
sudo journalctl -u alvify-worker -n 50 --no-pager
```

Causas comuns:
- `WORKER_ID` ou `WORKER_API_KEY` inválidos → `401 Unauthorized` nos logs
- VPS sem acesso à internet ou DNS falhou → `Cannot connect to host workers.alvify.com.br`
- Python ou dependência faltando → `ModuleNotFoundError`

### Erro `401 Unauthorized`

Confirme que o `WORKER_ID` e `WORKER_API_KEY` em `/opt/alvify-worker/.env` correspondem exatamente ao worker cadastrado no painel admin. API Keys são case-sensitive.

### Playwright não encontra o Chromium

```bash
sudo PLAYWRIGHT_BROWSERS_PATH=/opt/alvify-worker/.playwright \
  /opt/alvify-worker/venv/bin/playwright install chromium --with-deps
sudo chown -R alvify:alvify /opt/alvify-worker/.playwright
sudo systemctl restart alvify-worker
```

### Alto uso de memória / OOM

Reduza `MAX_CONCURRENCY` para `1` e reinicie. Cada sessão Chromium consome ~300–500 MB de RAM.

### Verificar conectividade manualmente

```bash
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer <WORKER_API_KEY>" \
  -H "X-Worker-ID: <WORKER_ID>" \
  https://workers.alvify.com.br/internal/workers/jobs/poll
# Esperado: 204 (sem jobs) ou 200 (job disponível)
```

---

## Estrutura de diretórios pós-instalação

```
/opt/alvify-worker/
├── main.py                # processo principal do worker
├── requirements.txt       # dependências Python
├── .env                   # credenciais (chmod 640, dono alvify)
├── venv/                  # virtual environment Python
├── app/                   # módulos do backend (scraper, browser pool, etc.)
│   └── core/
│       ├── scraper.py
│       ├── browser_pool.py
│       └── result_cache.py
└── .playwright/           # browsers Chromium
    └── chromium-*/
```
