# Prompt para o Agente do Servidor de Origem

## Situação Atual

Os códigos estão prontos e commitados:

### 1. alvify-worker (já pushado)
- **Repo:** `https://github.com/kallyd/alvify-worker.git`
- **Branch:** `feat/worker-evolution`
- **12 arquivos alterados, 2733 linhas adicionadas**

Para puxar:
```bash
cd /root/alvify-worker  # ou onde estiver o worker
git fetch origin
git checkout feat/worker-evolution
```

### 2. api-db (precisa criar o repo no GitHub)
- O repo `kallyd/api-db` **não existe ainda** no GitHub
- O código está commitado localmente em `/root/api-db` nesta máquina
- **28 arquivos, 4039 linhas**

Para criar e pushar (executar na máquina onde o código está):
```bash
# Instalar gh CLI se necessário:
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
sudo apt update && sudo apt install gh -y
gh auth login

# Criar e pushar:
cd /root/api-db
gh repo create kallyd/api-db --private --source=. --push
```

Ou manualmente:
1. Criar repo `api-db` em https://github.com/kallyd?tab=repositories → New
2. Depois:
```bash
cd /root/api-db
git push -u origin main
```

---

## O que fazer no servidor de produção

### Passo 1 — API-DB
```bash
# Se repo já existe no GitHub:
git clone https://github.com/kallyd/api-db.git /root/api-db
cd /root/api-db
docker compose build api
docker compose up -d api

# Criar indexes no PostgreSQL:
psql -h localhost -U postgres -d cnpj -f scripts/init.sql
```

### Passo 2 — Worker
```bash
cd /root/alvify-worker  # ou onde estiver
git fetch origin
git checkout feat/worker-evolution

# Instalar nova dependência
pip install beautifulsoup4

# Atualizar .env com novas variáveis:
cat >> .env << 'EOF'
CNPJ_API_URL=https://api-cnpj.alvify.com.br
CNPJ_API_KEY=
CNPJ_ENRICHMENT_ENABLED=true
CNPJ_ENRICHMENT_TIMEOUT=5
GLOBAL_DEDUP_ENABLED=true
QUALITY_SCORE_MIN=20
GRID_SEARCH_THRESHOLD=60
EOF

# Reiniciar
sudo systemctl restart alvify-worker
```

### Passo 3 — Verificar
```bash
# API-DB
curl https://api-cnpj.alvify.com.br/health
curl "https://api-cnpj.alvify.com.br/v1/empresas?cidade=CURITIBA&cnae=5611201&limit=2"

# Worker (deve iniciar sem erros)
journalctl -u alvify-worker -f --no-pager | head -20
```

---

## Documentação completa

O arquivo `implementacao_nova.md` no branch `feat/worker-evolution` do repo alvify-worker contém:
- Todas as mudanças detalhadas (api-db + worker)
- Formato final do lead JSON com todos os campos
- Pipeline completo de processamento
- Checklist de deploy
- Instruções de rollback
