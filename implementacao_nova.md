# Implementação Nova — Instruções para o Servidor de Origem

> Este documento detalha TODAS as mudanças feitas nos repositórios `api-db` e `alvify-worker`.
> Use-o para aplicar as atualizações no servidor de produção.

---

## 1. Visão Geral

Dois sistemas foram atualizados:

| Sistema | Localização | Stack |
|---------|------------|-------|
| **api-db** | `/root/api-db` | Go 1.22 + Fiber + PostgreSQL + Redis |
| **alvify-worker** | `/root/alvify-worker` | Python 3.11 + Playwright + aiohttp + Redis |

---

## 2. API-DB — Mudanças

### 2.1 Arquivos modificados

| Arquivo | Mudança |
|---------|---------|
| `internal/models/empresa.go` | +12 campos em `EmpresaFilters` (Bairro, CEP, Porte, OpcaoMEI, OpcaoSimples, DataAberturaDE/ATE, CapitalMin/Max, TemEmail, TemTelefone, DDD, Q) |
| `internal/handlers/empresa_handler.go` | +helpers `queryBool`, `queryFloat`, novos handlers `BulkGet`, `Count`, parsing dos 12 novos filtros |
| `internal/repository/empresa_repository.go` | +interface `FindByCNPJs`, `Count` |
| `internal/repository/postgres/empresa_postgres.go` | +`FindByCNPJs`, `Count`, novos filtros no `FindAll` (multi-CNAE, trigram, LIMIT+1 pattern) |
| `internal/service/empresa_service.go` | +`BulkGetByCNPJ`, `Count`, cache 24h/7d, `HasNext`, cache key expandida |
| `pkg/response/response.go` | +`HasNext bool`, +struct `Count` |
| `cmd/server/main.go` | +rotas `/empresas/count`, `/empresas/bulk`, CORS POST+X-API-Key |
| `scripts/init.sql` | +7 novos indexes |

### 2.2 Novos endpoints

```
GET  /v1/empresas          — busca paginada (12 filtros novos + q + multi-cnae)
GET  /v1/empresas/count    — contagem total com mesmos filtros
POST /v1/empresas/bulk     — busca múltiplos CNPJs (max 50)
GET  /v1/empresas/:cnpj    — busca por CNPJ (existente, sem mudanças)
```

### 2.3 Novos filtros disponíveis

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `bairro` | string | Bairro exato |
| `cep` | string | Prefixo do CEP (ex: "80000") |
| `porte` | int | 1=ME, 3=EPP, 5=Demais |
| `mei` | bool | Opção MEI |
| `simples` | bool | Opção Simples Nacional |
| `data_abertura_de` | string | YYYY-MM-DD início |
| `data_abertura_ate` | string | YYYY-MM-DD fim |
| `capital_min` | float | Capital social mínimo |
| `capital_max` | float | Capital social máximo |
| `tem_email` | bool | Tem/não tem email na RF |
| `tem_telefone` | bool | Tem/não tem telefone |
| `ddd` | string | Prefixo DDD (ex: "41") |
| `q` | string | Busca textual trigram (min 3 chars) |
| `cnae` | string | Aceita múltiplos: "5611201,5611202" |

### 2.4 SQL — Executar no PostgreSQL de produção

```sql
CREATE INDEX IF NOT EXISTS idx_empresas_bairro ON empresas (bairro);
CREATE INDEX IF NOT EXISTS idx_empresas_cep_pattern ON empresas (cep text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_empresas_porte ON empresas (porte_empresa);
CREATE INDEX IF NOT EXISTS idx_empresas_data_abertura ON empresas (data_abertura);
CREATE INDEX IF NOT EXISTS idx_empresas_ddd_pattern ON empresas (ddd_telefone1 text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_empresas_municipio_porte ON empresas (municipio, porte_empresa);
CREATE INDEX IF NOT EXISTS idx_empresas_uf_data_abertura ON empresas (uf, data_abertura);
```

### 2.5 Deploy da api-db

```bash
cd /root/api-db
docker compose build api
docker compose up -d api

# Verificar:
curl https://api-cnpj.alvify.com.br/health
curl "https://api-cnpj.alvify.com.br/v1/empresas?cidade=CURITIBA&porte=3&limit=3"
curl "https://api-cnpj.alvify.com.br/v1/empresas/count?uf=PR&cnae=5611201"
curl -X POST https://api-cnpj.alvify.com.br/v1/empresas/bulk \
  -H "Content-Type: application/json" \
  -d '{"cnpjs":["12345678000100"]}'
```

---

## 3. Worker — Mudanças

### 3.1 Arquivos NOVOS (copiar para o servidor)

| Arquivo | Descrição |
|---------|-----------|
| `core/cnpj_client.py` | Cliente HTTP para api-db (search, bulk, count, get_by_cnpj) |
| `core/enrichment.py` | Enrichment de websites (emails, socials, tecnologias) + diagnóstico digital |
| `core/cadastral_scraper.py` | Scraper que busca na api-db sem Google Maps |
| `core/grid_search.py` | Grid search geográfico (3x3 quadrantes) |
| `core/validators.py` | Validação de qualidade (telefone, site, endereço, nome) |

### 3.2 Arquivos MODIFICADOS

| Arquivo | Mudanças |
|---------|----------|
| `main.py` | Job routing (google_maps/cadastral/empresas_novas), CNPJ client global, global dedup, enrichment híbrido, diagnóstico digital, validação de qualidade, grid search, `_boost_score_with_cadastral()` |
| `core/scraper.py` | +11 campos novos (place_id, lat/lng, photos_count, price_level, description, has_google_posts, facebook, socials, service_options, full_hours, owner_verified), over-fetch 1.3x, backoff exponencial melhorado |
| `core/dedup.py` | +`GlobalDedup` class (Redis, TTL 30 dias) |
| `.env.example` | +7 variáveis novas |
| `requirements.txt` | +`beautifulsoup4` |

### 3.3 Variáveis de ambiente NOVAS

Adicionar ao `.env` do worker:

```env
# CNPJ API Integration
CNPJ_API_URL=https://api-cnpj.alvify.com.br
CNPJ_API_KEY=
CNPJ_ENRICHMENT_ENABLED=true
CNPJ_ENRICHMENT_TIMEOUT=5

# Quality & Dedup
GLOBAL_DEDUP_ENABLED=true
QUALITY_SCORE_MIN=20
GRID_SEARCH_THRESHOLD=60
```

### 3.4 Nova dependência

```bash
pip install beautifulsoup4
# ou rebuild do container:
docker compose build worker
```

### 3.5 Deploy do worker

```bash
cd /root/alvify-worker

# Instalar dependência nova
pip install -r requirements.txt

# Reiniciar
sudo systemctl restart alvify-worker
# ou Docker:
docker compose restart worker

# Verificar logs:
journalctl -u alvify-worker -f --no-pager | head -20
```

---

## 4. Novos Tipos de Job

### 4.1 google_maps (padrão — melhorado)

O comportamento padrão agora inclui:
- Extração expandida (11 campos novos)
- Over-fetch (30% mais URLs para compensar filtragem)
- Grid search automático quando `max_results > 60` em cidades conhecidas
- Enrichment híbrido (cruzamento com api-db)
- Diagnóstico digital (tags de necessidades de marketing)
- Validação de qualidade (descarte de leads score < 20)
- Dedup global cross-job (Redis)

```json
{
  "job_id": "uuid",
  "type": "google_maps",
  "keyword": "restaurante",
  "city": "Curitiba",
  "state": "PR",
  "max_results": 100
}
```

### 4.2 cadastral (busca direta na base CNPJ)

```json
{
  "job_id": "uuid",
  "type": "cadastral",
  "keyword": "5611201",
  "city": "Curitiba",
  "state": "PR",
  "max_results": 200,
  "porte": 3,
  "tem_email": false,
  "ddd": "41"
}
```

### 4.3 empresas_novas (últimos 7 dias)

```json
{
  "job_id": "uuid",
  "type": "empresas_novas",
  "keyword": "5611201",
  "city": "São Paulo",
  "state": "SP",
  "max_results": 50
}
```

---

## 5. Pipeline de Processamento por Lead

Cada lead passa por:

```
1. Global Dedup (Redis 30d)     → skip se já enviado em outro job
2. Local Dedup (memória)        → skip se duplicado no mesmo job
3. Website Enrichment           → emails, sociais, tecnologias
4. CNPJ Enrichment              → cruzar com dados cadastrais (CNPJ, porte, capital)
5. Score Boost                  → ajustar score com dados cadastrais
6. Diagnóstico Digital          → needs_website, needs_seo, needs_social, etc.
7. Validação de Qualidade       → score 0-100, descarta < 20
8. Batch Submit                 → envia ao backend em lotes de 50
```

---

## 6. Campos do Lead (Formato Final)

```json
{
  "name": "Restaurante Exemplo",
  "category": "Restaurante",
  "address": "Rua XV de Novembro, 100",
  "neighborhood": "Centro",
  "city": "Curitiba",
  "state": "PR",
  "phone": "+5541999998888",
  "website": "restauranteexemplo.com.br",
  "instagram": "https://instagram.com/restauranteexemplo",
  "facebook": "https://facebook.com/restauranteexemplo",
  "socials": {
    "instagram": "https://instagram.com/restauranteexemplo",
    "facebook": "https://facebook.com/restauranteexemplo"
  },
  "rating": 4.5,
  "review_count": 230,
  "score": 87,
  "digital_status": "good",
  "tags": ["sem site próprio", "precisa de SEO", "EPP", "maturidade digital: média"],

  "place_id": "0x94ce504b5e60b0ad:0x123abc",
  "latitude": -25.4284,
  "longitude": -49.2733,
  "photos_count": 45,
  "price_level": "$$",
  "description": "Restaurante especializado em comida caseira...",
  "has_google_posts": true,
  "service_options": ["Retirada", "Entrega", "Consumo no local"],
  "full_hours": {"Seg": "11:00–22:00", "Ter": "11:00–22:00"},
  "owner_verified": true,
  "hours": "11:00–22:00",
  "photo_url": "https://lh3.googleusercontent.com/...",

  "cnpj": "12345678000100",
  "razao_social": "RESTAURANTE EXEMPLO LTDA",
  "cnae_fiscal": "5611201",
  "capital_social": 150000.0,
  "porte_empresa": 3,
  "data_abertura": "2019-03-15",
  "opcao_mei": false,

  "emails": ["contato@restauranteexemplo.com.br"],
  "technologies": ["WordPress", "Google Analytics", "WhatsApp Widget"],

  "digital_diagnosis": {
    "needs_website": false,
    "needs_seo": true,
    "needs_social_media": false,
    "needs_paid_traffic": true,
    "needs_redesign": false,
    "needs_automation": true,
    "needs_google_business": false,
    "digital_maturity_score": 55,
    "issues": ["Sem meta description", "Sem ferramentas de tracking"],
    "suggested_services": ["otimização SEO", "configuração de tráfego pago", "automação de marketing"]
  },
  "suggested_services": ["otimização SEO", "configuração de tráfego pago"],
  "digital_maturity_score": 55,

  "quality_score": 85,
  "quality_issues": [],

  "source": "google_maps",
  "status": "new"
}
```

---

## 7. Grid Search — Cidades Suportadas

O grid search (3x3 quadrantes) é ativado automaticamente para `max_results > 60` nas seguintes cidades:

São Paulo, Campinas, Guarulhos, Santos, Sorocaba, Ribeirão Preto, São José dos Campos, Osasco, Santo André, São Bernardo do Campo, Rio de Janeiro, Niterói, Belo Horizonte, Uberlândia, Juiz de Fora, Curitiba, Londrina, Maringá, Florianópolis, Joinville, Porto Alegre, Caxias do Sul, Salvador, Fortaleza, Recife, Natal, João Pessoa, Maceió, São Luís, Teresina, Aracaju, Brasília, Goiânia, Campo Grande, Cuiabá, Manaus, Belém.

Para outras cidades, o scraper usa busca padrão (scroll simples).

---

## 8. Variáveis de Ambiente Completas do Worker

```env
# === Existentes ===
API_URL=https://workers.alvify.com.br
WORKER_ID=<uuid>
WORKER_API_KEY=<key>
MAX_CONCURRENCY=2
MAX_BROWSER_SLOTS=6
VERSION=1.0.0
HEALTH_PORT=8001
LOG_LEVEL=INFO
REDIS_URL=redis://localhost:6379/0

# === NOVAS ===
CNPJ_API_URL=https://api-cnpj.alvify.com.br
CNPJ_API_KEY=
CNPJ_ENRICHMENT_ENABLED=true
CNPJ_ENRICHMENT_TIMEOUT=5
GLOBAL_DEDUP_ENABLED=true
QUALITY_SCORE_MIN=20
GRID_SEARCH_THRESHOLD=60
```

---

## 9. Checklist de Deploy

- [ ] Executar SQL dos novos indexes no PostgreSQL
- [ ] Rebuild e restart da api-db
- [ ] Verificar `GET /health` da api-db
- [ ] Copiar novos arquivos para o worker (5 arquivos em `core/`)
- [ ] Copiar `main.py`, `core/scraper.py`, `core/dedup.py` atualizados
- [ ] Atualizar `.env` com variáveis novas
- [ ] `pip install beautifulsoup4` (ou rebuild container)
- [ ] Restart do worker
- [ ] Verificar logs: `journalctl -u alvify-worker -f`
- [ ] Testar job google_maps (max_results=20, deve funcionar como antes + campos novos)
- [ ] Testar job cadastral (keyword=5611201, city=Curitiba)
- [ ] Verificar Redis: `redis-cli KEYS "alvify:dedup:*" | wc -l` (deve crescer após jobs)

---

## 10. Rollback

Se algo falhar:

```bash
# api-db: reverter para imagem anterior
docker compose down api
docker compose up -d api  # com tag anterior

# worker: os campos novos são opcionais — backend ignora campos desconhecidos
# Para rollback completo, restaurar main.py e core/ do backup
```

As mudanças são 100% backward-compatible:
- Novos filtros são opcionais (não enviá-los = comportamento anterior)
- Novos campos nos leads são ignorados pelo backend se não reconhecidos
- Grid search, dedup global e validators podem ser desabilitados via env vars
