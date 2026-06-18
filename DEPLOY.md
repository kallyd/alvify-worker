# Alvify Worker + API-DB — Guia de Deploy e Atualização

## Resumo das Mudanças

### API-DB (api-cnpj.alvify.com.br)

A API de consulta CNPJ foi ampliada com:

| Feature | Descrição |
|---------|-----------|
| **12 novos filtros** | bairro, cep, porte, mei, simples, data_abertura_de/ate, capital_min/max, tem_email, tem_telefone, ddd |
| **Busca por texto (trigram)** | Parâmetro `q` busca substring em razao_social e nome_fantasia |
| **Multi-CNAE** | `cnae=5611201,5611202,5611203` (comma-separated) |
| **Bulk CNPJ** | `POST /v1/empresas/bulk` — até 50 CNPJs por request |
| **Endpoint de contagem** | `GET /v1/empresas/count` — total de matches |
| **has_next na paginação** | Campo `has_next: true/false` no response |
| **Cache otimizado** | List: 24h / CNPJ individual: 7 dias |
| **7 novos indexes** | Suporte performático aos novos filtros |

### Worker (alvify-worker)

O worker foi evoluído com:

| Feature | Descrição |
|---------|-----------|
| **Job routing** | Suporte a 3 tipos de job: `google_maps`, `cadastral`, `empresas_novas` |
| **Enrichment híbrido** | Leads do Maps são cruzados com dados cadastrais da api-db |
| **Score enriquecido** | Score de prospecção considera porte e capital social |
| **Scraper cadastral** | Busca direto na api-db por CNAE/cidade/porte sem Google Maps |
| **Módulo enrichment** | Extrai emails, redes sociais e tecnologias de websites |
| **Diagnóstico digital** | Identifica necessidades de marketing (SEO, site, social, etc.) |
| **Busca empresas novas** | Filtra por data de abertura (últimos 7 dias) |
| **Extração expandida** | place_id, lat/lng, fotos, price_level, description, Google Posts, Facebook, horários, etc. |
| **Grid search** | Divide cidades em quadrantes para 3-5x mais cobertura geográfica |
| **Dedup global** | Redis-backed cross-job dedup com TTL de 30 dias |
| **Validação de qualidade** | Score de qualidade 0-100 com descarte automático de leads ruins |
| **Over-fetch** | Coleta 30% mais URLs para compensar filtragem |
| **Retry melhorado** | Backoff exponencial: 5s → 15s → 45s → 90s no CAPTCHA |

---

## Deploy da API-DB

### 1. Build e deploy

```bash
cd /root/api-db
docker compose build api
docker compose up -d api
```

### 2. Criar novos indexes (se banco já existe)

Conectar ao PostgreSQL e executar:

```sql
CREATE INDEX IF NOT EXISTS idx_empresas_bairro ON empresas (bairro);
CREATE INDEX IF NOT EXISTS idx_empresas_cep_pattern ON empresas (cep text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_empresas_porte ON empresas (porte_empresa);
CREATE INDEX IF NOT EXISTS idx_empresas_data_abertura ON empresas (data_abertura);
CREATE INDEX IF NOT EXISTS idx_empresas_ddd_pattern ON empresas (ddd_telefone1 text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_empresas_municipio_porte ON empresas (municipio, porte_empresa);
CREATE INDEX IF NOT EXISTS idx_empresas_uf_data_abertura ON empresas (uf, data_abertura);
```

> Nota: se o banco for recriado do zero, o `scripts/init.sql` já inclui esses indexes.

### 3. Verificar

```bash
curl https://api-cnpj.alvify.com.br/health
curl "https://api-cnpj.alvify.com.br/v1/empresas?cidade=CURITIBA&cnae=5611201&limit=5"
curl "https://api-cnpj.alvify.com.br/v1/empresas/count?uf=PR&porte=3"
```

---

## Deploy do Worker

### 1. Atualizar arquivos

Copiar para o servidor:
- `main.py`
- `core/cnpj_client.py` (novo)
- `core/enrichment.py` (novo)
- `core/cadastral_scraper.py` (novo)
- `requirements.txt` (atualizado)
- `.env` (atualizar com novas variáveis)

### 2. Instalar dependências

```bash
pip install -r requirements.txt
# ou via Docker:
docker compose build worker
```

### 3. Configurar variáveis de ambiente

Adicionar ao `.env` do worker:

```env
# CNPJ API Integration
CNPJ_API_URL=https://api-cnpj.alvify.com.br
CNPJ_API_KEY=<sua-chave-aqui>
CNPJ_ENRICHMENT_ENABLED=true
CNPJ_ENRICHMENT_TIMEOUT=5

# Quality & Dedup
GLOBAL_DEDUP_ENABLED=true
QUALITY_SCORE_MIN=20
GRID_SEARCH_THRESHOLD=60
```

### 4. Reiniciar

```bash
# Se systemd:
sudo systemctl restart alvify-worker

# Se Docker:
docker compose restart worker
```

---

## Novos Tipos de Job

### google_maps (padrão — comportamento existente)

```json
{
  "job_id": "uuid",
  "type": "google_maps",
  "keyword": "restaurante",
  "city": "Curitiba",
  "state": "PR",
  "max_results": 50
}
```

### cadastral (busca na base de 68M empresas)

```json
{
  "job_id": "uuid",
  "type": "cadastral",
  "keyword": "5611201",
  "city": "Curitiba",
  "state": "PR",
  "max_results": 100,
  "porte": 3,
  "tem_email": false,
  "ddd": "41"
}
```

O `keyword` pode ser:
- Um código CNAE de 7 dígitos (ex: `"5611201"` = restaurantes)
- Texto livre para busca por nome (ex: `"padaria"`)

Filtros opcionais adicionais no job dict:
- `porte`: 1 (ME), 3 (EPP), 5 (Demais)
- `mei`: true/false
- `simples`: true/false
- `tem_email`: true/false
- `tem_telefone`: true/false
- `ddd`: código DDD (ex: "41", "11")
- `data_abertura_de`: YYYY-MM-DD
- `data_abertura_ate`: YYYY-MM-DD

### empresas_novas (empresas abertas recentemente)

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

Por padrão filtra empresas abertas nos últimos 7 dias. Aceita os mesmos filtros opcionais do tipo `cadastral`.

---

## Endpoints da API-DB (referência rápida)

### GET /v1/empresas

Busca paginada com filtros.

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `cnpj` | string | CNPJ exato (14 dígitos) |
| `razao_social` | string | Prefixo da razão social |
| `nome_fantasia` | string | Prefixo do nome fantasia |
| `cidade` | string | Município (ex: "Curitiba") |
| `uf` | string | UF (ex: "PR") |
| `cnae` | string | CNAE(s) separados por vírgula |
| `situacao` | int | 2=Ativa |
| `bairro` | string | Bairro exato |
| `cep` | string | Prefixo do CEP |
| `porte` | int | 1=ME, 3=EPP, 5=Demais |
| `mei` | bool | true/false |
| `simples` | bool | true/false |
| `data_abertura_de` | string | YYYY-MM-DD (início) |
| `data_abertura_ate` | string | YYYY-MM-DD (fim) |
| `capital_min` | float | Capital social mínimo |
| `capital_max` | float | Capital social máximo |
| `tem_email` | bool | Tem/não tem email |
| `tem_telefone` | bool | Tem/não tem telefone |
| `ddd` | string | Prefixo do DDD |
| `q` | string | Busca por substring (min 3 chars) |
| `limit` | int | Registros por página (max 200) |
| `cursor` | int | Cursor para paginação |

### GET /v1/empresas/count

Mesmos filtros, retorna `{"success": true, "count": N}`.

### GET /v1/empresas/:cnpj

Busca empresa por CNPJ exato.

### POST /v1/empresas/bulk

```json
{"cnpjs": ["12345678000100", "98765432000199"]}
```

Máximo 50 CNPJs. Retorna array de empresas encontradas.

---

## Códigos CNAE Comuns (para referência)

| CNAE | Atividade |
|------|-----------|
| 5611201 | Restaurantes |
| 5611203 | Lanchonetes |
| 4781400 | Vestuário |
| 9602501 | Cabeleireiros |
| 8630503 | Odontologia |
| 8630504 | Psicologia |
| 4771701 | Farmácias |
| 4744099 | Materiais de construção |
| 5620104 | Padarias |
| 9313100 | Academias |
| 5611204 | Bares |
| 4753900 | Tapetes e cortinas |
| 4712100 | Minimercados |
| 8622400 | Clínicas médicas |

---

## Arquitetura Pós-Atualização

```
┌─────────────────────────────────────────────────────────────┐
│                    Alvify Platform API                        │
│              workers.alvify.com.br                            │
└──────────────────────┬──────────────────────────────────────┘
                       │  poll / submit leads
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   alvify-worker                               │
│                                                              │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ Google Maps │  │ Cadastral Scraper│  │  Enrichment   │  │
│  │  Scraper    │  │  (api-db query)  │  │  (websites)   │  │
│  └──────┬──────┘  └────────┬─────────┘  └───────┬───────┘  │
│         │                  │                     │           │
│         └──────────────────┼─────────────────────┘           │
│                            │                                 │
│                   ┌────────▼────────┐                        │
│                   │  CNPJ Client    │                        │
│                   │  (hybrid enrich)│                        │
│                   └────────┬────────┘                        │
└────────────────────────────┼────────────────────────────────┘
                             │
                ┌────────────▼────────────┐
                │      api-db             │
                │  api-cnpj.alvify.com.br │
                │  68M empresas (PG+Redis)│
                └─────────────────────────┘
```

---

## Diagnóstico Digital (Marketing Feature)

Cada lead agora recebe um **diagnóstico digital automático** que identifica exatamente quais serviços de marketing a empresa precisa.

### Campos adicionados ao lead:

```json
{
  "digital_diagnosis": {
    "needs_website": false,
    "needs_seo": true,
    "needs_social_media": true,
    "needs_paid_traffic": true,
    "needs_redesign": false,
    "needs_automation": true,
    "needs_google_business": false,
    "digital_maturity_score": 35,
    "issues": [
      "Sem meta description",
      "Sem Open Graph tags",
      "Sem presença no Instagram",
      "Sem ferramentas de tracking/analytics",
      "Sem ferramenta de automação/email marketing"
    ],
    "suggested_services": [
      "otimização SEO",
      "gestão de redes sociais",
      "configuração de tráfego pago",
      "automação de marketing"
    ]
  },
  "suggested_services": ["otimização SEO", "gestão de redes sociais", ...],
  "digital_maturity_score": 35,
  "tags": ["sem instagram", "precisa de SEO", "precisa de social media", 
           "precisa de tráfego pago", "precisa de automação", 
           "maturidade digital: baixa", "EPP", "capital alto"]
}
```

### O que é detectado:

| Necessidade | Como é detectada |
|-------------|-----------------|
| **Precisa de site** | Empresa não tem website |
| **Precisa de SEO** | Sem meta description, sem H1, sem OG tags, sem canonical, sem Schema.org (2+ problemas) |
| **Precisa de social media** | Sem Instagram ou nenhuma rede social |
| **Precisa de tráfego pago** | Tem site mas sem Google Analytics/GTM/Facebook Pixel |
| **Precisa de redesign** | Site sem HTTPS, sem viewport (não-responsivo), copyright antigo (3+ anos) |
| **Precisa de automação** | Sem RD Station, HubSpot ou Mailchimp |
| **Precisa de Google Meu Negócio** | Sem avaliações ou < 10 reviews |

### Score de Maturidade Digital (0-100):

| Faixa | Label | Significado |
|-------|-------|-------------|
| 0-20 | Crítica | Praticamente sem presença digital |
| 21-40 | Baixa | Presença básica, muitas lacunas |
| 41-60 | Média | Tem o básico mas falta otimização |
| 61-80 | Boa | Presença sólida, pode melhorar |
| 81-100 | Alta | Madura digitalmente |

### Como usar no frontend:

A agência pode filtrar leads por:
- `suggested_services` — "me dê leads que precisam de SEO"
- `digital_maturity_score` — "me dê leads com score < 40" (mais oportunidade)
- `digital_diagnosis.needs_*` — filtros booleanos diretos
- `tags` contendo "precisa de ..." — busca textual simples
