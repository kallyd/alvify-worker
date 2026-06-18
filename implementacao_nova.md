# Adaptação do Servidor Principal — Novos Campos e Tipos de Job

> Os workers remotos foram atualizados e agora enviam dados enriquecidos.
> O servidor principal (API que recebe os leads + frontend) precisa ser adaptado
> para aceitar, armazenar e expor essas informações.

---

## 1. Contexto

O worker remoto (`alvify-worker`) foi evoluído com:
- **11 campos novos** por lead (place_id, lat/lng, fotos, socials, diagnóstico digital, etc.)
- **3 tipos de job** (google_maps, cadastral, empresas_novas)
- **Diagnóstico digital** (identifica necessidades de marketing do lead)
- **Score de qualidade** (0-100, leads < 20 são descartados no worker)
- **Enrichment híbrido** (cruzamento Google Maps + base CNPJ de 68M empresas)

O servidor principal precisa ser adaptado para receber e usar esses dados.

---

## 2. Novos Campos nos Leads (que o worker agora envia)

O worker envia leads via `POST /internal/workers/jobs/{job_id}/leads/batch` e `POST /internal/workers/jobs/{job_id}/lead`.

### 2.1 Campos que já existiam (sem mudança)
```
name, category, address, neighborhood, city, state, phone, website,
instagram, rating, review_count, score, digital_status, tags, hours,
photo_url, status, source
```

### 2.2 Campos NOVOS (precisam ser aceitos e armazenados)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `place_id` | string | Google Maps place ID (ex: "0x94ce504b:0x123abc") |
| `latitude` | float | Coordenada geográfica |
| `longitude` | float | Coordenada geográfica |
| `photos_count` | int | Número de fotos no Google Maps |
| `price_level` | string | Faixa de preço ("$", "$$", "$$$") |
| `description` | string | Descrição do negócio (max 300 chars) |
| `has_google_posts` | bool | Se tem Google Posts ativos |
| `facebook` | string | URL do Facebook |
| `socials` | object | `{"instagram": "url", "facebook": "url", "twitter": "url", ...}` |
| `service_options` | array[string] | ["Retirada", "Entrega", "Consumo no local"] |
| `full_hours` | object | `{"Seg": "11:00–22:00", "Ter": "11:00–22:00", ...}` |
| `owner_verified` | bool | Se o dono reivindicou o perfil no Google |
| `cnpj` | string | CNPJ da empresa (14 dígitos, vindo da base cadastral) |
| `razao_social` | string | Razão social oficial |
| `cnae_fiscal` | string | Código CNAE (7 dígitos) |
| `capital_social` | float | Capital social em R$ |
| `porte_empresa` | int | 1=ME, 3=EPP, 5=Demais |
| `data_abertura` | string | Data de abertura (YYYY-MM-DD) |
| `opcao_mei` | bool | Se é MEI |
| `emails` | array[string] | Emails extraídos do website |
| `technologies` | array[string] | Tecnologias detectadas (WordPress, React, etc.) |
| `digital_diagnosis` | object | Diagnóstico completo (ver 2.3) |
| `suggested_services` | array[string] | Serviços de marketing recomendados |
| `digital_maturity_score` | int | Score de maturidade digital (0-100) |
| `quality_score` | int | Score de qualidade do dado (0-100) |
| `quality_issues` | array[string] | Problemas de qualidade identificados |

### 2.3 Estrutura do `digital_diagnosis`

```json
{
  "needs_website": true,
  "needs_seo": false,
  "needs_social_media": true,
  "needs_paid_traffic": true,
  "needs_redesign": false,
  "needs_automation": true,
  "needs_google_business": false,
  "digital_maturity_score": 35,
  "issues": [
    "Empresa não possui website",
    "Sem presença no Instagram",
    "Sem ferramentas de tracking/analytics",
    "Sem ferramenta de automação/email marketing"
  ],
  "suggested_services": [
    "criação de site",
    "gestão de redes sociais",
    "configuração de tráfego pago",
    "automação de marketing"
  ]
}
```

---

## 3. Novos Tipos de Job (que o servidor precisa despachar)

Atualmente o servidor só despacha jobs do tipo `google_maps`. Agora pode despachar 3 tipos:

### 3.1 google_maps (existente — sem mudança no dispatch)
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

### 3.2 cadastral (NOVO — busca na base CNPJ sem Google Maps)
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

O `keyword` pode ser:
- Código CNAE de 7 dígitos (ex: "5611201" = restaurantes)
- Texto livre (ex: "padaria") — busca por nome fantasia

Filtros opcionais no job dict:
- `porte`: 1 (ME), 3 (EPP), 5 (Demais)
- `mei`: true/false
- `simples`: true/false
- `tem_email`: true/false
- `tem_telefone`: true/false
- `ddd`: código DDD (ex: "41")
- `data_abertura_de`: YYYY-MM-DD
- `data_abertura_ate`: YYYY-MM-DD

### 3.3 empresas_novas (NOVO — empresas abertas recentemente)
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

Filtra automaticamente empresas abertas nos últimos 7 dias. Aceita mesmos filtros do `cadastral`.

---

## 4. Adaptações Necessárias no Servidor Principal

### 4.1 Banco de dados (schema)

Adicionar colunas na tabela de leads (ou usar JSONB para campos flexíveis):

```sql
-- Novos campos do lead
ALTER TABLE leads ADD COLUMN IF NOT EXISTS place_id VARCHAR(64);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS photos_count INTEGER DEFAULT 0;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS price_level VARCHAR(10);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS has_google_posts BOOLEAN DEFAULT FALSE;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS facebook VARCHAR(255);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS socials JSONB DEFAULT '{}';
ALTER TABLE leads ADD COLUMN IF NOT EXISTS service_options JSONB DEFAULT '[]';
ALTER TABLE leads ADD COLUMN IF NOT EXISTS full_hours JSONB DEFAULT '{}';
ALTER TABLE leads ADD COLUMN IF NOT EXISTS owner_verified BOOLEAN DEFAULT FALSE;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS cnpj VARCHAR(14);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS razao_social VARCHAR(255);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS cnae_fiscal VARCHAR(7);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS capital_social DOUBLE PRECISION DEFAULT 0;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS porte_empresa INTEGER DEFAULT 0;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS data_abertura DATE;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS opcao_mei BOOLEAN DEFAULT FALSE;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS emails JSONB DEFAULT '[]';
ALTER TABLE leads ADD COLUMN IF NOT EXISTS technologies JSONB DEFAULT '[]';
ALTER TABLE leads ADD COLUMN IF NOT EXISTS digital_diagnosis JSONB DEFAULT '{}';
ALTER TABLE leads ADD COLUMN IF NOT EXISTS suggested_services JSONB DEFAULT '[]';
ALTER TABLE leads ADD COLUMN IF NOT EXISTS digital_maturity_score INTEGER DEFAULT 0;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS quality_score INTEGER DEFAULT 0;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS quality_issues JSONB DEFAULT '[]';

-- Indexes úteis para filtros no frontend
CREATE INDEX IF NOT EXISTS idx_leads_cnpj ON leads (cnpj) WHERE cnpj IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_digital_maturity ON leads (digital_maturity_score);
CREATE INDEX IF NOT EXISTS idx_leads_quality ON leads (quality_score);
CREATE INDEX IF NOT EXISTS idx_leads_porte ON leads (porte_empresa) WHERE porte_empresa > 0;
CREATE INDEX IF NOT EXISTS idx_leads_coords ON leads (latitude, longitude) WHERE latitude IS NOT NULL;
```

### 4.2 API de recepção de leads (endpoint batch/lead)

Os endpoints `POST /internal/workers/jobs/{job_id}/leads/batch` e `/lead` precisam **aceitar os campos novos** no JSON e salvá-los:

1. Adicionar os campos ao struct/model do lead
2. Mapear para as novas colunas no banco
3. Workers antigos continuam enviando só campos básicos — campos novos ficam NULL/default

### 4.3 Dispatch de jobs (novo campo `type`)

O endpoint que cria/despacha jobs precisa suportar o campo `type` no job dict:

```json
{
  "job_id": "...",
  "type": "cadastral",
  "keyword": "...",
  "city": "...",
  ...
}
```

Se `type` ausente → worker assume `"google_maps"` (backward compat).

### 4.4 Frontend — novos filtros para a agência

| Filtro | Campo | Uso |
|--------|-------|-----|
| "Precisa de site" | `digital_diagnosis.needs_website = true` | Agências de web design |
| "Precisa de SEO" | `digital_diagnosis.needs_seo = true` | Agências de SEO |
| "Precisa de social media" | `digital_diagnosis.needs_social_media = true` | Social media managers |
| "Precisa de tráfego pago" | `digital_diagnosis.needs_paid_traffic = true` | Agências de performance |
| "Maturidade digital baixa" | `digital_maturity_score < 40` | Leads com mais oportunidade |
| "Porte EPP" | `porte_empresa = 3` | Empresas de tamanho ideal |
| "Capital alto" | `capital_social >= 100000` | Empresas com budget |
| "Tem localização" | `latitude IS NOT NULL` | Visualizar no mapa |
| "Com CNPJ verificado" | `cnpj IS NOT NULL` | Dados cadastrais confirmados |

---

## 5. Fluxo Completo (Antes vs Depois)

### ANTES:
```
Worker scrapa Maps → envia lead básico → servidor salva → frontend mostra lista
```

### DEPOIS:
```
Worker scrapa Maps
  → enriquece com base CNPJ (68M empresas)
  → analisa website (emails, tech, SEO)
  → calcula diagnóstico digital
  → valida qualidade
  → envia lead completo com 30+ campos

Worker recebe job "cadastral"
  → consulta base CNPJ diretamente (sem browser)
  → retorna empresas filtradas por CNAE/porte/cidade
  → rápido: ~200 leads em segundos

Servidor recebe leads enriquecidos
  → salva todos os campos novos
  → frontend filtra por necessidade de serviço
  → agência recebe leads pré-qualificados com argumento de venda pronto
```

---

## 6. Backward Compatibility

- **Workers antigos** continuam funcionando — enviam só campos básicos, novos ficam NULL
- **Jobs sem `type`** = tratados como `google_maps`
- **Campos novos são todos opcionais** — nenhum é NOT NULL
- **O worker descarta leads ruins** (quality < 20) — menos lixo chega ao servidor

---

## 7. Checklist de Ações

1. [ ] Rodar migrations SQL (seção 4.1) para adicionar colunas novas
2. [ ] Atualizar struct/model do lead para aceitar campos novos no JSON
3. [ ] Adicionar campo `type` ao dispatch de jobs (default "google_maps")
4. [ ] Criar interface no admin para despachar jobs "cadastral" e "empresas_novas"
5. [ ] Adicionar filtros no frontend: diagnóstico digital, porte, maturidade, serviços sugeridos
6. [ ] (Opcional) Visualização de mapa usando lat/lng
7. [ ] (Opcional) Exibir diagnóstico digital na ficha do lead

---

## 8. CNAEs Comuns (referência para interface de criação de jobs cadastrais)

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
| 4712100 | Minimercados |
| 8622400 | Clínicas médicas |
