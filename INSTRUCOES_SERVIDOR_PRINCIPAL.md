# Instruções para o Servidor Principal — Configuração + WebSocket + Teste

> Este documento é para o agente que gerencia o servidor principal (API + frontend).
> O worker remoto já está atualizado e pronto para enviar dados enriquecidos.
> Você precisa adaptar o servidor para receber, armazenar e testar.

---

## 1. O que o Worker Agora Envia

O worker envia leads via:
- `POST /internal/workers/jobs/{job_id}/leads/batch` (array de leads)
- `POST /internal/workers/jobs/{job_id}/lead` (lead individual)
- Progresso via: `POST /internal/workers/jobs/{job_id}/progress`

Cada lead agora tem **30+ campos** (antes eram ~15). Os novos campos estão listados na seção 3.

---

## 2. Ações Necessárias

### 2.1 Banco de Dados — Migrations

Execute no PostgreSQL do servidor principal:

```sql
-- Novos campos do lead (todos opcionais — não quebra workers antigos)
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

-- Indexes para performance
CREATE INDEX IF NOT EXISTS idx_leads_cnpj ON leads (cnpj) WHERE cnpj IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_digital_maturity ON leads (digital_maturity_score);
CREATE INDEX IF NOT EXISTS idx_leads_quality ON leads (quality_score);
CREATE INDEX IF NOT EXISTS idx_leads_porte ON leads (porte_empresa) WHERE porte_empresa > 0;
CREATE INDEX IF NOT EXISTS idx_leads_coords ON leads (latitude, longitude) WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_place_id ON leads (place_id) WHERE place_id IS NOT NULL;
```

### 2.2 API — Aceitar Campos Novos

Atualizar o model/struct do Lead para incluir os novos campos. O endpoint de ingest (`/internal/workers/jobs/{job_id}/leads/batch`) deve salvar todos os campos que receber.

### 2.3 Dispatch de Jobs — Campo `type`

O job dict agora tem um campo `type`. Adicionar suporte:

```json
{
  "job_id": "uuid",
  "type": "google_maps",       // ou "cadastral" ou "empresas_novas"
  "keyword": "restaurante",
  "city": "Curitiba",
  "state": "PR",
  "max_results": 100,
  // Filtros opcionais (só para type=cadastral/empresas_novas):
  "porte": 3,
  "tem_email": false,
  "ddd": "41"
}
```

Se `type` não for enviado, o worker assume `"google_maps"` (backward compat).

---

## 3. Schema Completo do Lead (JSON)

```json
{
  // === Campos existentes (sem mudança) ===
  "name": "Restaurante Exemplo",
  "category": "Restaurante",
  "address": "Rua XV de Novembro, 100 - Centro",
  "neighborhood": "Centro",
  "city": "Curitiba",
  "state": "PR",
  "phone": "+5541999998888",
  "website": "restauranteexemplo.com.br",
  "instagram": "https://www.instagram.com/restauranteexemplo",
  "rating": 4.5,
  "review_count": 230,
  "score": 87,
  "digital_status": "good",
  "tags": ["sem site próprio", "precisa de SEO", "alto potencial"],
  "hours": "11:00–22:00",
  "photo_url": "https://lh3.googleusercontent.com/...",
  "status": "new",
  "source": "google_maps",

  // === Campos NOVOS ===
  "place_id": "0x94ce504b5e60b0ad:0x123abc",
  "latitude": -25.4284,
  "longitude": -49.2733,
  "photos_count": 45,
  "price_level": "$$",
  "description": "Restaurante especializado em comida caseira",
  "has_google_posts": true,
  "facebook": "https://facebook.com/restauranteexemplo",
  "socials": {
    "instagram": "https://instagram.com/restauranteexemplo",
    "facebook": "https://facebook.com/restauranteexemplo",
    "tiktok": "https://tiktok.com/@restauranteexemplo"
  },
  "service_options": ["Retirada", "Entrega", "Consumo no local"],
  "full_hours": {
    "Seg": "11:00–22:00",
    "Ter": "11:00–22:00",
    "Qua": "11:00–22:00",
    "Qui": "11:00–22:00",
    "Sex": "11:00–23:00",
    "Sáb": "11:00–23:00",
    "Dom": "Fechado"
  },
  "owner_verified": true,

  // Dados cadastrais (da base CNPJ)
  "cnpj": "12345678000100",
  "razao_social": "RESTAURANTE EXEMPLO LTDA",
  "cnae_fiscal": "5611201",
  "capital_social": 150000.0,
  "porte_empresa": 3,
  "data_abertura": "2019-03-15",
  "opcao_mei": false,

  // Enrichment de website
  "emails": ["contato@restauranteexemplo.com.br"],
  "technologies": ["WordPress", "Google Analytics", "WhatsApp Widget"],

  // Diagnóstico digital
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
    "suggested_services": ["otimização SEO", "tráfego pago", "automação"]
  },
  "suggested_services": ["otimização SEO", "configuração de tráfego pago"],
  "digital_maturity_score": 55,

  // Qualidade do dado
  "quality_score": 85,
  "quality_issues": []
}
```

---

## 4. WebSocket — Configuração

O worker envia progresso via HTTP POST. Para o frontend receber atualizações em tempo real, o servidor principal deve:

### 4.1 Implementar WebSocket no servidor

```
wss://workers.alvify.com.br/ws/jobs/{job_id}
```

Quando o worker envia `POST /internal/workers/jobs/{job_id}/progress`:
```json
{"pct": 45, "n": 9, "msg": "Extraindo lead 9/20: Pizzaria Bella"}
```

O servidor deve **broadcast via WebSocket** para todos os clients conectados nesse job_id:
```json
{
  "type": "progress",
  "job_id": "uuid",
  "pct": 45,
  "n": 9,
  "msg": "Extraindo lead 9/20: Pizzaria Bella"
}
```

### 4.2 Eventos WebSocket

| Tipo | Quando | Payload |
|------|--------|---------|
| `progress` | Worker envia progresso | `{pct, n, msg}` |
| `lead_new` | Novo lead inserido | `{lead: {...}}` |
| `lead_batch` | Batch de leads inserido | `{leads: [...], count: N}` |
| `job_complete` | Job finalizado | `{job_id, total_leads, duration_s}` |
| `job_error` | Job falhou | `{job_id, error}` |

### 4.3 Frontend — Conectar ao WebSocket

```javascript
const ws = new WebSocket(`wss://workers.alvify.com.br/ws/jobs/${jobId}`);

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  switch(data.type) {
    case 'progress':
      updateProgressBar(data.pct, data.msg);
      break;
    case 'lead_new':
      addLeadToList(data.lead);
      break;
    case 'lead_batch':
      addLeadsToList(data.leads);
      break;
    case 'job_complete':
      showComplete(data.total_leads, data.duration_s);
      break;
    case 'job_error':
      showError(data.error);
      break;
  }
};
```

---

## 5. Teste End-to-End

Após configurar tudo, testar com um job real:

### 5.1 Criar job de teste

```bash
# Job Google Maps (padrão)
curl -X POST https://workers.alvify.com.br/internal/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <worker-api-key>" \
  -d '{
    "type": "google_maps",
    "keyword": "restaurante",
    "city": "Francisco Alves",
    "state": "PR",
    "max_results": 5
  }'
```

### 5.2 Verificar resultados esperados

Após o job completar (~30-60s para 5 leads), verificar:

```sql
-- Deve ter leads com campos novos preenchidos
SELECT 
  name, phone, instagram, 
  digital_maturity_score, quality_score,
  suggested_services,
  latitude, longitude, place_id
FROM leads 
WHERE search_id = '<job_search_id>'
ORDER BY quality_score DESC;
```

### 5.3 Verificar WebSocket

```bash
# Instalar wscat se necessário: npm install -g wscat
wscat -c "wss://workers.alvify.com.br/ws/jobs/<job_id>"
# Deve receber mensagens de progresso em tempo real
```

### 5.4 Testar job cadastral

```bash
curl -X POST https://workers.alvify.com.br/internal/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <worker-api-key>" \
  -d '{
    "type": "cadastral",
    "keyword": "5611201",
    "city": "Curitiba",
    "state": "PR",
    "max_results": 10,
    "porte": 3
  }'
```

---

## 6. Checklist

- [ ] Executar SQL migrations (seção 2.1)
- [ ] Atualizar model/struct do Lead com campos novos
- [ ] Atualizar endpoint de ingest para salvar campos novos
- [ ] Adicionar campo `type` ao dispatch de jobs
- [ ] Implementar WebSocket endpoint (`/ws/jobs/{job_id}`)
- [ ] Broadcast progresso do worker via WebSocket
- [ ] Broadcast novos leads via WebSocket
- [ ] Testar job google_maps (5 leads, verificar campos novos)
- [ ] Testar job cadastral (precisa api-db rodando)
- [ ] Testar WebSocket no frontend
- [ ] Verificar que workers antigos continuam funcionando (backward compat)

---

## 7. Info de Conexão do Worker

O worker se conecta ao servidor principal assim:

```env
API_URL=https://workers.alvify.com.br
WORKER_ID=<uuid>
WORKER_API_KEY=<key>
```

Endpoints que o worker chama:
- `POST /internal/workers/register` — registro inicial
- `GET /internal/workers/jobs/poll` — buscar próximo job
- `POST /internal/workers/jobs/{job_id}/progress` — enviar progresso
- `POST /internal/workers/jobs/{job_id}/lead` — enviar lead individual
- `POST /internal/workers/jobs/{job_id}/leads/batch` — enviar batch de leads
- `POST /internal/workers/jobs/{job_id}/complete` — marcar job como concluído
- `POST /internal/workers/jobs/{job_id}/error` — reportar erro

Se mudar para WebSocket bidirecional (worker ↔ servidor), o worker pode receber jobs via WS ao invés de polling — mais eficiente mas não necessário agora.
