# cgb-go-v1-source - Código Fonte Completo

**Código fonte completo** da solução cgb-go-v1 para Rinha de Backend 2025.

## 🚀 Arquitetura

Sistema monolítico em Go com foco em performance:
- **Go 1.24.5** com FastHTTP
- **Redis** para persistência
- **Fila in-memory** para hot path (sem Redis no caminho crítico)
- **Docker** com otimizações de GC

## 📊 Performance Alcançada

- **p99**: 10.37ms (18% de bônus)
- **Consistência**: 0 inconsistências
- **Throughput**: ~254 req/s

## 🛠️ Como Executar

```bash
# 1. Preparar dependências Go
go mod tidy

# 2. Subir Payment Processors (obrigatório primeiro)
cd rinha-de-backend-2025/payment-processor
docker-compose up -d

# 3. Subir solução
docker-compose up --build -d

# 4. Testar básico
curl http://localhost:9999/healthcheck

# 5. Testar performance (k6)
cd rinha-de-backend-2025/rinha-test
k6 run rinha.js
```

## 📝 Submissão

**Repositório de submissão**: https://github.com/CiceroGB/cgb-go-v1
**Docker Hub**: cicerocg/rinha-v1:latest

---

Desenvolvido para Rinha de Backend 2025 🚀