## barbearia — fluxo

```
                         POST /appointments
  Producer (CLI) ──────────────────────────────► Recepcao (HTTP)
                                                    │
                                      valida corte ─┤
                                      ocupa cadeira ─┤ (DynamoDB ADD +1, atomico)
                                                    │
                                                    ▼
                                              SQS FIFO
                                           (barbearia.fifo)
                                                    │
                                       long poll ───┤ (espera msg ate 20s)
                                                    │
                                                    ▼
                                              Worker (barbeiro)
                                                    │
                                       corta 5-15s ─┤
                                       deleta msg  ─┤
                                                    │
                          POST /heartbeat           │
  Recepcao ◄────────────────────────────────────────┘
      │                                    estado: dormindo | cortando | terminou
      │
      ├── heartbeat "terminou" → libera cadeira (DynamoDB ADD -1)
      ├── persiste estado no DynamoDB (pk=worker#id, com TTL)
      │
      ▼
  GET /stats → { cadeiras_ocupadas, workers[] }
```

## o que cada parte faz

- **producer**: CLI que simula clientes chegando, manda POST pro recepcao
- **recepcao**: HTTP server, valida corte, controla cadeiras (max 3), enfileira no SQS, recebe heartbeat do worker, expoe /stats
- **worker**: consome da fila com long polling, corta, manda heartbeat pro recepcao
- **SQS FIFO**: fila com ordem garantida e dedup por hash do body
- **DynamoDB**: tabela unica (single table design), pk diferencia tipo — "cadeiras" pro counter atomico, "worker#id" pro heartbeat

## comunicacao

- producer → recepcao: **HTTP sincrono** (POST /appointments, espera resposta 202/422/503)
- recepcao → SQS: **async** (enfileira e responde pro producer sem esperar processamento)
- SQS → worker: **long polling** (worker fica esperando msg, nao fica batendo em loop)
- worker → recepcao: **HTTP sincrono** (POST /heartbeat, fire-and-forget)
- recepcao/worker → DynamoDB: **sincrono** (operacoes atomicas, counter de cadeiras)

## resiliencia

- recepcao morreu? no startup sincroniza cadeiras com o numero de msgs na fila (SQS eh a fonte de verdade)
- worker morreu no meio do corte? msg volta pra fila pelo visibility timeout (30s)
- barbearia lotada? recepcao retorna 503 sem enfileirar
