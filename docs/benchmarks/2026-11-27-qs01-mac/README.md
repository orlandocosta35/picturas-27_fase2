# QS-01 — Lote misto com 1 e 3 réplicas (2026-11-27)

Medição de `REQ-VID-EDIT-011`: um pedido de legendagem longo e dois pedidos curtos,
publicados seguidos, com 1 réplica de `video_tool` e depois com 3 réplicas.

## Ambiente

- macOS 26.5.1, Apple M4 Pro, 24 GB RAM.
- Docker Desktop (Linux 6.12.54-linuxkit, arm64): 12 CPUs, 7,65 GiB para os contentores.
- Docker Compose 5.0.0, RabbitMQ 4.0.5-management.
- Código no commit `9316492`, sem alterações. Limites por réplica de `docker-compose.yaml`:
  2 CPUs e 2,5 GiB.
- Projeto Compose isolado (`-p rnf-bench`), apenas os serviços `rabbitmq` e `video_tool`.

## Entradas

| Ficheiro | Duração | fps | Fotogramas | SHA-256 |
|---|---|---|---|---|
| `samples/video/speech_mandela_177s_1080p.webm` | 176,62 s | 25 | 4415 | `3a58e266d7552d700846b362933d1649b384493dc4a68db22df12d0bd3c1db3f` |
| `samples/video/sample_10s_720p.mp4` | 10,00 s | 30 | 300 | `a261d91c6cd4b7d1bc445259439613107e4c49484587be25ef19036336255280` |

Pedidos de cada repetição, publicados por esta ordem e sem esperar pela conclusão:

1. `subtitles` no vídeo de 177 s;
2. `rotate` (`degrees: 90`) no vídeo de 10 s;
3. `saturation` (`saturationFactor: 1.5`) no vídeo de 10 s.

Antes de cada repetição há um aquecimento não medido: uma legendagem curta por réplica,
para o modelo Whisper já estar carregado em memória.

## Método

`bench_qs01.py` publica os pedidos no exchange `picturas` com a routing key `video_queue`,
no mesmo formato que o `projects` usa, com confirmação do broker. Lê as respostas do
worker em `project_queue`.

- `t_submit`: instante imediatamente antes da publicação.
- `t_confirm`: confirmação da publicação pelo broker.
- `t_reply`: receção da resposta do worker.
- `latency_s` = `t_reply − t_submit` (espera na fila + processamento).
- `worker_processing_s`: tempo de processamento indicado pelo próprio worker.

Em paralelo, `docker stats --no-stream` é recolhido em ciclo (uma amostra a cada ~2 s por
contentor) para `*-stats.csv`.

**Limite do método:** os pedidos são publicados diretamente no RabbitMQ. Não inclui o
percurso HTTP, as quotas, o `projects` nem o MongoDB. Mede o broker e o worker.

## Resultados (3 repetições por configuração)

Latência em segundos, por repetição e mediana:

| Pedido | 1 réplica | Mediana | 3 réplicas | Mediana |
|---|---|---|---|---|
| `subtitles` (177 s) | 107,07 / 105,17 / 105,96 | 105,96 | 107,68 / 107,59 / 106,36 | 107,59 |
| `rotate` (10 s) | 109,56 / 107,61 / 108,60 | 108,60 | 2,66 / 2,61 / 2,71 | 2,66 |
| `saturation` (10 s) | 112,15 / 110,25 / 111,26 | 111,26 | 2,98 / 2,97 / 3,28 | 2,98 |

- Com 3 réplicas, os três pedidos começam no mesmo instante em réplicas diferentes
  (ver `results/worker-3replicas.log`). A latência dos pedidos curtos fica praticamente
  igual ao seu tempo de processamento (~2,5–3 s).
- A legendagem não fica mais rápida: as réplicas reduzem a espera, não o trabalho de cada vídeo.
- CPU máxima observada por contentor: 210,5 % (1 réplica) e 212,0 % (3 réplicas).
  RAM máxima: 2,17 GiB e 2,23 GiB, abaixo do limite de 2,5 GiB.
- Todos os 18 pedidos medidos terminaram com sucesso, sem reentregas nem contentores terminados.

## Ficheiros

- `bench_qs01.py`: publica um lote e mede uma repetição.
- `run_campaign.sh`: campanha completa (corre na raiz do repositório).
- `results/<run>-jobs.csv`: instantes e latências por pedido.
- `results/<run>-stats.csv`: série CPU/RAM por contentor.
- `results/worker-1replica.log`, `results/worker-3replicas.log`: logs do worker com horas.
- `results/replicas-3.txt`: contentores ativos na configuração de 3 réplicas.

## Repetir

```bash
python3 -m pip install pika==1.3.2
sh docs/benchmarks/2026-11-27-qs01-mac/run_campaign.sh
docker compose -p rnf-bench down -v
```

O último comando remove apenas os contentores e volumes do projeto `rnf-bench`.
