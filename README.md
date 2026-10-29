# HDFS Data Lake Cluster

Учебный локальный data-lake стенд на базе экосистемы Hadoop для оркестрации, валидации и хранения данных о пожарах и погоде. Проект демонстрирует построение ETL/ELT-пайплайна от извлечения из внешних источников до загрузки в партиционированное хранилище HDFS с использованием Apache Airflow и Apache Spark.

## Содержание

- [О проекте](#о-проекте)
- [Архитектура](#архитектура)
- [Технологический стек](#технологический-стек)
- [Структура репозитория](#структура-репозитория)
- [Требования](#требования)
- [Быстрый старт](#быстрый-старт)
- [Доступ к веб-интерфейсам](#доступ-к-веб-интерфейсам)
- [Описание дата-пайплайна](#описание-дата-пайплайна)
- [HDFS-структура данных](#hdfs-структура-данных)
- [Edge-bridge для Open-Meteo](#edge-bridge-для-open-meteo)
- [Конфигурация и переменные](#конфигурация-и-переменные)
- [Управление кластером](#управление-кластером)
- [Особенности реализации](#особенности-реализации)
- [Известные ограничения](#известные-ограничения)
- [Планы по доработке](#планы-по-доработке)

## О проекте

Проект собирает и структурирует два источника данных:

- **[NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/)** — данные о термальных аномалиях и активных пожарах в формате CSV.
- **[Open-Meteo](https://open-meteo.com/)** — исторические и прогнозные метеоданные в формате JSON по заданным географическим областям.

Полученные данные проходят валидацию с помощью PySpark, сохраняются в слой `raw/` распределённой файловой системы HDFS с партиционированием по дате (`year/month/day`) и оркеструются через Airflow DAG'и.

## Архитектура

```mermaid
graph TD
  A[NASA FIRMS API] --> B[Airflow DAG: firms_source_to_raw]
  B --> C[HDFS /raw/firms/...]
  C --> D[Airflow DAG: firms_raw_to_stg]
  D --> E[HDFS /stg/firms/...]
  
  F[Open-Meteo API] --> G[Edge Bridge :8765]
  G --> H[Airflow DAG: open_meteo_source_to_raw]
  H --> I[HDFS /raw/open-meteo/...]
  I --> J[Airflow DAG: open_meteo_raw_to_stg]
  J --> K[HDFS /stg/open-meteo/...]
  
  E --> L[Аналитика / DWH-слой]
  K --> L
  M[Postgres] --> N[Airflow Metadata]
  O[Hadoop HDFS/YARN] --> C
  O --> E
  O --> I
  O --> K
```

Кластер разворачивается в изолированной Docker-сети `cluster`. Каждый компонент вынесен в отдельный `docker-compose.yaml` для упрощения модульного управления.

## Технологический стек

| Компонент | Назначение | Версия/Особенности |
|---|---|---|
| **[Apache Hadoop](https://hadoop.apache.org/)** | Хранение и распределённые ресурсы (HDFS + YARN) | `3.5.0` |
| **[Apache Spark](https://spark.apache.org/)** | Валидация и предобработка данных | Интеграция через `SparkSubmitOperator` из Airflow |
| **[Apache Airflow](https://airflow.apache.org/)** | Оркестрация ETL-пайплайна | `3.4.0b2` (на базе `apache/airflow`) |
| **[PostgreSQL](https://www.postgresql.org/)** | Metadata-база Airflow | `17` |
| **[Apache Kafka](https://kafka.apache.org/)** | Инфраструктурный компонент (заложен в составе кластера) | Kraft-режим, мульти-брокерная конфигурация |
| **[ClickHouse](https://clickhouse.com/)** | Инфраструктурный компонент (заложен в составе кластера) | Кластер из 3 нод + ClickHouse Keeper |
| **[Docker](https://www.docker.com/) / [Docker Compose](https://docs.docker.com/compose/)** | Контейнеризация и оркестрация сервисов | V2 |

## Структура репозитория

```text
HdfsCluster/
├── airflow/                # Конфигурация, DAG'и, Spark-приложения и образ Airflow
│   ├── apps/               # PySpark-скрипты валидации payload
│   ├── dags/               # DAG'и, хуки, утилиты и callback'и
│   │   ├── FIRES/          # Пайплайн NASA FIRMS
│   │   ├── open-meteo/     # Пайплайн Open-Meteo
│   │   ├── handlers/       # Обработчики алертов/дедлайнов
│   │   └── utils/          # Общие утилиты
│   ├── hadoop_conf/        # Hadoop-конфиги, проброшенные в Airflow
│   └── airflow.env         # Переменные окружения Airflow (требуется создать)
├── hadoop/                 # Образ и docker-compose для HDFS NameNode/DataNode/ResourceManager/NodeManager
├── spark/                  # Конфигурация Spark-стека
├── kafka/                  # Конфигурация Kafka-кластера (KRaft)
├── ClickHouse/             # Конфигурация ClickHouse-кластера и Keeper
├── postgres/               # Инициализация и конфигурация PostgreSQL
├── infra/                  # Вспомогательные скрипты инфраструктуры
├── build.py                # CLI-обёртка для управления жизненным циклом кластера
└── edge_bridge.py          # Локальный HTTP-прокси через Chrome/Edge CDP для Open-Meteo
```

## Требования

Перед запуском убедитесь, что установлено следующее:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (с поддержкой WSL2 на Windows)
- [Python 3.10+](https://www.python.org/)
- Доступ в интернет для скачивания образов и обращения к внешним API
- Свободные порты: `8080`, `8088`, `9870`, `8020`, `9000`, `5432`, `8765` (и прочие порты, используемые стеком)
- Для работы `edge_bridge.py` (при использовании Open-Meteo-пайплайна): запущенный Microsoft Edge/Google Chrome с включённым [Chrome DevTools Protocol](https://chromedevtools.github.io/devtools-protocol/) (`--remote-debugging-port=9222`)

> Рекомендуется выделить не менее 16 ГБ ОЗУ и 4 CPU-ядер для комфортного одновременного запуска всех стеков.

## Быстрый старт

### 1. Клонирование репозитория

```bash
git clone https://github.com/anatoliy266/MyLittleCluster.git
cd MyLittleCluster
```

### 2. Создание файла секретов Airflow

В директории `airflow/` необходимо создать файл `airflow.env`. Минимальный обязательный набор переменных:

1. Ключи можно сгенерировать командой: python3 -c "import secrets; print(secrets.token_hex(32))": \
  `AIRFLOW__API_AUTH__JWT_SECRET`=your_jwt_secret_here \
  `AIRFLOW__CORE__FERNET_KEY`=your_fernet_key_here \
  `AIRFLOW__API__SECRET_KEY`=your_secret_key_here 

2. Учетные данные администратора (Web UI) \
  `AIRFLOW_ADMIN_USERNAME`=admin \
  `AIRFLOW_ADMIN_PASSWORD`=your_secure_password 

3. в проекте используется база данных posgres, развернутая в контейнере postgres, если база развернута в том же контейнере что и Airflow - AIRFLOW_DB_HOST=localhost либо лбой другой адрес доступный из сети cluster \
`AIRFLOW_DB_USER`=airflow \
`AIRFLOW_DB_PASSWORD`=airflow_db_password \
`AIRFLOW_DB_NAME`=airflow \
`AIRFLOW_DB_HOST`=postgres \
`AIRFLOW_DB_PORT`=5432

4. Строка подключения SQLAlchemy \
Формат: `postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@${AIRFLOW_DB_HOST}:${AIRFLOW_DB_PORT}/${AIRFLOW_DB_NAME}` 

    `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`=you://conn:string@localhost:5432/airflow

### 3. Инициализация HDFS

При первом запуске требуется отформатировать NameNode.

1. Откройте `docker-compose.yml`, найдите сервис `namenode` и временно добавьте туда команду форматирования:
   ```yaml
     namenode:
       # ......
       command: ["hdfs", "namenode", "-format"]
   ```

2. Запустите контейнер для выполнения форматирования:
   ```bash
   docker compose up namenode
   ```
   *Контейнер успешно отформатирует файловую систему и завершит работу (остановится).*

3. Удалите флаг -format из сервиса `namenode` `hadoop/docker-compose.yml`.
  ```yaml
     namenode:
       # ......
       command: ["hdfs", "namenode"]
   ```

### 4. Запуск всего кластера

```bash
python build.py start
```

После успешного старта будут доступны все веб-интерфейсы кластера. Первый запуск может занять 5–10 минут из-за сборки образов и инициализации сервисов.

## Доступ к веб-интерфейсам

| Сервис | URL | Назначение |
|---|---|---|
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | Мониторинг и запуск DAG'ов, просмотр логов задач |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | Состояние HDFS, просмотр директорий и блоков |
| **YARN ResourceManager UI** | [http://localhost:8088](http://localhost:8088) | Мониторинг YARN-приложений и ресурсов кластера |
| **Edge Bridge (healthcheck)** | [http://localhost:8765/health](http://localhost:8765/health) | Проверка доступности локального CDP-моста для Open-Meteo |

## Описание дата-пайплайна

В проекте реализованы два независимых источника данных, каждый из которых разбит на связку DAG'ов `raw` и `stg`.

### 1. NASA FIRMS

| DAG | Этап | Описание |
|---|---|---|
| `firms_source_to_raw` (`FIRES/raw/firms_source_to_raw.py`) | Extract → Load to raw | Получение CSV по списку спутников (`VIIRS_NOAA20_NRT`, `VIIRS_NOAA21_NRT`) за `logical_date` напрямую с NASA FIRMS. Для проверки ответа используется spark-приложение `apps/spark_firms_payload_validation`. Валидные файлы сохраняются в `hdfs:///raw/nasa_firms/year=YYYY/month=MM/day=DD/`. |
| `firms_raw_to_stg` (`FIRES/stg/firms_raw_to_stg.py`) | Raw → Stg | Чтение сырых CSV из `raw/`, приведение к единому виду, фильтрация/нормализация полей и запись в слой `stg/` для последующего использования. |

### 2. Open-Meteo

| DAG | Этап | Описание |
|---|---|---|
| `open_meteo_source_to_raw` (`open-meteo/raw/open_meteo_source_to_raw.py`) | Extract → Load to raw | Разбиение `bounding_box` на чанки, запрос через `edge_bridge.py` (CDP) к Open-Meteo API. Полученные JSON сохраняются в `hdfs:///raw/open-meteo/year=YYYY/month=MM/day=DD/`. |
| `open_meteo_raw_to_stg` (`open-meteo/stg/open_meteo_raw_to_stg.py`) | Raw → Stg | Обработка сырых JSON из `raw/`, трансформация структуры, валидация/приведение к единому виду и загрузка в `hdfs:///stg/open-meteo/year=YYYY/month=MM/day=DD/`. |

Весь пайплайн оркестрируется Airflow. Данные хранятся в HDFS с партиционированием по дате.

## HDFS-структура данных

Данные организованы по принципу «raw-слой» с явным партиционированием по дате:

```text
hdfs://namenode:8020/
├── raw/
│   ├── nasa_firms/
│   |   └── year=YYYY/month=MM/day=DD/
│   ├── open-meteo/
│   |   └── year=YYYY/month=MM/day=DD/
|   └── logs/airflow/
└── stg/
    ├── nasa_firms/
    |   └── year=YYYY/month=MM/day=DD/
    └── open-meteo/
         └── year=YYYY/month=MM/day=DD/

```

## Edge-bridge для Open-Meteo

Файл `edge_bridge.py` — локальный HTTP-мост, который использует Chrome DevTools Protocol (CDP) для получения ответа от `api.open-meteo.com` через запущенный экземпляр Microsoft Edge.

**Как это работает:**

1. Мост поднимается на `http://127.0.0.1:8765`
2. При запросе `GET /v1/forecast?...` мост открывает новую вкладку в Edge через CDP, навигирует по целевому URL, дожидается `Page.loadEventFired` и извлекает `document.body.innerText` через `Runtime.evaluate`
3. Полученный текст парсится как JSON и проксируется обратно Airflow
4. Созданная вкладка сразу закрывается

**Запуск моста (отдельно от Docker):**

```bash
# Убедитесь, что Edge запущен с CDP
# msedge --remote-debugging-port=9222 --user-data-dir=%TEMP%\edge-debug

pip install websocket-client
python edge_bridge.py
```

> Данный подход используется в рамках учебного стенда для преодоления отдельных сетевых ограничений, доступ к апи open meteo через браузер должен быть настроен отдельно

## Конфигурация и переменные

### Ключевые файлы конфигурации

| Файл | Описание |
|---|---|
| `airflow/airflow.env` | Основные переменные Airflow, секреты, подключения (должен быть создан вручную) |
| `airflow/airflow_conf/airflow.cfg` | Расширенная конфигурация Airflow (executor, logging, auth, connections) |
| `hadoop/hadoop_conf/*.xml` | Конфиги HDFS/YARN/MapReduce |
| `spark/spark_conf/spark-defaults.conf` | Базовые настройки Spark |
| `postgres/postgres_conf/*` | Конфигурация PostgreSQL и init-скрипты |
| `kafka/kafka_conf/*/server.properties` | Конфиги Kafka-брокеров |
| `ClickHouse/clickhouse_conf/*` | Конфиги нод ClickHouse и Keeper |

### Airflow Connections (рекомендуемые)

Для корректной работы DAG'ов в Airflow UI необходимо настроить подключения:

| Conn Id | Тип | Примечание |
|---|---|---|
| `hdfs_conn` | `HTTP` / `WebHDFS` | Используется `WebHDFSHook` для работы с HDFS |
| `spark_default` | `Spark` | Конфиг для `SparkSubmitOperator` |

### Airflow Variables

В DAG'ах используются переменные Airflow (например, токен для NASA FIRMS). Их можно задать через `Admin → Variables` в Airflow UI.


## Особенности реализации

- **Модульная архитектура** — каждый компонент Big Data-стека развёрнут в собственном compose-файле с общей внешней сетью `cluster`.
- **Партиционирование в HDFS** — хранение по `year/month/day` для удобства последующей фильтрации и инкрементальной обработки.
- **Два независимых пайплайна** — FIRMS и Open-Meteo изолированы по DAG'ам, путям в HDFS и логике обработки.
- **Интеграция с WebHDFS** — работа с HDFS из Airflow реализована через `WebHDFSHook`.
- **Airflow 3** — используются подходы из Airflow 3 (`airflow.sdk`, deadline/callback-механика).
- **CDP-прокси** — решение для получения данных Open-Meteo без прямых HTTP-запросов из контейнерного окружения в текущей конфигурации.

## Известные ограничения

Проект ориентирован на **локальный учебный стенд**, поэтому имеет ряд компромиссов:

- **Воспроизводимость** — `airflow/airflow.env` и часть конфигурационных файлов не отслеживаются в git. Перед запуском требуется их подготовить вручную.
- **Мёртвая инфраструктура** — стеки Kafka и ClickHouse развёрнуты, но в текущих DAG'ах не используются.
- **Spark-стек** — отдельный `spark/` compose-стек в текущем виде нуждается в доработке путей конфигурации. Основной путь запуска Spark-задач — через `SparkSubmitOperator` из контейнера Airflow.
- **Edge-bridge** — требует ручного запуска Microsoft Edge с включённым CDP на хост-машине. Не подходит для «бесшовного» запуска только через `docker compose`.
- **Идемпотентность** — отдельные шаги пайплайна стоит доработать для безопасного повторного запуска (`backfill`) за прошлые даты.


## Планы по доработке

- [ ] оформить `edge_bridge` как управляемый сервис
- [ ] Интегрировать ClickHouse в пайплайн 
- [ ] Довести идемпотентность `publish`-шага и политику перезаписи до production-подобного вида
- [ ] Добавить базовые unit-тесты для утилит и проверку импорта DAG'ов

**Статус проекта:** учебный демонстрационный стенд. Подходит для изучения принципов построения Hadoop-кластера, оркестрации в Airflow, работы с HDFS и интеграции PySpark в data-pipeline.
