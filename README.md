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
  A[NASA FIRMS API] --> B[Airflow DAG: firms_raw_to_stg_v2]
  C[Open-Meteo API] --> D[Edge Bridge :8765]
  D --> E[Airflow DAG: nasa_openmeteo_extract]
  B --> F[HDFS /raw/firms/...]
  E --> G[HDFS /stg/open-meteo/...]
  G --> H[Spark Job: payload validation]
  H --> F[HDFS /raw/open-meteo/...]
  F --> I[Аналитика / DWH-слой]
  J[Postgres] --> K[Airflow Metadata]
  L[Spark Cluster] --> H
  M[Hadoop HDFS/YARN] --> F
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
├── edge_bridge.py          # Локальный HTTP-прокси через Chrome/Edge CDP для Open-Meteo
└── PIPELINE_AUDIT_REPORT.md # Детальный технический аудит проекта
```

## Требования

Перед запуском убедитесь, что установлено следующее:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (с поддержкой WSL2 на Windows)
- [Python 3.10+](https://www.python.org/)
- Доступ в интернет для скачивания образов и обращения к внешним API
- Свободные порты: `8080`, `8088`, `9870`, `8020`, `9000`, `5432`, `8765` (и прочие порты, используемые стеком)
- Для работы `edge_bridge.py` (при использовании Open-Meteo-пайплайна): запущенный Microsoft Edge/Google Chrome с включённым [Chrome DevTools Protocol](https://chromedevtools.github.io/devtools-protocol/) (`--remote-debugging-port=9222`)

> Рекомендуется выделить не менее 8–12 ГБ ОЗУ и 4–6 CPU-ядер для комфортного одновременного запуска всех стеков.

## Быстрый старт

### 1. Клонирование репозитория

```bash
git clone https://github.com/<your-username>/HdfsCluster.git
cd HdfsCluster
```

### 2. Создание файла секретов Airflow

В директории `airflow/` необходимо создать файл `airflow.env`. Минимальный набор переменных можно ориентировать по текущим конфигурациям в репозитории. Обязателен как минимум конфиг для подключения к PostgreSQL и базовые настройки Airflow.

### 3. Инициализация HDFS

При первом запуске требуется отформатировать NameNode и поправить права доступа на томах.

```bash
python build.py init
```

Скрипт выполнит:

1. Форматирование `namenode` через compose-профиль `init`
2. Ожидание завершения контейнера форматирования
3. Корректировку владельцев `hadoop:hadoop` для томов `namenode_data` и `datanode_data`

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

В проекте реализованы два независимых DAG'а.

### 1. `firms_raw_to_stg_v2` (NASA FIRMS)

Ответственный за сбор данных о пожарах.

| Этап | Описание |
|---|---|
| **Extract** | Запрос CSV по списку спутников (`VIIRS_SNPP_NRT`, `NOAA20_NRT`, `NOAA21_NRT`) за `logical_date`. Выполняется прямая загрузка по URL FIRMS. |
| **Validation (raw-check)** | Базовая проверка ответа: Content-Type, наличие CSV-заголовка, минимальная валидность первых байт. |
| **Load to HDFS (raw)** | Сохранение в `hdfs:///raw/firms/source=.../satellite=.../year=YYYY/month=MM/day=DD/` с перезаписью при повторном запуске. |

### 2. `nasa_openmeteo_extract` (Open-Meteo)

Более многоступенчатый пайплайн с валидацией через Spark.

| Этап | Описание |
|---|---|
| **Prepare areas** | Разбиение целевого `bounding_box` на чанки для построчного/пообластного сбора данных. Параметры запроса формируются на основе списка `models`. |
| **Extract** | Для каждого чанка выполняется запрос через `edge_bridge.py`. Ответ в формате JSON сохраняется во временное хранилище для последующей обработки. |
| **Store to staging (HDFS)** | Файлы копируются в `hdfs:///stg/open-meteo/.../year=YYYY/month=MM/day=DD/`. |
| **Spark validation** | `SparkSubmitOperator` запускает `apps/spark_open_meteo_payload_validation.py`. Задача проверяет читаемость JSON, базовую структуру и целостность payload перед публикацией. |
| **Publish to raw** | После успешной валидации валидные файлы перемещаются в слой `raw/` по той же партиционированной схеме. |
| **Retry-ветка** | Реализованы дополнительные задачи для повторного извлечения/валидации при частичном падении DAG'а. |

## HDFS-структура данных

Данные организованы по принципу «raw-слой» с явным партиционированием по дате:

```text
hdfs://namenode:8020/
├── raw/
│   ├── firms/
│   │   └── source=firms_modaps/
│   │       └── satellite={VIIRS_SNPP_NRT|NOAA20_NRT|NOAA21_NRT}/
│   │           └── year=YYYY/month=MM/day=DD/
│   └── open-meteo/
│       └── area={...}/
│           └── year=YYYY/month=MM/day=DD/
├── stg/
│   └── open-meteo/
│       └── area={...}/
│           └── year=YYYY/month=MM/day=DD/
└── raw/logs/airflow/  # (настраиваемый путь для remote-логирования)
```

Такой подход упрощает последующую фильтрацию по дате, инкрементальную загрузку и построение витрин в аналитических слоях.

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

> Данный подход используется в рамках учебного стенда для обойти отдельные сетевые ограничения и наглядно продемонстрировать работу с CDP.

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

## Управление кластером

Управление осуществляется через `build.py`:

| Команда | Действие |
|---|---|
| `python build.py init` | Инициализация HDFS (только при первом запуске) |
| `python build.py start` | Сборка и запуск всех стеков в фоновом режиме |
| `python build.py stop` | Остановка контейнеров с сохранением данных в Docker volumes |
| `python build.py clean` | **Полный сброс**. Останавливает кластер, удаляет контейнеры и **все** volumes проекта. Требует подтверждения `y` |

Остановить отдельные стеки можно стандартными командами `docker compose` из соответствующих директорий (`hadoop/`, `airflow/`, `kafka/`, `ClickHouse/`, `postgres/`, `spark/`).

## Особенности реализации

- **Модульная архитектура** — каждый компонент Big Data-стека развёрнут в собственном compose-файле с общей внешней сетью `cluster`.
- **Партиционирование в HDFS** — хранение по `year/month/day` для удобства последующей фильтрации и инкрементальной обработки.
- **Два независимых пайплайна** — FIRMS и Open-Meteo изолированы по DAG'ам, путям в HDFS и логике обработки.
- **Валидация через Spark** — отдельный PySpark job проверяет целостность JSON-пейлоада перед публикацией в `raw/`.
- **Интеграция с WebHDFS** — работа с HDFS из Airflow реализована через `WebHDFSHook`.
- **Airflow 3 совместимость** — используются подходы из Airflow 3 (`airflow.sdk`, deadline/callback-механика).
- **CDP-прокси** — нетривиальное решение для получения данных Open-Meteo без прямых HTTP-запросов из контейнерного окружения в текущей конфигурации.

## Известные ограничения

Проект ориентирован на **локальный учебный стенд**, поэтому имеет ряд компромиссов:

- **Воспроизводимость** — `airflow/airflow.env` и часть конфигурационных файлов не отслеживаются в git. Перед запуском требуется их подготовить вручную.
- **Мёртвая инфраструктура** — стеки Kafka и ClickHouse развёрнуты, но в текущих DAG'ах не используются.
- **Spark-стек** — отдельный `spark/` compose-стек в текущем виде нуждается в доработке путей конфигурации. Основной путь запуска Spark-задач — через `SparkSubmitOperator` из контейнера Airflow.
- **Edge-bridge** — требует ручного запуска Microsoft Edge с включённым CDP на хост-машине. Не подходит для «бесшовного» запуска только через `docker compose`.
- **Наблюдаемость** — remote-логирование в HDFS настроено частично, часть механик алертинга нуждается в доработке.
- **Идемпотентность** — отдельные шаги пайплайна стоит доработать для безопасного повторного запуска (`backfill`) за прошлые даты.

Более подробный разбор багов, антипаттернов и рекомендаций по правкам приведён в [`PIPELINE_AUDIT_REPORT.md`](./PIPELINE_AUDIT_REPORT.md).

## Планы по доработке

- [ ] Добавить `README.md`-гайд по полному запуску с нуля и `airflow.env.example`
- [ ] Исправить конфигурацию Spark-стека и унифицировать запуск Spark
- [ ] Перевести Open-Meteo-запросы на прямой HTTP-вызов либо оформить `edge_bridge` как управляемый сервис
- [ ] Интегрировать Kafka/ClickHouse в пайплайн либо удалить неиспользуемые стеки
- [ ] Довести идемпотентность `publish`-шага и политику перезаписи до production-подобного вида
- [ ] Добавить базовые unit-тесты для утилит и проверку импорта DAG'ов
- [ ] Настроить централизованное логирование и работоспособные алерты

**Статус проекта:** учебный демонстрационный стенд. Подходит для изучения принципов построения Hadoop-кластера, оркестрации в Airflow, работы с HDFS и интеграции PySpark в data-pipeline.
