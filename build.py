#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent

# Порядок запуска важен. Сначала БД и сеть, потом Hadoop, затем остальной стек
COMPOSE_STACKS = [
    ("postgres", ROOT / "postgres" / "docker-compose.yaml"),
    ("hadoop", ROOT / "hadoop" / "docker-compose.yaml"),
    ("spark", ROOT / "spark" / "docker-compose.yaml"),
    ("kafka", ROOT / "kafka" / "docker-compose.yaml"),
    ("clickhouse", ROOT / "ClickHouse" / "docker-compose.yaml"),
    ("airflow", ROOT / "airflow" / "docker-compose.yaml"),
]

NETWORK_NAME = "cluster"
AIRFLOW_ENV = ROOT / "airflow" / "airflow.env"


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """Запуск команды с выводом в консоль"""
    print(f"\n[CMD] {' '.join(cmd)}")
    if cwd:
        print(f"[CWD] {cwd}")
    result = subprocess.run(cmd, cwd=str(cwd) if cwd else None)
    if check and result.returncode != 0:
        print(f"\n[ОШИБКА] Команда завершилась с кодом {result.returncode}")
        sys.exit(result.returncode)
    return result


def ensure_network() -> None:
    """Создаёт внешнюю сеть cluster, если её нет"""
    print(f"\n[INFO] Проверка внешней сети '{NETWORK_NAME}'...")
    result = subprocess.run(["docker", "network", "ls", "--format", "{{.Name}}"], stdout=subprocess.PIPE, text=True)
    if NETWORK_NAME not in result.stdout.splitlines():
        print(f"[INFO] Сеть '{NETWORK_NAME}' не найдена. Создаю...")
        run(["docker", "network", "create", NETWORK_NAME], check=False)
    else:
        print(f"[OK] Сеть '{NETWORK_NAME}' уже существует")


def check_env() -> None:
    """Проверка наличия обязательных файлов"""
    if not AIRFLOW_ENV.exists():
        print(f"\n[ОШИБКА] Не найден файл: {AIRFLOW_ENV}")
        print("Создай airflow/airflow.env по примеру из README перед запуском кластера.")
        sys.exit(1)
    print(f"[OK] Найден {AIRFLOW_ENV}")


def up() -> None:
    """Сборка и запуск всего кластера"""
    print("=" * 60)
    print("ЗАПУСК HDFS DATA LAKE КЛАСТЕРА")
    print("=" * 60)

    ensure_network()
    check_env()

    for name, compose_path in COMPOSE_STACKS:
        if not compose_path.exists():
            print(f"\n[ПРЕДУПРЕЖДЕНИЕ] {compose_path} не найден. Пропускаю стек '{name}'")
            continue
        print(f"\n{'=' * 60}")
        print(f"ПОДЪЁМ СТЕКА: {name.upper()}")
        print(f"{'=' * 60}")
        run(["docker", "compose", "-f", str(compose_path), "up", "-d", "--build"])

    print("\n" + "=" * 60)
    print("КЛАСТЕР УСПЕШНО ЗАПУЩЕН")
    print("=" * 60)
    print("\nДоступные интерфейсы:")
    print("  Airflow UI:        http://localhost:8080")
    print("  HDFS NameNode UI:  http://localhost:9870")
    print("  YARN RM UI:        http://localhost:8088")
    print("  Edge Bridge:       http://localhost:8765/health")
    print("\nДля остановки: python build.py down")
    print("=" * 60)


def down() -> None:
    """Остановка всего кластера с сохранением данных в volumes"""
    print("=" * 60)
    print("ОСТАНОВКА HDFS DATA LAKE КЛАСТЕРА")
    print("=" * 60)

    for name, compose_path in reversed(COMPOSE_STACKS):
        if not compose_path.exists():
            continue
        print(f"\n{'=' * 60}")
        print(f"ОСТАНОВКА СТЕКА: {name.upper()}")
        print(f"{'=' * 60}")
        run(["docker", "compose", "-f", str(compose_path), "down"], check=False)

    print("\n" + "=" * 60)
    print("КЛАСТЕР ОСТАНОВЛЕН")
    print("Данные в Docker volumes сохранены.")
    print("=" * 60)


def restart() -> None:
    """Перезапуск всего кластера"""
    down()
    up()


def ps() -> None:
    """Статус всех контейнеров кластера"""
    print("=" * 60)
    print("СТАТУС КОНТЕЙНЕРОВ КЛАСТЕРА")
    print("=" * 60)

    for name, compose_path in COMPOSE_STACKS:
        if not compose_path.exists():
            continue
        print(f"\n--- {name.upper()} ---")
        run(["docker", "compose", "-f", str(compose_path), "ps"], check=False)


def logs() -> None:
    """Просмотр логов выбранного стека"""
    print("\nДоступные стеки для просмотра логов:")
    for i, (name, _) in enumerate(COMPOSE_STACKS, 1):
        print(f"  {i}. {name}")
    print("  0. Отмена")

    try:
        choice = input("\nВыбери стек: ").strip()
        if choice == "0":
            return
        idx = int(choice) - 1
        if idx < 0 or idx >= len(COMPOSE_STACKS):
            print("[ОШИБКА] Неверный выбор")
            sys.exit(1)
        name, compose_path = COMPOSE_STACKS[idx]
        print(f"\n[INFO] Логи стека {name}. Для выхода нажми Ctrl+C")
        run(["docker", "compose", "-f", str(compose_path), "logs", "-f"], check=False)
    except (ValueError, IndexError):
        print("[ОШИБКА] Неверный выбор")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[INFO] Выход из просмотра логов")


def build() -> None:
    """Только сборка образов без запуска"""
    print("=" * 60)
    print("СБОРКА ОБРАЗОВ КЛАСТЕРА")
    print("=" * 60)

    ensure_network()

    for name, compose_path in COMPOSE_STACKS:
        if not compose_path.exists():
            print(f"\n[ПРЕДУПРЕЖДЕНИЕ] {compose_path} не найден. Пропускаю стек '{name}'")
            continue
        print(f"\n{'=' * 60}")
        print(f"СБОРКА СТЕКА: {name.upper()}")
        print(f"{'=' * 60}")
        run(["docker", "compose", "-f", str(compose_path), "build"])


def clean() -> None:
    """Полная очистка: остановка, удаление контейнеров, сетей и volumes"""
    print("=" * 60)
    print("ПОЛНАЯ ОЧИСТКА КЛАСТЕРА")
    print("=" * 60)
    print("\nВНИМАНИЕ!")
    print("Будут удалены ВСЕ контейнеры, а также ВСЕ volumes данного проекта.")
    print("Все данные в HDFS, PostgreSQL, Kafka и ClickHouse БУДУТ УТЕРЯНЫ.")
    print("=" * 60)

    confirm = input("\nПродолжить? Напиши 'YES' для подтверждения: ").strip()
    if confirm != "YES":
        print("\n[INFO] Операция отменена")
        return

    for name, compose_path in reversed(COMPOSE_STACKS):
        if not compose_path.exists():
            continue
        print(f"\n[INFO] Удаление стека {name} с volumes...")
        run(["docker", "compose", "-f", str(compose_path), "down", "-v", "--remove-orphans"], check=False)

    print("\n[INFO] Попытка удаления сети cluster (если не используется)...")
    run(["docker", "network", "rm", NETWORK_NAME], check=False)

    print("\n" + "=" * 60)
    print("КЛАСТЕР ПОЛНОСТЬЮ ОЧИЩЕН")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CLI для управления локальным Hadoop/Airflow/Spark/Kafka/ClickHouse кластером",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Примеры использования:
  python build.py up        Запустить весь кластер
  python build.py down      Остановить кластер с сохранением данных
  python build.py restart   Перезапустить кластер
  python build.py ps        Показать статус контейнеров
  python build.py logs      Просмотр логов по стеку
  python build.py build     Только собрать образы
  python build.py clean     Полный сброс (УДАЛИТЬ ВСЕ ДАННЫЕ)
""",
    )
    parser.add_argument(
        "command",
        choices=["up", "start", "down", "stop", "restart", "ps", "status", "logs", "build", "clean"],
        help="Команда для управления кластером",
    )

    args = parser.parse_args()
    cmd = args.command

    if cmd in ("up", "start"):
        up()
    elif cmd in ("down", "stop"):
        down()
    elif cmd == "restart":
        restart()
    elif cmd in ("ps", "status"):
        ps()
    elif cmd == "logs":
        logs()
    elif cmd == "build":
        build()
    elif cmd == "clean":
        clean()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
