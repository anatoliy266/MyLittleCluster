import subprocess
import sys
import os

# Путь к секретам Airflow относительно корня проекта
ENV_FILE_PATH = "./airflow/airflow.env"

def run_command(cmd_list):
    """Служебная функция для безопасного запуска консольных команд"""
    try:
        subprocess.run(cmd_list, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Ошибка при выполнении команды: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("\n❌ Ошибка: Docker не найден. Убедитесь, что Docker Desktop запущен.")
        sys.exit(1)

def start():
    """Запуск всего Enterprise-кластера"""
    if not os.path.exists(ENV_FILE_PATH):
        print(f"\n❌ Ошибка: Файл секретов не найден по пути {ENV_FILE_PATH}")
        print("Пожалуйста, создайте его перед запуском.")
        sys.exit(1)
        
    print("\n🚀 Запуск Big Data кластера (Hadoop + Spark + Airflow)...")
    cmd = ["docker", "compose", "--env-file", ENV_FILE_PATH, "up", "-d", "--build"]
    run_command(cmd)
    print("\n✅ Кластер успешно поднят в фоновом режиме!")
    print("🌍 Airflow UI: http://localhost:8080")
    print("🌍 YARN UI:    http://localhost:8088")
    print("🌍 HDFS UI:    http://localhost:9870")

def stop():
    """Полная остановка кластера без удаления данных в volumes"""
    print("\n🛑 Останавливаем контейнеры кластера...")
    cmd = ["docker", "compose", "down"]
    run_command(cmd)
    print("\n✅ Кластер успешно остановлен.")

def clean():
    """Остановка кластера с полной очисткой всех данных (жесткий сброс)"""
    print("\n⚠️ ВНИМАНИЕ: Будут удалены ВСЕ контейнеры и очищены базы данных/HDFS!")
    confirm = input("Вы уверены, что хотите продолжить? (y/n): ")
    if confirm.lower() == 'y':
        cmd = ["docker", "compose", "down", "-v"]
        run_command(cmd)
        print("\n✅ Кластер остановлен, все volumes полностью очищены.")
    else:
        print("\n❌ Операция отменена.")

def main():
    if len(sys.argv) < 2:
        # Если запустили без параметров — по умолчанию стартуем кластер
        start()
        return

    action = sys.argv[1].lower()

    if action == "start":
        start()
    elif action == "stop":
        stop()
    elif action == "clean":
        clean()
    else:
        print("\n❌ Неизвестная команда!")
        print("Используйте один из параметров:")
        print("  python manage.py start  - запустить кластер (по умолчанию)")
        print("  python manage.py stop   - остановить контейнеры")
        print("  python manage.py clean  - остановить кластер и СТЕРЕТЬ все данные в базах/HDFS")

if __name__ == "__main__":
    main()