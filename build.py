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

def init():
    """Инициализация HDFS: форматирование NameNode и правка прав доступа"""
    print("\n🔧 Инициализация HDFS...")
    
    # 1. Форматируем NameNode через init-профиль
    print("   • Форматирование NameNode...")
    cmd_format = ["docker", "compose", "--profile", "init", "up", "namenode-format"]
    run_command(cmd_format)
    
    # 2. Ждём завершения контейнера форматтера
    print("   • Ожидание завершения форматирования...")
    cmd_wait = ["docker", "wait", "namenode-format"]
    run_command(cmd_wait)
    
    # 3. Чиним права на томе: форматтер работал от root, а неймнода от hadoop
    print("   • Исправление прав доступа на томе namenode_data...")
    cmd_chown = [
        "docker", "run", "--rm",
        "-v", "hdfscluster_namenode_data:/hadoop/dfs/name",
        "--user", "root",
        "apache/hadoop:3.5.0",
        "chown", "-R", "hadoop:hadoop", "/hadoop/dfs/name"
    ]
    run_command(cmd_chown)
    cmd_chown_dn = [
        "docker", "run", "--rm",
        "-v", "hdfscluster_datanode_data:/hadoop/dfs/data",
        "--user", "root",
        "apache/hadoop:3.5.0",
        "chown", "-R", "hadoop:hadoop", "/hadoop/dfs/data"
    ]
    run_command(cmd_chown_dn)
    print("\n✅ HDFS успешно инициализирована!")

def start():
    """Запуск всего Enterprise-кластера"""
    if not os.path.exists(ENV_FILE_PATH):
        print(f"\n❌ Ошибка: Файл секретов не найден по пути {ENV_FILE_PATH}")
        print("Пожалуйста, создайте его перед запуском.")
        sys.exit(1)
        
    print("\n🚀 Запуск Big Data кластера (Hadoop + Spark + Airflow)...")
    cmd = ["docker", "compose", "up", "-d", "--build"]
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
        run_command(["docker", "compose", "--profile", "init", "down", "-v"])
        
        # На всякий случай — принудительно удаляем все контейнеры проекта
        run_command(["docker", "rm", "-f", "namenode-format", "namenode", "datanode", 
                      "resourcemanager", "nodemanager", "historyserver", "spark", 
                      "postgres", "airflow-init", "airflow-api-server", 
                      "airflow-dag-processor", "airflow-scheduler"])
        
        # Удаляем все тома проекта
        run_command(["docker", "volume", "rm", "-f", "hdfscluster_namenode_data",
                      "hdfscluster_datanode_data", "hdfscluster_history_data",
                      "hdfscluster_postgres_data", "hdfscluster_spark_bin",
                      "hdfscluster_java_bin"])
        
        print("\n✅ Кластер остановлен, все volumes полностью очищены.")
    else:
        print("\n❌ Операция отменена.")

def main():
    if len(sys.argv) < 2:
        start()
        return

    action = sys.argv[1].lower()

    if action == "init":
        init()
    elif action == "start":
        start()
    elif action == "stop":
        stop()
    elif action == "clean":
        clean()
    else:
        print("\n❌ Неизвестная команда!")
        print("Используйте один из параметров:")
        print("  python manage.py init   - инициализировать HDFS (первый запуск)")
        print("  python manage.py start  - запустить кластер")
        print("  python manage.py stop   - остановить контейнеры")
        print("  python manage.py clean  - остановить кластер и СТЕРЕТЬ все данные")

if __name__ == "__main__":
    main()