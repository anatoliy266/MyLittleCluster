# Bootstrap-файл инфраструктуры.
# Не содержит ни одного сервиса — только сети. Запускается один раз:
#   docker compose -f infra/compose.yaml up -d
# Все остальные стеки подключаются к этим сетям через external: true
# и самостоятельно сети НЕ создают.


REQUIRED_NETWORKS=("cluster" "kafka-net")

echo "Проверка и создание общих сетей Docker..."

for net in "${REQUIRED_NETWORKS[@]}"; do
    # Проверяем, существует ли сеть. Если да, команда вернет 0 (истина)
    if docker network inspect "$net" >/dev/null 2>&1; then
        echo "Сеть '$net' уже существует. Пропускаем."
    else
        echo "Создание общей сети: $net..."
        docker network create --driver bridge --attachable "$net"
    fi
done

echo -e "\nИнфраструктура готова к работе!"