import csv 
import io 
import json
import logging
import requests
from kafka import KafkaProducer

logger = logging.getLogger("airflow.task")

def fetch_and_push_data(logical_date, firms_key: str, bootstrap_servers: list, topic_name: str):
    """Скачивает термоточки из NASA FIRMS API и отправляет в Kafka. """
    target_date = logical_date.strftime("%Y-%m-%d")
    current_hour = logical_date.hour

    satellites = ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT"]
    bbox_list = [97.836607, 50.414121, 115.177044, 58.906169]
    region_bbox = ",".join(map(str, bbox_list))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        max_block_ms=5000  
    )

    for sat in satellites:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{firms_key}/{sat}/{region_bbox}/1/{target_date}"
        logger.info(f"Запрос данных для спутника {sat} за дату {target_date}")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            csv_data = io.StringIO(response.text)
            fires = list(csv.DictReader(csv_data))
            
        except Exception as e:
            logger.error(f"Критическая ошибка при обработке спутника {sat}: {e}")
            producer.close()
            raise 

        for fire in fires:
            if "acq_time" not in fire or "latitude" not in fire or "longitude" not in fire:
                continue

            acq_time_clean = fire["acq_time"].zfill(4)
            # fire_hour = int(acq_time_clean[:2])

            # payload = {
            #     "lat": float(fire["latitude"]),
            #     "lon": float(fire["longitude"]),
            #     "frp": float(fire["frp"]),
            #     "temp": float(fire.get("bright_ti4", fire.get("brightness", 0))),
            #     "timestamp": f"{fire['acq_date']} {acq_time_clean[:2]}:{acq_time_clean[2:]}:00",
            #     "satellite": sat,
            #     "confidence": fire.get("confidence", "N")
            # }

            # msg_key = f"{payload['lat']}_{payload['lon']}_{fire['acq_date']}_{acq_time_clean}"
            # producer.send(topic_name, key=msg_key.encode("utf-8"), value=payload)

            data = fire.copy();
            data["satellite_name"] = sat
            data["custom_timestamp"] = f"{fire['acq_date']} {acq_time_clean[:2]}:{acq_time_clean[2:]}:00"

            msg_key = f"{fire['latitude']}_{fire['longitude']}_{fire['acq_date']}_{acq_time_clean}"
            producer.send(topic_name, key=msg_key.encode("utf-8"), value=data)

    producer.flush()
    producer.close()
    logger.info(f"Продюсер успешно обработал батч за час: {current_hour}:00")