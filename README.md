# ELT-Процесс, погодные данные с OpenMeteo
ETL-пайплайн на Apache Airflow для получения, обработки и сохранения данных о погоде в трёх городах мира.

Проект создан в рамках изучения Data Engineering и демонстрирует практическое применение Apache Airflow.

## Установка и запуск

**Требования**
- Docker
- Docker Compose

## Шаги
1. Клонируй репозиторий:
```bashgit
clone https://github.com/elivlikswer/airflow-elt-openmeteo.git
cd airflow-pet-project
```
2. Создай файл .env:
```bashecho 
"AIRFLOW_UID=$(id -u)" > .env
```
3. Инициализируй базу данных Airflow:
```bashdocker
compose up airflow-init
```
4. Запусти Airflow:
```bashdocker
compose up -d
```
5. Открой браузер:
```commandline
http://localhost:8080
Логин: airflow
Пароль: airflow
```

## Настройка проекта
#### 1. В Airflow UI перейди в Admin → Connections и создай:

| Поле | Значение |
|---|---|
| Connection Id | `open_meteo` |
| Connection Type | `HTTP` |
| Host | `https://api.open-meteo.com` |

#### 2. Variables 
**`data`** (тип JSON) — координаты городов:
```json
{
  "Tokyo":  {"lat": 35.68, "lon": 139.69},
  "Sydney": {"lat": -33.87, "lon": 151.21},
  "London": {"lat": 51.50, "lon": 0.12}
}
```
 
**`api_url`** — шаблон URL запроса:
```
/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true
```

## Пример вывода

```bashdocker
 ========================================
      DAILY REPORT FOR WEATHER
======================================== 
 Done. Returned value was: None
   city      temp_c temp_f status
   Tokyo     13.6   56.48  comfortable
   London    10.5   50.90  comfortable
   Sydney    18.5   65.30  comfortable
========================================
```