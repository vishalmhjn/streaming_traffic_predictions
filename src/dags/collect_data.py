import requests
import xml.etree.ElementTree as Xet

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import time
from random import choice

import requests

import time
import pandas as pd
import datetime as dt


def traffic_xml2csv(root):
    sample_12 = []
    sample_9 = []
    for child in root:
        if child.tag == "fecha_hora":
            continue
        sample = []
        for i in range(len(child)):
            sample.append(child[i].text)
        if len(child) == 12:
            sample_12.append(sample)
        else:
            sample_9.append(sample)

    cols1 = [
        "idelem",
        "descripcion",
        "accesoAsociado",
        "intensidad",
        "ocupacion",
        "carga",
        "nivelServicio",
        "intensidadSat",
        "error",
        "subarea",
        "st_x",
        "st_y",
    ]
    cols2 = [
        "idelem",
        "intensidad",
        "ocupacion",
        "carga",
        "nivelServicio",
        "velocidad",
        "error",
        "st_x",
        "st_y",
    ]
    data1 = pd.DataFrame(sample_12, columns=cols1)
    data2 = pd.DataFrame(sample_9, columns=cols2)
    data = pd.concat([data1, data2], axis=0)

    return data


def get_feed():
    timestr = time.strftime("%Y%m%d-%H%M%S")
    url = "https://informo.madrid.es/informo/tmadrid/pm.xml"
    try:
        resp = requests.get(url, allow_redirects=True)
        root = Xet.fromstring(str(resp.content, "utf-8"))
        data = traffic_xml2csv(root)
        data.to_csv(
            "output_traffic_madrid/mtd_" + timestr + ".csv",
            index=False,
        )
    except:
        print("Oops!  That was no valid data. Try again...\n\n" + resp.content)


default_args = {
    "owner": "vishal",
    "start_date": dt.datetime(
        2023, 8, 18, 11, 0, 0, tzinfo=dt.timezone(dt.timedelta(seconds=7200))
    ),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "MadridTrafficData",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
) as dag:
    print_starting = BashOperator(
        task_id="starting",
        bash_command='echo "Starting now....."',
    )
    csvsave = PythonOperator(task_id="collectingmadriddata", python_callable=get_feed)
print_starting >> csvsave
