import os
import asyncio
import websockets
import json
import csv
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

def flatten_json(json_obj):
    flattened = {}

    def flatten(item, prefix=''):
        if isinstance(item, dict):
            for key, value in item.items():
                flatten(value, f"{prefix}{key}_")
        else:
            flattened[prefix[:-1]] = item

    flatten(json_obj)
    return flattened

async def connect_ais_stream(max_readings=1000, time_limit_seconds=60):
    load_dotenv()
    api_key = os.getenv("API_KEY")

    readings_count = 0
    start_time = datetime.now(timezone.utc)
    time_limit = timedelta(seconds=time_limit_seconds)

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {"APIKey": api_key, "BoundingBoxes": [[[-180, -90], [180, 90]]]}

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)

        with open("output.csv", "w", newline='') as csvfile:
            fieldnames = [
                "Message_ShipStaticData_AisVersion",
                "Message_ShipStaticData_CallSign",
                "Message_ShipStaticData_Destination",
                "Message_ShipStaticData_Dimension_A",
                "Message_ShipStaticData_Dimension_B",
                "Message_ShipStaticData_Dimension_C",
                "Message_ShipStaticData_Dimension_D",
                "Message_ShipStaticData_Dte",
                "Message_ShipStaticData_Eta_Day",
                "Message_ShipStaticData_Eta_Hour",
                "Message_ShipStaticData_Eta_Minute",
                "Message_ShipStaticData_Eta_Month",
                "Message_ShipStaticData_FixType",
                "Message_ShipStaticData_ImoNumber",
                "Message_ShipStaticData_MaximumStaticDraught",
                "Message_ShipStaticData_MessageID",
                "Message_ShipStaticData_Name",
                "Message_ShipStaticData_RepeatIndicator",
                "Message_ShipStaticData_Spare",
                "Message_ShipStaticData_Type",
                "Message_ShipStaticData_UserID",
                "Message_ShipStaticData_Valid",
                "MessageType",
                "MetaData_MMSI",
                "MetaData_ShipName",
                "MetaData_latitude",
                "MetaData_longitude",
                "MetaData_time_utc",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            async for message_json in websocket:
                message = json.loads(message_json)
                message_type = message["MessageType"]

                if message_type == "ShipStaticData":
                    flattened_message = flatten_json(message)
                    writer.writerow(flattened_message)
                    readings_count += 1

                if readings_count >= max_readings or datetime.now(timezone.utc) - start_time > time_limit:
                    break

if __name__ == "__main__":
    asyncio.run(connect_ais_stream(max_readings=10, time_limit_seconds=60))

