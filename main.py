import os
import asyncio
import websockets
import json
import sqlite3
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# IMO: The IMO number is a seven-digit unique reference number. It is assigned to each ship for
# identification purposes and is permanently associated with the hull.
#
# MMSI: MMSI is a unique nine-digit number for identifying a ship. It is programmed into all AIS
# systems and VHF electronics on board of the vessel. The number is international standardized
# for contacting vessels. In some cases, it is possible that the MMSI number of a vessel changes,
# e.g. the vessel is sold or long-term chartered and the flag changes.
#
# ENI: The ENI number is an eight-digit unique registration number for inland vessels. The
# number is permanently assigned to a vessel for its lifetime.
#

# Details on the fields: https://www.aisstream.io/documentation#ShipStaticData
json_to_sql_column_mapping = {
    "MessageType": "message_type",
    "Message_ShipStaticData_AisVersion": "ais_version",
    "Message_ShipStaticData_CallSign": "call_sign",
    "Message_ShipStaticData_Destination": "destination",
    "Message_ShipStaticData_Dimension_A": "dim_a",
    "Message_ShipStaticData_Dimension_B": "dim_b",
    "Message_ShipStaticData_Dimension_C": "dim_c",
    "Message_ShipStaticData_Dimension_D": "dim_d",
    "Message_ShipStaticData_Dte": "dte",
    "Message_ShipStaticData_Eta_Day": "eta_day",
    "Message_ShipStaticData_Eta_Hour": "eta_hour",
    "Message_ShipStaticData_Eta_Minute": "eta_minute",
    "Message_ShipStaticData_Eta_Month": "eta_month",
    "Message_ShipStaticData_FixType": "fix_type",
    "Message_ShipStaticData_ImoNumber": "imo_number",
    "Message_ShipStaticData_MaximumStaticDraught": "max_static_draught",
    "Message_ShipStaticData_MessageID": "message_id",
    "Message_ShipStaticData_Name": "name",
    "Message_ShipStaticData_RepeatIndicator": "repeat_indicator",
    "Message_ShipStaticData_Spare": "spare",
    "Message_ShipStaticData_Type": "type",
    "Message_ShipStaticData_UserID": "user_id",
    "Message_ShipStaticData_Valid": "valid",
    "MetaData_MMSI": "mmsi",
    "MetaData_ShipName": "ship_name",
    "MetaData_latitude": "latitude",
    "MetaData_longitude": "longitude",
    "MetaData_time_utc": "time_utc",
}

def flatten_json(json_obj):
    flattened = {}

    def flatten(item, prefix=''):
        if isinstance(item, dict):
            for key, value in item.items():
                full_key = f"{prefix}{key}"
                if full_key in json_to_sql_column_mapping:
                    mapped_key = json_to_sql_column_mapping[full_key]
                    flatten(value, f"{mapped_key}_")
                else:
                    flatten(value, f"{full_key}_")
        else:
            flattened[prefix[:-1]] = item

    flatten(json_obj)
    return flattened


def init_db():
    conn = sqlite3.connect("ais_data.db")
    c = conn.cursor()
    column_types = {
        "id": "INTEGER PRIMARY KEY",
        "timestamp": "DATETIME DEFAULT CURRENT_TIMESTAMP",
        "mmsi": "INTEGER",
        "imo_number": "INTEGER",
        "ship_name": "TEXT",
        "time_utc": "TEXT",
        "latitude": "REAL",
        "longitude": "REAL",
        "ais_version": "INTEGER",
        "call_sign": "TEXT",
        "destination": "TEXT",
        "dim_a": "INTEGER",
        "dim_b": "INTEGER",
        "dim_c": "INTEGER",
        "dim_d": "INTEGER",
        "dte": "BOOLEAN",
        "eta_day": "INTEGER",
        "eta_hour": "INTEGER",
        "eta_minute": "INTEGER",
        "eta_month": "INTEGER",
        "fix_type": "INTEGER",
        "max_static_draught": "REAL",
        "message_id": "INTEGER",
        "name": "TEXT",
        "repeat_indicator": "INTEGER",
        "spare": "BOOLEAN",
        "type": "INTEGER",
        "user_id": "INTEGER",
        "valid": "BOOLEAN",
        "message_type": "TEXT",
    }

    table_columns = ", ".join([f"{col} {col_type}" for col, col_type in column_types.items()])

    c.execute(f"CREATE TABLE IF NOT EXISTS ais_data ({table_columns})")
    conn.commit()

    return conn

async def connect_ais_stream(time_limit_seconds):
    load_dotenv()
    api_key = os.getenv("API_KEY")

    start_time = datetime.now(timezone.utc)
    time_limit = timedelta(seconds=time_limit_seconds)

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        # [34.476303252067794, -32.10294030310476], [-4.439310436487275, 3.548062666131116] - West African Coastline
        subscribe_message = {"APIKey": api_key, "BoundingBoxes":  [[[34.476303252067794, -32.10294030310476], [-4.439310436487275, 3.548062666131116]]]}

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)

        conn = init_db()
        c = conn.cursor()
        count_records = 0

        async for message_json in websocket:
            message = json.loads(message_json)
            message_type = message["MessageType"]

            if message_type == "ShipStaticData":
                #print("Original message JSON:", message) 
                flattened_message = flatten_json(message)

                column_value_pairs = [(column, value) for column, value in flattened_message.items()]
                columns = ', '.join(pair[0] for pair in column_value_pairs)
                placeholders = ', '.join('?' for _ in column_value_pairs)
                values = tuple(pair[1] for pair in column_value_pairs)

                sql = f"INSERT INTO ais_data ({columns}) VALUES ({placeholders})"
                #print("SQL query:", sql)  # Print the SQL query
                #print("Values:", values)  # Print the values
                c.execute(sql, values)
                conn.commit()
                count_records = count_records + 1

                print("# ", count_records, values)

            if datetime.now(timezone.utc) - start_time > time_limit:
                break

        conn.close()

if __name__ == "__main__":
    asyncio.run(connect_ais_stream(time_limit_seconds=3000))

