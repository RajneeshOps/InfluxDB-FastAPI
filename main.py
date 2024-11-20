from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import Optional


# FastAPI app initialization
app = FastAPI()

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"  # Replace with your InfluxDB URL
TOKEN = ""           # Replace with your InfluxDB token
ORG = "opstree"                        # Replace with your organization name
BUCKET = "mybucketry"                  # Replace with your bucket name

# Initialize InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Pydantic model for request body validation
class DataPoint(BaseModel):
    measurement: str
    tags: dict  # Key-value pairs for tags
    fields: dict  # Key-value pairs for fields
    timestamp: str  # ISO 8601 format timestamp (e.g., "2024-11-20T12:00:00Z")

@app.post("/insert-data")
async def insert_data(data: DataPoint):
    try:
        # Create a Point object
        point = Point(data.measurement)
        for key, value in data.tags.items():
            point = point.tag(key, value)
        for key, value in data.fields.items():
            point = point.field(key, value)
        point = point.time(data.timestamp)

        # Write data to InfluxDB
        write_api.write(bucket=BUCKET, org=ORG, record=point)
        return {"message": "Data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting data: {str(e)}")

delete_api = client.delete_api()

class DeleteRange(BaseModel):
    start_time: str  # ISO 8601 format, e.g., "2024-01-01T00:00:00Z"
    stop_time: str   # ISO 8601 format, e.g., "2024-01-02T00:00:00Z"
    measurement: str # Measurement name

@app.delete("/delete")
async def delete_data_between_ranges(data: DeleteRange):
    """
    Delete data from InfluxDB between a specified time range.

    Args:
        data (DeleteRange): The time range and measurement details.

    Returns:
        str: Success message.
    """
    try:
        # Construct a predicate for the measurement
        predicate = f'_measurement="{data.measurement}"'

        # Execute the delete operation
        delete_api.delete(
            start=data.start_time,
            stop=data.stop_time,
            predicate=predicate,
            bucket=BUCKET,
            org=ORG
        )
        return {"message": "Data successfully deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


