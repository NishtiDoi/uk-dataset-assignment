import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import requests
import logging
from tqdm import tqdm
from time import time
import json

column_names = [
    'uuid', 'price', 'date', 'postcode', 'type', 'isNew', 'duration', 'code',
    'dNo', 'street', 'locality', 'town', 'district', 'county', 'skip1', 'skip2'
]

app = FastAPI()

DATA_URL = "http://prod1.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
DATA_FOLDER = Path("data")
DATA_FILE = DATA_FOLDER / "uk_property_dataset.csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

download_status = {"status": "not started"}

async def download_dataset():
    global download_status

    if DATA_FILE.exists():
        download_status["status"] = "already downloaded"
        return

    try:
        download_status["status"] = "downloading"
        DATA_FOLDER.mkdir(parents=True, exist_ok=True)

        response = requests.get(DATA_URL, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        progress = tqdm(total=total_size, unit="iB", unit_scale=True)

        with open(DATA_FILE, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  
                    f.write(chunk)
                    progress.update(len(chunk))
        
        progress.close()  
        download_status["status"] = "Downloaded"

    except Exception as e:
        download_status["status"] = "failed"
        download_status["error"] = str(e)

@app.get("/download")
async def download_data(background_tasks: BackgroundTasks):
    global download_status

    if DATA_FILE.exists():
        download_status["status"] = "already downloaded"
        return JSONResponse(status_code=200, content={"message": "Data has already been downloaded."})
    elif download_status["status"] == "downloading":
        return JSONResponse(status_code=202, content={"message": "Data download is in progress."})
    elif download_status["status"] == "failed":
        return JSONResponse(status_code=500, content={"error": download_status["error"]})
    else:
        background_tasks.add_task(download_dataset)
        return JSONResponse(status_code=202, content={"message": "Data download initiated."})

@app.get("/deduplicate")
async def deduplicate_data():
    global download_status
    
    if DATA_FILE.exists():
        try:
            print("Deduplication initiated..")
            start_time = time()
            
            deduplicated_data = []
            chunk_size = 10000  
            
            for chunk in pd.read_csv(DATA_FILE, chunksize=chunk_size, header=None, names=column_names, dtype=str):
                deduplicated_chunk = chunk.drop_duplicates(subset=["street", "locality", "town", "district", "county"])
                deduplicated_data.append(deduplicated_chunk)
            
            deduplicated_df = pd.concat(deduplicated_data, ignore_index=True)
            deduplicated_df.to_csv(DATA_FOLDER / "deduplicated_uk_property_dataset.csv", index=False)
            
            deduplicated_df.replace({pd.NA: None, float('inf'): None, float('-inf'): None}, inplace=True)

            end_time = time()
            duration = end_time - start_time
            logger.info(f"Data deduplication completed successfully in {duration:.2f} seconds.")
            
            deduplicated_json = deduplicated_df.to_json(orient="records")
            del deduplicated_df
            deduplicated_output = json.loads(deduplicated_json)

            json_str = json.dumps(deduplicated_output, indent=4, default=str)
            return JSONResponse(content=json_str, media_type='application/json')
        
        except Exception as e:
            logger.error(f"An error occurred during deduplication: {e}")
            raise HTTPException(status_code=500, detail=f"Internal Server Error during deduplication. Error: {e}")
    else:
        logger.warning("Data file not found for deduplication.")
        return JSONResponse(status_code=404, content={"message": "Data file not found for deduplication"})

column_types = {column: 'object' for column in column_names}

@app.get("/data/{uuid}")
async def get_data(uuid: str):
    try:
        deduplicated_file = DATA_FOLDER / "deduplicated_uk_property_dataset.csv"
        df = pd.read_csv(deduplicated_file, header=None, names=column_names, low_memory=False)

        filtered_data = df[df["uuid"] == uuid].to_dict(orient="records")

        if filtered_data:
            for record in filtered_data:
                for key, value in record.items():
                    if pd.isna(value) or pd.isnull(value) or value in (float('inf'), float('-inf')):
                        record[key] = None

            return filtered_data[0] 
        else:
            raise HTTPException(status_code=404, detail="UUID not found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error.") from e
