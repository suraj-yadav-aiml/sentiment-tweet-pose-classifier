from fastapi import FastAPI, Request
import uvicorn
import time

from transformers import pipeline
from transformers import AutoImageProcessor

from utility.data_model import NLPDataInput,NLPDataOutput,ImageDataInput,ImageDataOutput
from utility.helper_function import set_device, download_ml_models
from utility.constanst import *


app = FastAPI()

print("Downloading VIT TOkenizer...")
image_processor = AutoImageProcessor.from_pretrained(pretrained_model_name_or_path=POSE_ESTIMATION_TOKENIZER,
                                                     use_fast = True)

device = set_device()

model_path = [
        (S3_PREFIX_SENTIMENT_ANALYSIS, LOCAL_PATH_SENTIMENT_ANALYSIS),
        (S3_PREFIX_DISASTER_TWEET, LOCAL_PATH_DISASTER_TWEET),
        (S3_PREFIX_POSE_ESTIMATION, LOCAL_PATH_POSE_ESTIMATION)
    ]

download_ml_models(bucket_name=BUCKET_NAME, model_paths=model_path, force_download=False)

sentiment_model = pipeline('text-classification', model=LOCAL_PATH_SENTIMENT_ANALYSIS, device=device)
tweeter_model = pipeline('text-classification', model=LOCAL_PATH_DISASTER_TWEET, device=device)
pose_model = pipeline('image-classification', model=LOCAL_PATH_POSE_ESTIMATION, device=device, image_processor=image_processor)


@app.get("/")
def home():
    return "Server is running ..."

@app.post("/api/v1/sentiment_analysis")
def sentiment_analysis(data: NLPDataInput):
    start = time.time()
    output = sentiment_model(data.text)
    end = time.time()
    prediction_time = end-start

    labels = [prediction.get('label') for prediction in output]
    scores = [prediction.get('score') for prediction in output]

    return NLPDataOutput(
        model=LOCAL_PATH_SENTIMENT_ANALYSIS.split("/")[1],
        text=data.text,
        target=labels,
        score=scores,
        prediction_time=prediction_time
    )

@app.post("/api/v1/disater_classifier")
def disater_classifier(data: NLPDataInput):
    start = time.time()
    output = tweeter_model(data.text)
    end = time.time()
    prediction_time = end-start

    lables = [prediction['label'] for prediction in output]
    scores = [prediction['score'] for prediction in output]

    return NLPDataOutput(
        model=LOCAL_PATH_DISASTER_TWEET.split("/")[1],
        text = data.text,
        target=lables,
        score=scores,
        prediction_time=prediction_time
    )

@app.post("/api/v1/pose_classifier")
def pose_classifier(data: ImageDataInput):
    start = time.time()
    output = pose_model(data.url)
    end = time.time()
    prediction_time = end-start

    labels = [prediction[0]['label'] for prediction in output]
    scores = [prediction[0]['score'] for prediction in output]

    return ImageDataOutput(
        model = LOCAL_PATH_POSE_ESTIMATION.split("/")[1],
        url = data.url,
        target=labels,
        score=scores,
        prediction_time=prediction_time
    )




if __name__ == "__main__":
    uvicorn.run(app="app:app", port=8000, reload=True)
    
