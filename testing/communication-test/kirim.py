import sys
import time
import random
import cv2
from kafka import KafkaProducer
import json
from bson import json_util
from datetime import datetime
import base64
from json import dumps

topic = "message-sensor-2k93k2"

def publish_video(video_file):
    """
    Publish given video file to a specified Kafka topic. 
    Kafka Server is expected to be running on the localhost. Not partitioned.
    
    :param video_file: path to video file <string>
    """
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='103.106.72.187:9092')

    # Open file
    video = cv2.VideoCapture(video_file)
    
    print('publishing video...')

    while(video.isOpened()):
        success, frame = video.read()

        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        
        # Convert image to png
        ret, buffer = cv2.imencode('.jpg', frame)

        # Convert to bytes and send to kafka
        producer.send(topic, buffer.tobytes())

        time.sleep(0.2)
    video.release()
    print('publish complete')

    
def publish_camera():
    """
    Publish camera video stream to specified Kafka topic.
    Kafka Server is expected to be running on the localhost. Not partitioned.
    """

    # Start up producer
    producer = KafkaProducer(bootstrap_servers=['103.106.72.187:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

    
    camera = cv2.VideoCapture(0)
    try:
        while(True):
            success, frame = camera.read()
        
            ret, buffer = cv2.imencode('.jpg', frame)
            # producer.send(topic, buffer.tobytes())

            data = {
                "device_code": "2k93k2-hf83",
                "date_add": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
                "image":base64.b64encode(buffer).decode(),
                "gps":{
                    "latitude":-7.575973 + (random.randint(1,1000) / 10000 ),
                    "longitude":112.878304 - ( random.randint(1,1000) / 10000 )
                },
                "temperature": random.randint(1900,4000) / 100,
                "fuel":900
            }
            # print(data)
            producer.send(topic, value=data)

            
            # Choppier stream, reduced load on processor
            time.sleep(1)

    except Exception as e:
        print("\n")
        print(e)
        print("\nExiting.")
        sys.exit(1)

    
    camera.release()


if __name__ == '__main__':
    """
    Producer will publish to Kafka Server a video file given as a system arg. 
    Otherwise it will default by streaming webcam feed.
    """
    if(len(sys.argv) > 1):
        video_path = sys.argv[1]
        publish_video(video_path)
    else:
        print("publishing feed!")
        publish_camera()