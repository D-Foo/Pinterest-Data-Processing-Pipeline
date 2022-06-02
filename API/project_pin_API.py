from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaClient
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata
from kafka.errors import TopicAlreadyExistsError

app = FastAPI()



class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="PinProducer",
    value_serializer=lambda pindata: dumps(pindata).encode("ascii")
) 

@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send(topic="PinTopic", value=data)
    return item


if __name__ == '__main__':

    #Create topic if it doesn't exist
   
    client_conn = KafkaClient(
        bootstrap_servers="localhost:9092", # Specific the broker address to connect to
        client_id="Broker test" # Create an id from this client for reference
    )

    # Check that the server is connected and running
    print(client_conn.bootstrap_connected())

    # Create a new Kafka client to adminstrate our Kafka broker
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
        client_id="Kafka Administrator"
    )

    # topics must be pass as a list to the create_topics method
    topics = []
    topics.append(NewTopic(name="PinTopic", num_partitions=3, replication_factor=1))

    # Topics to create must be passed as a list
    try:
        admin_client.create_topics(new_topics=topics)
    except TopicAlreadyExistsError:
        print("PinTopic already exists")
        pass
    
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
