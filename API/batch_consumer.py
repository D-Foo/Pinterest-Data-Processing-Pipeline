from kafka import KafkaConsumer
import json
from json import loads
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
#import os

class BatchConsumer: 

    def __init__(self):
    # create our consumer to retrieve the message from the topics
        self.data_stream_consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",    
            value_deserializer=lambda message: loads(message),
            auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
        )
        self.message_counter = 1
        self.data_folder_path = 'data'
        self.bucket_name = 'pindatabucket'
        self.s3_client = boto3.client('s3')

    def upload_event(self) -> bool:
        try:
            #file_size = os.path.getsize(f'{batch_consumer.data_folder_path}/{self.message_counter:04d}.json')
            #print(f'Uploading {batch_consumer.data_folder_path}/{self.message_counter:04d}.json, Size is: {file_size}B')
            self.s3_client.upload_file(f'{batch_consumer.data_folder_path}/{self.message_counter:04d}.json', self.bucket_name, f'{batch_consumer.data_folder_path}/{self.message_counter:04d}.json')
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
        except ClientError as err:
            print(f"Client Error: {err=}, {type(err)=}")
            return False
        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
        return True

if __name__ == '__main__':
    batch_consumer = BatchConsumer()
    batch_consumer.data_stream_consumer.subscribe(topics=["PinTopic"])

    # Loops through all messages in the consumer handles the event
    for message in batch_consumer.data_stream_consumer:
        out_file = open(f'{batch_consumer.data_folder_path}/{batch_consumer.message_counter:04d}.json' , 'w')
        json.dump(message, out_file)
        out_file.close()
        batch_consumer.upload_event()
        batch_consumer.message_counter += 1

        
