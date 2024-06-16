import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

import paho.mqtt.client as mqtt
import yaml
import json

config = {}


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    if "metrics" in config.keys():
        for topic in config["metrics"].keys():
            print(topic)
            client.subscribe(topic)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))
    print(config["metrics"][msg.topic])
    topic_config = config["metrics"][msg.topic]
    payload = json.loads(msg.payload.decode('utf-8'))
    value = payload

    for token in topic_config["value_path"]:
        buffer = value
        value = buffer[token]
    print(value)

    if "remap" in topic_config.keys():
        print("remapping")
        remap = topic_config["remap"]
        value = ((value - remap["in_min"]) / (remap["in_max"] - remap["in_min"])) * (
                    remap["out_max"] - remap["out_min"]) + remap["out_min"]
        value = int(value)
    print(value)

    p = influxdb_client.Point(topic_config["metric_name"])
    p.field("value", value)
    for key, value in topic_config["labels"].items():
        p.tag(key, value)

    write_api.write(bucket=bucket, org=org, record=p)


bucket = "metrics"
org = "<my-org>"
token = "<my-token>"
# Store the URL of your InfluxDB instance
url = "192.168.1.230:8086"

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)

# Write script
write_api = client.write_api(write_options=SYNCHRONOUS)
#


if __name__ == '__main__':
    with open('config.yml', 'r') as file:
        config = yaml.safe_load(file)
        print(config)

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    mqttc.connect("192.168.1.230", 1883, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    mqttc.loop_forever()
