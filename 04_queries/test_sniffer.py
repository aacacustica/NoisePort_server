# import paho.mqtt.client as mqtt
# from config import *



# BROKER= MQTT_BROKER
# PORT= int(MQTT_PORT)
# TOPIC= "aacacustica/#"



# def on_connect(client, userdata, flags, rc):
#     print(f"Connected (rc={rc}) → subscribing to {TOPIC}")
#     client.subscribe(TOPIC)

# def on_message(client, userdata, msg):
#     print(f"[{msg.topic}] {msg.payload.decode('utf-8')}")


# def main():
#     client = mqtt.Client()
#     client.on_connect = on_connect
#     client.on_message = on_message

#     client.connect(BROKER, PORT, keepalive=60)
#     client.loop_forever()

# if __name__ == "__main__":
#     main()



import json
import paho.mqtt.client as mqtt
from config import MQTT_BROKER, MQTT_PORT

BROKER = MQTT_BROKER
PORT   = int(MQTT_PORT)
TOPIC  = "aacacustica/#"

def on_connect(client, userdata, flags, rc):
    print(f"[mqtt] Connected (rc={rc}), subscribing to {TOPIC}")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload_str = msg.payload.decode('utf-8', errors='replace')
    print(f"\n--- Message on topic {msg.topic} ---")
    # 1) print raw
    print("Raw payload:", payload_str)
    # 2) try to parse JSON and pretty-print
    try:
        data = json.loads(payload_str)
        print("Parsed JSON:")
        print(json.dumps(data, indent=2))
        # 3) optionally, if it's a list of records:
        if isinstance(data, list) and data and isinstance(data[0], dict):
            print("Keys in each record:", list(data[0].keys()))
    except json.JSONDecodeError:
        print("⚠️ Payload is not valid JSON")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, keepalive=60)
client.loop_forever()
