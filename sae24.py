import random
from paho.mqtt import client as mqtt_client
import csv

broker = 'test.mosquitto.org'
port = 1883
topic = "IUT/Colmar/SAE24/Maison1"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'emqx'
password = 'public'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        donnee= msg.payload.decode()

        data= donnee.split (',')
        mac = data[0][3:len(data[0])]
        piece = data[1][6:len(data[1])]
        date = data[2][5:len(data[2])]
        time = data[3][5:len(data[3])]
        temp = data[4][5:len(data[4])]

        dataend = []
        dataend.append(mac)
        dataend.append(piece)
        dataend.append(date)
        dataend.append(time)
        dataend.append(temp)
        print(dataend)

        with open('donnee_mqtt.csv', 'a+', newline='') as f:
            write =csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            write.writerow(dataend)


    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()