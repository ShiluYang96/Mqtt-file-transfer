"""
Author: Steve 
Resource: http://www.steves-internet-guide.com/client-connections-python-mqtt/
"""

import time
import json
import sys
import getopt
import paho.mqtt.client as mqtt


def test_latency(broker_name, port_num, TLS_service, path, QOS):
    mqttclient_log = True
    max_connection_time = 6  # if not connected after this time assume failure
    cname = "pinger"
    msg = "ping test:" + cname
    topic = "pingtest"
    broker = broker_name
    port = port_num
    inputs = {"broker": broker, "port": port, "topic": "pingtest", "loops":
              4, "loop_delay": 1, "silent_flag": False, "username": "",
              "password": ""}

    mqttclient_log = False
    # end user editable data
    responses = []
    sent = []

    ####

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            client.connected_flag = True
            """print("connected sending ", inputs["loops"], " messages ", \
                  inputs["loop_delay"], " second Intervals")"""
        else:
            client.bad_connection_flag = True
            if rc == 5:
                print("broker requires authentication")

    def on_disconnect(client, userdata, rc):
        m = "disconnecting reason  ", str(rc)
        client.connect_flag = False
        client.disconnect_flag = True

    def on_subscribe(client, userdata, mid, granted_qos):
        # print("subscribed ok ")
        client.suback_flag = True

    def on_publish(client, userdata, mid):
        client.puback_flag = True

    def on_message(client, userdata, message):
        topic = message.topic
        msgr = str(message.payload.decode("utf-8"))
        responses.append(json.loads(msgr))
        client.rmsg_count += 1
        client.rmsg_flagset = True

    def on_log(client, userdata, level, buf):
        print("log: ", buf)

    def Initialise_client_object():
        mqtt.Client.bad_connection_flag = False
        mqtt.Client.suback_flag = False
        mqtt.Client.connected_flag = False
        mqtt.Client.disconnect_flag = False
        mqtt.Client.disconnect_time = 0.0
        mqtt.Client.disconnect_flagset = False
        mqtt.Client.rmsg_flagset = False
        mqtt.Client.rmsg_count = 0
        mqtt.Client.display_msg_count = 0

    def Initialise_clients(cname):
        # flags set
        client = mqtt.Client(cname)
        if mqttclient_log:  # enable mqqt client logging
            client.on_log = on_log
        client.on_connect = on_connect  # attach function to callback
        client.on_message = on_message  # attach function to callback
        client.on_disconnect = on_disconnect
        client.on_subscribe = on_subscribe
        client.on_publish = on_publish
        return client

    def get_input(argv):
        broker_in = ""
        port_in = 0
        topics_in = ""
        try:
            opts, args = getopt.getopt(argv, "h:p:t:c:d:su:P:")
        except getopt.GetoptError:
            print(sys.argv[0], " -h <broker> -p <port> -t <topic> -c <count> \
    -d <delay> -u <username> -P <pasword>-s <silent True>")
            sys.exit(2)
        for opt, arg in opts:

            if opt == '--help':
                print(sys.argv[0], " -h <broker> -p <port> -t <topic> -c <count> \
    -d <delay> -u <username> -P <pasword>-s <silent True>")
                sys.exit()
            elif opt == "-h":
                inputs["broker"] = str(arg)
            elif opt == "-t":
                inputs["topic"] = str(arg)
            elif opt == "-p":
                inputs["port"] = int(arg)
            elif opt == "-c":
                inputs["loops"] = int(arg)
            elif opt == "-u":
                inputs["username"] = str(arg)
            elif opt == "-P":
                inputs["password"] = str(arg)
            elif opt == "-d":
                inputs["loop_delay"] = int(arg)
            elif opt == "-s":
                inputs["silent_flag"] = True

        return ((broker_in, port_in, topics_in))

    # start
    if __name__ == "__main__":
        if len(sys.argv) >= 2:
            get_input(sys.argv[1:])
    ###
    Initialise_client_object()  # create object flags
    # create client object and set callbacks
    client = Initialise_clients("mqtt_ping_test_latency")

    if inputs["username"] != "":  # set username/password
        client.username_pw_set(
            username=inputs["username"], password=inputs["password"])

    """print("connecting to broker ", inputs["broker"], "on port ", inputs["port"], \
          " topic", inputs["topic"])"""
    try:
        if TLS_service:
            client.tls_set(path)
        else:
            pass
        # establish connection
        res = client.connect(inputs["broker"], inputs["port"])
    except:
        print("can't connect to broker", inputs["broker"])
        sys.exit()

    client.loop_start()
    # while not client.connected_flag and not client.bad_connection_flag:
    #    time.sleep(.25)
    if client.bad_connection_flag:
        print("connection failure to broker ", inputs["broker"])
        client.loop_stop()
        sys.exit()
    if inputs["silent_flag"]:
        print("Silent Mode is on")
    client.subscribe(inputs["topic"], qos=QOS)
    # while not client.suback_flag:  # wait for subscribe to be acknowledged
    #    time.sleep(.25)
    count = 0
    tbegin = time.time()

    try:
        while count < inputs["loops"]:
            wait_response_flag = True
            client.rmsg_flagset = False
            count += 1
            m_out = json.dumps((msg, count))
            sent.append(m_out)
            if not inputs["silent_flag"]:
                # print("sending:", m_out)
                pass
            client.publish(inputs["topic"], m_out, qos=QOS)  # publish
            tstart = time.time()
            # print("flags " ,wait_response_flag,client.rmsg_flagset)
            while wait_response_flag and (time.time() - tstart) < 3:
                if responses and client.rmsg_flagset:
                    ttrip = time.time() - tstart
                    if not inputs["silent_flag"]:
                        # print("received:", responses.pop(0), "time= %2.3f" % ttrip)
                        pass
                    wait_response_flag = False
            time.sleep(inputs["loop_delay"])

    except KeyboardInterrupt:
        pass

    tt = time.time() - tbegin
    latency = (tt - 4)/4
    # print("Total time= %2.2f" % tt)
    # print("Total sent=", count)
    # print("Total received=", client.rmsg_count)
    # print(latency)
    # time.sleep(2)
    client.disconnect()
    client.loop_stop()
    # time.sleep(2)
    return latency


"""
broker = "broker.hivemq.com"  # block:2MB, waiting_period:0.000001
# broker = "test.mosquitto.org"  # block:1MB, waiting_period:0.2
TLS_service = False
port = 1883
qos = 1
TLS_addr = "e:/PythonProject/mosquitto.org.crt"
while True:
    latency = test_latency(broker, port,TLS_service, TLS_addr, qos)
    print(latency)
    time.sleep(2)
"""
