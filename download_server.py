import os
import json
import gzip
import argparse
import paho.mqtt.client as paho


def get_args():
    parser = argparse.ArgumentParser('MQTT upload server:')
    parser.add_argument('-b', '--broker', type=str, default='test.mosquitto.org',
                        help='MQTT broker')
    parser.add_argument('-p', '--upload_file_path', type=str, default='./signal_data/sample',
                        help='the path of the file to upload')
    parser.add_argument('--TLS_service', type=bool, default=False,
                        help='whether to use encrypted transmission')
    parser.add_argument('--TLS_addr', type=str,
                        default="./mosquitto.org.crt", help='path of client certificate')
    args = parser.parse_args()
    return args

class Download_Server:
    def __init__(self,args):
        # mqtt setting
        self.broker = args.broker
        self.TLS_service = args.TLS_service
        self.TLS_addr = args.TLS_addr

        # MQTT topic names
        self.topic = "data/files"
        self.topic_latency = "data/files/latency"
        self.topic_bandwidth = "data/files/bandwidth"
        self.topic_broken_file = "data/files/broken_file"

        self.bytes_received = 0

    def decompress(self, file_name, Decompress):
        # decompress received file
        if Decompress:
            with gzip.open(file_name, 'rb') as zip_file:
                zip_content = zip_file.read()
                # open new file, prepare to write
                out_file = open(file_name[:-3], "wb")  
                out_file.write(zip_content)
                out_file.close()
            os.remove(file_name)

    def extract_file_data(self, file_data):
        data = json.loads(file_data)
        filename = data["filename"]
        return filename
    
    #  main receiver code, check packet type:
    #  header->open new file; end->close file; file data->write
    def process_message(self, msg):
        # check received message is header or end
        if len(msg) == 200:  
            msg_in = msg.decode("utf-8", "ignore")
            msg_in = msg_in.split(",,")
            if msg_in[0] == "header":  # header
                self.filename = self.extract_file_data(msg_in[1])
                if self.filename[-3:] == '.gz':  # if compressed file
                    self.Decompress = True
                else:
                    self.Decompress = False

                # rename output file
                self.file_out = "./download/" + self.filename  
                # open new file, prepare to write
                self.fout = open(self.file_out, "wb")  
            
            # check is it really last packet?
            if msg_in[0] == "end":  
                # check right file received?
                if not str(self.bytes_received) == msg_in[3]:  
                    print("broken file", self.filename, self.bytes_received, msg_in[3])
                    self.c_publish(self.client, self.topic_broken_file, self.filename, 0)

                self.bytes_received = 0
                self.fout.close()
                self.decompress(self.file_out, self.Decompress)
                return False
            else:
                # write file data
                if msg_in[0] != "header":  
                    return True
                else:
                    return False
        else:
            if len(msg) < 100:
                print(msg)
            self.bytes_received = self.bytes_received + len(msg)
            return True

    def on_message(self, client, userdata, message):
        if message.topic == "data/files":
            filedata = self.process_message(message.payload)
            try:
                if filedata:
                    self.fout.write(message.payload)
                else:
                    print("received")
            except:
                print("no header or end received")
        else:
            print(message.topic + " " + " " + str(message.payload))

    def c_publish(self, client, topic, out_message, qos):
        res, mid = client.publish(topic, out_message, qos)


    def download(self):
        # create client and initialize
        client = paho.Client("client-receive-file")
        client.on_message = self.on_message
        client.mid_value = None

        # connect to broker
        if self.TLS_service:
            client.tls_set(self.TLS_addr)
            port = 8883
        else:
            port = 1883
        client.connect(self.broker, port)
        print("connected to broker ", self.broker)

        # subscribe to topic
        print("subscribing ", self.topic)
        client.subscribe(self.topic, qos=1)
        client.subscribe(self.topic_bandwidth)
        client.subscribe(self.topic_latency)

        client.loop_forever(),

if __name__ == '__main__':
    args = get_args()
    Server = Download_Server(args)
    Server.download()
