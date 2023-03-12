import os
import sys
import time
import json

import gzip
import argparse
import tabulate
import mqtt_ping
import shutil
import paho.mqtt.client as paho


def get_args():
    parser = argparse.ArgumentParser('MQTT upload server:')
    parser.add_argument('-b', '--broker', type=str, default='test.mosquitto.org',
                        help='MQTT broker')
    parser.add_argument('--qos', type=int, default=1,
                        help='quality of service level')
    parser.add_argument('-c', '--compress_level', type=int, default=0,
                        help='whether to compress transfered data, 0->no compression, 1->gzip compression')
    parser.add_argument('-p', '--upload_file_path', type=str, default='./signal_data/sample',
                        help='the path of the file to upload')
    parser.add_argument('--TLS_service', type=bool, default=False,
                        help='whether to use encrypted transmission')
    parser.add_argument('--TLS_addr', type=str,
                        default="./mosquitto.org.crt", help='path of client certificate')
    args = parser.parse_args()
    return args


class Upload_Server:

    def __init__(self, args):
        # mqtt setting
        self.broker = args.broker  # block:max, waiting_period:none
        self.qos = args.qos
        self.waiting_period = 0.02
        self.data_block_size = 1000000

        self.TLS_service = args.TLS_service
        self.TLS_addr = args.TLS_addr
        # Compress = True
        self.Compress_level = args.compress_level

        # MQTT topic names
        self.topic = "data/files"
        self.topic_latency = "data/files/latency"
        self.topic_bandwidth = "data/files/bandwidth"
        self.topic_broken_file = "data/files/broken_file"

        self.broken_file = []
        self.connection_status = 6
        self.log_file = 'uploaded_files.txt'
        self.upload_file_path = args.upload_file_path

    def compress(self, filename_with_root):
        # if already compressed
        if filename_with_root[-3:] == '.gz':
            return filename_with_root

        if self.Compress_level > 0:
            # compress file
            with open(filename_with_root, 'rb') as f_in:
                with gzip.open(filename_with_root + '.gz', 'wb', compresslevel=self.Compress_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            compress_file_name = filename_with_root + '.gz'
            return compress_file_name
        else:
            return filename_with_root

    def check_filename(self, filename):
        # check if file already uploaded
        try:
            with open(self.log_file, 'r') as f:
                if filename in f.read():
                    return False
                else:
                    return True
        except:
            return True

    def add_filename(self, uploaded_file, log_info, show_result):
        # save uploaded filename in log
        if uploaded_file:
            if show_result:
                header = uploaded_file[0].keys()
                rows = [x.values() for x in uploaded_file]
                table = tabulate.tabulate(rows, header)
                print(table)  # show uploaded files
            else:
                with open(self.log_file, 'a+') as ff:
                    log_info = log_info.values()
                    ff.write(str(log_info))
                    ff.write('\n')
                    ff.close()

    def delete_file_name(self, path, filename):
        # when file broken, delete file name in log
        with open(path, "r+") as f:
            new_f = f.readlines()
            f.seek(0)
            for line in new_f:
                if filename not in line:
                    f.write(line)
            f.truncate()

    def check_history(self):
        # check if there is suspended uploading process
        try:
            with open('stopped_history.txt', 'r') as f:
                info = f.read()
                info = info.split(",,")
                stopped_file, uploaded_bytes = info
                f.close()
                os.remove('stopped_history.txt')
                if self.check_filename(stopped_file):
                    # if not finish uploading
                    check_result = True
                    print("continue to upload '" + stopped_file +
                          "' from " + uploaded_bytes)
                    return check_result, stopped_file, int(uploaded_bytes)
                else:
                    return False, None, 0
        except FileNotFoundError:
            return False, None, 0

    def save_suspend_info(self, filename, bytes_out):
        # save suspended uploading information in log
        info = filename + ",," + str(bytes_out)
        with open('stopped_history.txt', 'w+') as ff:
            ff.write(info)
            ff.write('\n')
            ff.close()

    def on_connect(self, client, userdata, flags, rc):
        self.connection_status = rc
        if rc == 0:
            print("connected to broker...", self.broker)
        else:
            print("Bad connection Returned code=", rc)

    def on_publish(self, client, userdata, mid):
        client.mid_value = mid
        client.puback_flag = True

    def on_message(self, client, userdata, message):
        if message.topic == "data/files/broken_file":
            self.broken_file.append((message.payload).decode("utf-8"))

    def wait_for(self, client, msgType, running_loop=False):
        # waiting for feedback of packet transfer, qos1
        client.running_loop = running_loop  # if using external loop
        wcount = 0
        # return True
        while True:
            # print("waiting"+ msgType)
            if msgType == "PUBACK":
                if client.on_publish:
                    if client.puback_flag:
                        return True

            if not client.running_loop:
                client.loop(self.waiting_period)  # check for messages manually
            time.sleep(self.waiting_period)
            # print("loop flag ",client.running_loop)
            wcount += 1
            if wcount > 40:
                print("return from wait loop taken too long")
                return False

    def send_header(self, client, filename):
        # send header with filename
        if self.Compress_level > 0:
            filename = filename + '.gz'
        file_data = {"filename": filename}
        file_data_json = json.dumps(file_data)
        header = "header" + ",," + file_data_json + ",,"
        header = bytearray(header, "utf-8")
        header.extend(b'x' * (200 - len(header)))
        self.c_publish(client, self.topic, header, self.qos)

    def send_end(self, client, filename, bytes_out):
        # send end with file size
        end = "end" + ",," + filename + ",," + ",," + str(bytes_out) + ",,"
        end = bytearray(end, "utf-8")
        end.extend(b'x' * (200 - len(end)))
        self.c_publish(client, self.topic, end, self.qos)

    def on_log(self, client, userdata, level, buf):
        print(buf)  # see log info.
        # pass

    def c_publish(self, client, pub_topic, out_message):
        # publish file data
        res, mid = client.publish(pub_topic, out_message, self.qos)  # publish
        if self.qos == 1:
            if res == 0:  # published ok
                if self.wait_for(client, "PUBACK", running_loop=True):
                    client.puback_flag = False  # reset flag
                else:
                    raise ConnectionRefusedError("not got puback")
    
    # main upload code, split packet in header, end, file data.
    # Measure upload performance and benchmarking
    def upload(self):
        # create client and initialize
        client = paho.Client(
            client_id="client-upload_server_2021", clean_session=False)
        client.on_publish = self.on_publish
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.on_log = self.on_log
        client.puback_flag = False  # use flag in publish ack
        client.mid_value = None
        client.enable_logger()

        # if use encrypted transmission
        if self.TLS_service:
            client.tls_set(self.TLS_addr)
            port = 8883
        else:
            port = 1883

        # connect to broker
        while not (self.connection_status == 0):
            try:
                client.connect(self.broker, port, keepalive=300)
                time.sleep(1)
                break
            except:
                time.sleep(3)
                print("failed to connect...")

        # check suspended uploading process
        Stopped_flag, stopped_file, stopped_point = self.check_history()
        time.sleep(4)

        # start loop
        client.loop_start()

        # parsing all files in target folders
        self._running = True
        while self._running:
            # measure latency
            try:
                latency = mqtt_ping.test_latency(
                    self.broker, port, self.TLS_service, self.TLS_addr, self.qos)
            except:
                latency = -0.1
                time.sleep(1)

            bandwidth_list = []
            uploaded_file = []  # list for already uploaded filenames
            try:
                for root, dirs, files in os.walk(self.upload_file_path):
                    for file in files:

                        if Stopped_flag:
                            # continue stopped process
                            file = stopped_file

                        filename_with_root = os.path.join(root, file)
                        filename = file
                        # check if file already uploaded
                        if self.check_filename(filename):
                            # if compress transfer is enabled
                            filename_with_root = self.compress(
                                filename_with_root)

                            fo = open(filename_with_root, "rb")
                            print("publishing " + filename, ", qos: " +
                                  str(self.qos), "compress_level: " + str(self.Compress_level))

                            start = time.perf_counter_ns()
                            bytes_out = 0
                            self.save_suspend_info(
                                filename, bytes_out)  # save cookies

                            # if header not uploaded
                            if stopped_point == 0:
                                self.send_header(client, filename)
                            bytes_out = bytes_out + 200
                            self.save_suspend_info(
                                filename, bytes_out)  # save cookies

                            Run_flag = True
                            count = 0
                            # start transfer one file
                            while Run_flag:
                                chunk = fo.read(self.data_block_size)
                                if chunk:
                                    out_message = chunk
                                    if bytes_out > stopped_point:  # no double upload
                                        # publish file data
                                        self.c_publish(client, self.topic,
                                                       out_message, self.qos)
                                    bytes_out = bytes_out + len(out_message)
                                    # save cookies
                                    self.save_suspend_info(filename, bytes_out)
                                else:
                                    # end of file
                                    bytes_out = bytes_out - 200
                                    self.send_end(client, filename, bytes_out)
                                    # remove cookies
                                    os.remove('stopped_history.txt')
                                    Run_flag = False

                            time_taken = (
                                time.perf_counter_ns() - start) / (10 ** 9)
                            time.sleep(1)

                            bandwidth = bytes_out / time_taken / 1024 / 1024 * 8
                            bandwidth_list.append(bandwidth)

                            dict = {
                                "filename": filename, "file size / Byte": bytes_out, "bandwidth / MBit/s": bandwidth}
                            uploaded_file.append(dict)

                            fo.close()

                            if self.Compress_level > 0:
                                # remove zip file
                                os.remove(filename_with_root)

                            if Stopped_flag:  # reset
                                Stopped_flag, stopped_file, stopped_point = False, None, 0

                            # write upload result
                            self.add_filename(uploaded_file, dict, False)
                # show result
                self.add_filename(uploaded_file, None, True)
                # publish latency and bandwidth
                self.c_publish(client, self.topic_latency, latency, 0)
                if bandwidth_list:
                    ave_bandwidth = sum(bandwidth_list) / len(bandwidth_list)
                    self.c_publish(client, self.topic_bandwidth,
                                   ave_bandwidth, 0)

                # check broken file
                client.subscribe(self.topic_broken_file, 0)
                if self.broken_file:  # when file broken
                    print("file broken, resending...")
                    for i in self.broken_file:
                        self.delete_file_name("uploaded_files.txt", i)
                    self.broken_file = []  # reset

            except PermissionError:
                time.sleep(1)
            except FileNotFoundError:
                pass
            except KeyboardInterrupt:
                pass
            except ConnectionRefusedError:
                self.add_filename(uploaded_file, None, True)  # show result
                python = sys.executable
                os.execl(python, python, *sys.argv)


if __name__ == "__main__":
    args = get_args()
    Server = Upload_Server(args)
    Server.upload()
