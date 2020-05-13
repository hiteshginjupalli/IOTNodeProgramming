import socket
import sys
import time
import threading
from threading import Lock


class CloudNode:
    def __init__(self):
        self.cloud_queue = []
        self.TCP_IP = '127.0.0.1'
        self.my_tcp_port = int(sys.argv[1])
        self.node_time = time.time()
        self.max_time = 250
        self.cloud_node_queue = []

    def cloud_threads(self):
        cloud_fog_Conn_thread = threading.Thread(target=self.connect_to_fog)
        cloud_iot_send_thread = threading.Thread(target=self.send_to_iot_node)
        cloud_fog_recv_thread = threading.Thread(target=self.receive_from_fog)

        cloud_fog_Conn_thread.start()
        cloud_iot_send_thread.start()
        cloud_fog_recv_thread.start()

        cloud_fog_Conn_thread.join()
        cloud_iot_send_thread.join()
        cloud_fog_recv_thread.join()

    def connect_to_fog(self):

        sock_cloud = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("Socket created")
        sock_cloud.bind(('', self.my_tcp_port))

        while True:
            if (time.time() - self.node_time) > self.max_time:
                break
            try:
                #sock_cloud.settimeout()
                sock_cloud.listen(5)
                #print("cloud socket listening for incoming messages ")
                data, addr = sock_cloud.accept()
                print(data)
                print("Connection requests accepted by cloud node")
                self.cloud_queue.append(data)
                time.sleep(1)

            except:
                continue

    def receive_from_fog(self):
        while True:
            if (time.time() - self.node_time) > self.max_time:
                break

            time.sleep(1)
            for nodes in self.cloud_queue:
                try:
                    nodes.settimeout(1)
                    message = nodes.recv(1024)
                    Lock.acquire()
                    print (" Message received from fog is : {}".format(message.decode()))
                    self.cloud_node_queue.append(message)
                    Lock.release()
                    time.sleep(1)
                except:
                    time.sleep(1)
                    continue

    # Append a message "processed by cloud node" to the original message and send to IOT node
    def send_to_iot_node(self):

        while True:
            if (time.time()-self.node_time) > self.max_time:
                print("End of thread")
                break
            time.sleep(1)

        # which Iot node generated this request --> send to that IOT node --> index in message
            if not self.cloud_queue or not self.cloud_node_queue:
                time.sleep(1)
                continue

            message = self.cloud_node_queue.pop(0)
            curr = message.split(' ')
            iot_port = int(curr[4])
            iot_ip = curr[3]
            seq_num = curr[0]
            time.sleep(curr[2])

            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            message += 'Processed by cloud' + " " + str(seq_num)

            print("Sending response to IOT node")
            s.sendto(message.encode(), (iot_ip, iot_port))


run = CloudNode()
run.cloud_threads()




