import socket
import sys
import time
import random
import threading


class IOTNode:
    def __init__(self):
        self.packet_seq = 0  # get from reading input args
        self.UDP_ip = '127.0.0.1'

        self.number_of_fog_nodes = 0
        self.interval = int(sys.argv[2])/1000
        self.MY_UDP = int(sys.argv[3])  # listen_port

        self.list_of_node_details = []  # [[ip1,port1],[ip2,port2]]
        self.message = ""

        self.UDP_IP = None
        self.UDP_PORT = None

        self.total_limit = 30

    def iot_node(self):
        print("Communicating to Fog_node")
        self.read_from_command_line()

        send = threading.Thread(target=self.send_req_to_fog)
        receive = threading.Thread(target=self.receive_from_server_node)

        send.start()
        receive.start()

        send.join()
        receive.join()

    def read_from_command_line(self):

        arguments = len(sys.argv) - 1
        pos = 4

        a = []
        while arguments >= pos:
            if len(a) < 2:
                a.append(sys.argv[pos])

            if len(a) == 2:
                a[1] = int(a[1])
                self.list_of_node_details.append(tuple(a[:]))
                a = []
            pos += 1

        self.number_of_fog_nodes = len(self.list_of_node_details)

        print ("The number of fog nodes are: " + str(self.number_of_fog_nodes))
        print ("The fog nodes are : " + str(self.list_of_node_details))

        return self.list_of_node_details

    def random_number(self):
        return int(random.uniform(0, len(self.list_of_node_details)))


    def request_generation(self):
        # message in a list
        self.message = str(self.packet_seq) + " " + str(random.randint(2, 6)) + " " + str(random.randint(3, 8)) + " " + self.UDP_ip + " " + str(self.MY_UDP) + " "

        print (self.message)
        return self.message

    def send_req_to_fog(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while self.packet_seq < self.total_limit:
            #print("Hello entering senf=d fun")

            try:
                print("Enter the message to send")
                message_to_send = self.request_generation()
                print (" The message to send is : {}".format(message_to_send))
                sock.sendto(message_to_send.encode(), (random.choice(self.list_of_node_details)))
                self.packet_seq += 1
                time.sleep(self.interval)

            except:
                continue


    def receive_from_server_node(self):

        recs = 0
        resp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        resp_sock.bind(("", self.MY_UDP))

        while True:
            print("Entering Recv Fun")
            time.sleep(1)
            if recs == self.total_limit:
                resp_sock.close()
                break

            data, addr = resp_sock.recvfrom(2048)
            rec_msg = data.decode()
            recs += 1
            print (" The response message is : " + rec_msg)


new = IOTNode()
new.iot_node()




