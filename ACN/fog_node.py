from socket import *
from time import *
import threading
from collections import deque
import sys
import random


class fognode:
    def __init__(self, max_res, t, my_tcp, my_udp, c, tcp_cloud):
        self.max_res_time = float(max_res)
        self.present_queueing_delay = 0  # present fog node info qtime
        self.fog_queue = deque([])  # queue for requests to process
        # self.fog_update_queue = deque([])
        self.fognode_qtimeinfo = {}  # to save other fog node qtime info
        self.t = float(t)
        self.my_tcp_port = int(my_tcp)
        self.my_udp_port = int(my_udp)
        self.my_ip = '127.0.0.1'  # my node ip
        self.cloud_ip = str(c)
        self.cloud_port = int(tcp_cloud)
        self.arguments = []  # just to read the incoming arguments
        self.neighbours = {}  # ip to port map
        self.node_socket_info = {}# node instances saved with ket as ip,port tuple and value as instance
        self.total_pakcount = 0
        self.local_pakcount = 0
        self.connected_soc = []
        self.start = time()
        self.lock = threading.Lock()
        self.s_cloudsend = socket(AF_INET, SOCK_STREAM)
        #self.s_fogsend = socket(AF_INET, SOCK_STREAM)
        self.s_iotreceive = socket(AF_INET, SOCK_DGRAM)
        self.s_iotsend = socket(AF_INET, SOCK_DGRAM)

    def connection(self):
        # while(self.total_pakcount <= 50):
        self.s_cloudsend.connect((self.cloud_ip, self.cloud_port))
        print ("Connected to cloud ")
        self.s_foglisten = socket(AF_INET, SOCK_STREAM)
        self.s_foglisten.bind(("", self.my_tcp_port))

        temp = random.randint(1,6)

        while (True):

            if ((len(self.node_socket_info) != len(self.neighbours))):
                try:
                    self.s_foglisten.settimeout(temp)
                    self.s_foglisten.listen(5)
                    (nodesocket, addr) = self.s_foglisten.accept()
                    self.node_socket_info[addr] = nodesocket

                except:
                    pass

            if ((len(self.node_socket_info) == len(self.neighbours))):
                break

            for i,j in self.neighbours:
                s_fogsend = socket(AF_INET, SOCK_STREAM)
                try:
                    #print("Sending Conn Request")
                    s_fogsend.settimeout(1)
                    s_fogsend.connect((i, j))
                    self.node_socket_info[(i,j)] = s_fogsend

                except:
                    s_fogsend.close()
                    continue
        return

    def operations(self):
        print(self.node_socket_info)
        # per_update_recv_thread = threading.Thread(target=self.periodic_update_receive)
        per_update_send_thread = threading.Thread(target=self.periodic_update_send)

        fogmsg_receive_thread = threading.Thread(target=self.fog_msg_receive)

        fogreceive_iot_thread = threading.Thread(target=self.receive_from_iot)
        fogsend_iot_thread = threading.Thread(target=self.send_to_iot)

        # per_update_recv_thread.start()
        per_update_send_thread.start()

        fogmsg_receive_thread.start()

        fogreceive_iot_thread.start()
        fogsend_iot_thread.start()
        ##self.fog_msg_send()

        # per_update_recv_thread.join()
        per_update_send_thread.join()

        fogmsg_receive_thread.join()

        fogreceive_iot_thread.join()
        fogsend_iot_thread.join()

    def receive_from_iot(self):
        self.s_iotreceive.bind(("", self.my_udp_port))
        #print(" ENtered ")
        while (True):
            try:
                self.s_iotreceive.settimeout(5)
                data, addr = self.s_iotreceive.recvfrom(1024)
                print(" The data received from IOT is :" + str(data))

                data = data.decode()
                if (data):
                    data = data.split(' ')
                    #print(" SPlit data :"+str(data))

                    if (int(data[1]) > 0):
                        expect_response_time = float(data[2]) + self.present_queueing_delay
                        print(expect_response_time)

                        if (expect_response_time <= self.max_res_time):
                            self.lock.acquire()
                            self.fog_queue.append(data)
                            #print("fog queue : " + str(self.fog_queue) + str(self.my_tcp_port))
                            self.lock.release()
                            self.present_queueing_delay = expect_response_time

                            if (int(data[0]) > self.local_pakcount):
                                self.local_pakcount = int(data[0])
                            if (self.local_pakcount > self.total_pakcount):
                                self.total_pakcount = self.local_pakcount


                        # else:
                        #     print(self.present_queueing_delay, data[2])
                        #     temp = 10**6
                        #     best_node = 0
                        #     for nodes in self.fognode_qtimeinfo:
                        #         if(self.fognode_qtimeinfo[nodes]<temp):
                        #             temp = self.fognode_qtimeinfo[nodes]
                        #             best_node = nodes
                        #
                        #     self.fog_msg_send(best_node, data)

                        else:
                            msg_string = ''
                            for i in range(len(data)):
                                msg_string += data[i] + ' '
                            msg_string += str(self.my_ip) + " " + + str(self.my_udp_port)+ 'Sent to cloud node for processing'
                            print (" msg string : " + str(msg_string))
                            self.s_cloudsend.send(msg_string.encode())
                else:
                    sleep(2)
            except:
                continue
        return

    def send_to_iot(self):
        while (time()-self.start < 70):
            if (not self.fog_queue):
                continue
            else:
                data = self.fog_queue.popleft()
                sleep(float(data[2]))
                msg_string = ''

                for i in range(len(data)):
                    msg_string += data[i] + ' '

                msg_string += str(self.my_ip) + " " + "Request processed by fog node" + " " + str(self.my_udp_port)
                self.present_queueing_delay -= float(data[2])

                self.s_iotsend.sendto(msg_string.encode(), (data[3], int(data[4])))  # for udp to send to an ip and port
        return

    def fog_msg_send(self, ip, data_list):
        msg_string = ''
        for i in range(len(data_list)):
            if (i == 1):
                msg_string + str(int(data_list[i]) - 1) + ' '
            else:
                msg_string + data_list[i] + ' '

        msg_string += str(self.my_ip) + ' ' + str(self.my_udp_port)
        print("Sending information to best neighbor node ", ip)
        self.node_socket_info[ip].send(msg_string.encode())

    def fog_msg_receive(self):

        while (time() - self.start < 70):
            for i, j in self.node_socket_info:
                node = self.node_socket_info[(i,j)]
                #print (" Fog msg receive - node : " +str(node))
                sleep(1)
                try:
                    node.settimeout(0.5)

                    data1 = node.recv(1024)
                    if not data:
                        continue

                    data1 = data1.decode().split(':')

                    data_update = []
                    data_msg = []
                    for k in data1:
                        if(len(k) <= 15):
                            if(k==''):
                                continue
                            data_update.append(k)
                        else:
                            data_msg.append(k)

                    if (len(data_update) > 0):
                        self.periodic_update_receive(data_update, i,j)

                    if(len(data_msg) > 0):
                        for x in data_msg:
                            data = x.split(' ')
                            if (int(data[1]) > 0):
                                expect_response_time = float(data[2]) + self.present_queueing_delay

                                if (expect_response_time <= self.max_res_time):
                                    self.lock.acquire()
                                    self.fog_queue.append(data)
                                    self.lock.release()
                                    self.present_queueing_delay = expect_response_time

                                    if (int(data[0]) > self.local_pakcount):
                                        self.local_pakcount = int(data[0])
                                    if (self.local_pakcount > self.total_pakcount):
                                        self.total_pakcount = self.local_pakcount


                                else:
                                    temp = 10 ** 6
                                    best_node = 0
                                    for nodes in self.fognode_qtimeinfo:
                                        #(nodes[0] not in data)
                                        if((self.fognode_qtimeinfo[nodes] < temp) and (nodes[1] not in data)):
                                            temp = self.fognode_qtimeinfo[nodes]
                                            best_node = nodes

                                    self.fog_msg_send(best_node, data)

                            else:
                                msg_string = ''
                                for t in range(len(data)):
                                    msg_string += data[t] + ' '
                                msg_string += str(self.my_ip) + ' ' + str(self.my_udp_port)
                                self.s_cloudsend.send(msg_string.encode())
                    else:
                        continue

                except:
                    continue

    def periodic_update_send(self):
        temp1 = self.start
        while (time()-self.start < 70):
            temp2 = time()
            if (temp2 - temp1 >= self.t):
                temp1 = temp2
                for i,j in self.node_socket_info:
                    msg = ':' + ','.join([str(self.present_queueing_delay), str(self.local_pakcount)]) + ':'
                    msg = msg.encode()
                    self.node_socket_info[(i,j)].sendall(msg)
        return

    def periodic_update_receive(self, data, addr, port):

        for i in data:
            data1 = i.split(',')
            self.fognode_qtimeinfo[(addr, port)] = float(data1[0])
            print(self.fognode_qtimeinfo)

            if (int(data1[1]) > self.total_pakcount):
                self.total_pakcount = int(data1[1])

            if (self.local_pakcount > self.total_pakcount):
                self.total_pakcount = self.local_pakcount
        return


if __name__ == "__main__":
    Max_Response_Time = sys.argv[1]
    t = sys.argv[2]
    MY_TCP = sys.argv[3]
    MY_UDP = sys.argv[4]
    C = sys.argv[5]
    TCP0 = sys.argv[6]
    con = sys.argv[7:]
    fog = fognode(Max_Response_Time, t, MY_TCP, MY_UDP, C, TCP0)    #con
    for x in con:
        fog.arguments.append(x)

    for x in range(0, len(fog.arguments), 2):
        fog.neighbours[(str(fog.arguments[x]), int(fog.arguments[x + 1]))] = int(fog.arguments[x + 1])

    fog.connection()
    fog.operations()

