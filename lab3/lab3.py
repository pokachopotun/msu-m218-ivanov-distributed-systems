import sys
import pika
import datetime
import time
from enum import IntEnum
from threading import Thread, Lock, Event
from random import randint
import os

class LogLevel(IntEnum):
    Check = 1
    Events = 2
    Methods = 3
    Debug = 4
    All = 5

global_log_level = LogLevel.All

def Log(level, message):
    if level <= global_log_level:
        print(message)

def LogWithIndex(level, idx, message):
    Log(level, "{} [id = {}] {}".format(datetime.datetime.now(), idx, message))

def LogMethod(level, idx, typename, method):
    LogWithIndex(level, idx, "[{}] {}".format(typename, method))

class TMsg:
    def __init__(self, msg_type = "NONE", sender = -1, receiver = -1, numbers = []):
        self.msg_type = msg_type
        self.sender = sender
        self.receiver = receiver
        self.numbers = numbers

    def ToString(self):
        serialized = "{} {} {}".format(self.msg_type, self.sender, self.receiver)
        for x in self.numbers:
            serialized += " {}".format(x)
        return serialized

    def FromString(self, serialized):
        data = serialized.split(' ')
        self.msg_type = data[0]
        self.sender = int(data[1])
        self.receiver = int(data[2])
        if len(data) >= 4:
            self.numbers = [int(x) for x in data[3:]]
        else:
            self.numbers = []


class TRabbitClient:
    def __init__(self, idx):
        self.idx = idx
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.input = self.connection.channel()
        self.output = self.connection.channel()

    def QueueName(self, to_id):
        return "queue_to_{}".format(to_id)

    def SendMessage(self, message, to_id):
        LogMethod(LogLevel.Methods, self.idx, "TRabbitClient", "SendMessage")
        target = self.QueueName(to_id)
        self.output.queue_declare(queue=target)
        self.output.basic_publish(exchange='', routing_key=target, body=message)

    def GetNextMessageForId(self, recipient_id, timeout_seconds = 1):
        LogMethod(LogLevel.Methods, self.idx, "TRabbitClient", "GetNextMessageForId")
        target = self.QueueName(recipient_id)
        self.input.queue_declare(queue=target)
        frame, properties, body = next(self.input.consume(queue=target, inactivity_timeout=timeout_seconds))
        if frame and properties and body:
            self.input.basic_ack(frame.delivery_tag)
            return body.decode()
        return None

    def Send(self, response):
        if response:
            for message in response:
                self.SendMessage(message.ToString(), message.receiver)

class TProcessor:
    def __init__(self, nodes_count, idx, resources_count):
        self.nodes_count = nodes_count
        self.resources_count = resources_count
        self.idx = idx
        self.need_max = None
        self.allocated = None
        self.total_resources = None
        self.workers = set()
        self.safe_order = None

    def ReadResourceConfig(self):
        with open("total_resources.txt", 'r') as file:
            self.total_resources = [int(x) for x in file.read().strip().split()]

    def InitAsLeader(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "InitAsLeader")
        self.workers.add(self.idx)
        self.ReadResourceConfig()

        self.TryReadAllocatedConfig()

        # leader zeroes config
        self.need_max = list()
        for node in range(self.nodes_count):
            self.need_max.append([0 for i in range(self.resources_count)])

    def InitiateCheck(self):
        LogMethod(LogLevel.Check, self.idx, "CHECK", "INITIATE!")
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "InitiateCheck")
        self.InitAsLeader()
        response = list()
        for receiver in range(self.nodes_count):
            if self.idx == receiver:
                continue
            response.append(TMsg("INITIATE", self.idx, receiver, []))
        return response

    def RecvNext(self, serialized):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "RecvNext Message: {}".format(serialized))
        if serialized:
            return self.ProcessMessage(serialized)
        return None

    def ProcessMessage(self, serialized):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessMessage")
        message = TMsg()
        message.FromString(serialized)
        if message.msg_type == "KILL":
            return self.ProcessKill(message)
        if message.msg_type == "DECLARE":
            return self.ProcessDeclare(message)
        if message.msg_type == "INITIATE":
            return self.ProcessInitiate(message)
        raise Exception("[{}] Unknown message type! Serialized message: \"{}\"".format(idx, serialized))

    def ProcessKill(self, message):
        LogMethod(LogLevel.Check, self.idx, "CHECK", "GOT KILLED BY COORDINATOR!")
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessKill")
        return "KMP"

    def ProcessInitiate(self, message):
        LogMethod(LogLevel.Check, self.idx, "CHECK", "DECLARE!")
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessInitiate")
        return self.DeclareMax(message.sender);

    def ReadMaxConfig(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ReadMaxConfig")
        self.need_max = list()
        with open("need_max.txt", 'r') as file:
            for line in file:
                self.need_max.append([int(x) for x in line.strip().split()])

    def CheckIfLeader(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "CheckIfLeader")
        return self.need_max[self.idx][0] == -1

    def DeclareMax(self, receiver):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "DeclareMax")
        return [TMsg("DECLARE", self.idx, receiver, self.need_max[self.idx]), ]

    def TryReadAllocatedConfig(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "TryReadAllocatedConfig")
        filename = "allocated.txt"
        if not os.path.exists(filename):
            return False
        self.allocated = list()
        with open(filename, 'r') as file:
            for line in file:
                self.allocated.append([int(x) for x in line.strip().split()])
        #for node in range(self.nodes_count):
        #    for resource in range(self.resources_count):
        #        self.total_resources[resource] -= self.allocated[node][resource]
        return True

    def InitAllocatedWithZeroes(self):
        self.allocated = list()
        for node in range(self.nodes_count):
            self.allocated.append([0 for i in range(self.resources_count)])

    def GenerateAllocated(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "GenerateAllocated")
        self.InitAllocatedWithZeroes()
        for resource in range(self.resources_count):
            available = self.total_resources[resource]
            for node in range(self.nodes_count):
                val = randint(0, min(available, self.need_max[node][resource]))
                available -= val
                self.allocated[node][resource] = val
            self.total_resources[resource] = available

        # leader does not allocate resources for itself
        for resource in range(self.resources_count):
            self.total_resources[resource] += self.allocated[self.idx][i]
            self.allocated[self.idx][i] = 0

    def KillAll(self):
        return [TMsg("KILL", self.idx, receiver, []) for receiver in range(self.nodes_count)]

    def ProcessDeclare(self, message):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessDeclare")
        sender = message.sender
        self.need_max[sender] = message.numbers
        self.workers.add(sender)
        if len(self.workers) == self.nodes_count:
            if self.allocated is None:
                self.GenerateAllocated()
            order = self.GetSafeOrder()
            if order is None:
                LogMethod(LogLevel.Check, self.idx, "CHECK", "STATE IS NOT SAFE!")
            else:
                LogMethod(LogLevel.Check, self.idx, "CHECK", "SAFE ORDER IS: {}".format(str(order)))
            return self.KillAll()
        return None

    def Sum(self, left, right):
        return [l + r for l, r in zip(left, right)]

    def LessEq(self, left, right):
        for l, r in zip(left, right):
            if l > r:
                return False
        return True

    def GetSafeOrder(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "GetSafeOrder")

        order = list()
        used = set()
        used.add(self.idx)
        while len(used) < self.nodes_count:
            cnt = 0
            for node in range(self.nodes_count):
                if node in used:
                    continue
                have_total = self.Sum(self.allocated[node], self.total_resources)
                if self.LessEq(self.need_max[node], have_total):
                    used.add(node)
                    order.append(node)
                    self.total_resources = have_total
                    cnt += 1
            if cnt == 0:
                return None
        return order


class TReader(Thread):
    def __init__(self, node, should_stop):
        Thread.__init__(self)
        self.client = TRabbitClient(node.idx)
        self.node = node
        self.should_stop = should_stop
        self.dead = False

    def run(self):
        self.node.ReadMaxConfig()
        if self.node.CheckIfLeader():
            LogMethod(LogLevel.Check, self.node.idx, "CHECK", "LEADER IS {}".format(self.node.idx))
            response = self.node.InitiateCheck()
            self.client.Send(response)
        while not self.should_stop.wait(1):
            serialized = self.client.GetNextMessageForId(self.node.idx)
            response = self.node.RecvNext(serialized)
            if response == "KMP":
                self.dead = True
                return
            self.client.Send(response)
        LogMethod(LogLevel.Debug, self.node.idx, "TReader", "Stop")

class TNode:
    def __init__(self, nodes_count, idx, resources_count):
        self.idx = idx
        self.node = TProcessor(nodes_count = nodes_count, idx = idx, resources_count = resources_count)
        self.should_stop = Event()
        self.reader = TReader(node = self.node, should_stop = self.should_stop)
        LogMethod(LogLevel.Events, self.idx, "TNode", "Initialized!")

    def start(self):
        LogMethod(LogLevel.Events, self.idx, "TNode", "Started!")
        self.reader.start()
        self.alive = True

    def IsAlive(self):
        return self.alive and not self.reader.dead

    def Kill(self):
        self.should_stop.set()
        self.reader.join()
        LogMethod(LogLevel.Events, self.idx, "TNode", "Stop")
        self.alive = False

if __name__ == "__main__":
    if (len(sys.argv) < 4):
        print("Use: python3 node.py log_level nodes_count resources_count")
        exit(0)

    global_log_level = LogLevel(int(sys.argv[1]))
    nodes_count = int(sys.argv[2])
    resources_count = int(sys.argv[3])

    nodes = [TNode(nodes_count = nodes_count, idx = i, resources_count = resources_count) for i in range(nodes_count)]

    for node in nodes:
        node.start()

    def Alive(nodes):
        for node in nodes:
            if node.IsAlive():
                return True
        return False

    while Alive(nodes):
        time.sleep(1)

    Log(LogLevel.Events, "All nodes are dead, shutting down!")
