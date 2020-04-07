import sys
import pika
import datetime
import time
from enum import IntEnum
from threading import Thread, Lock, Event

class LogLevel(IntEnum):
    Elections = 1
    Failures = 2
    Events = 3
    Methods = 4
    Debug = 5
    Ping = 6
    All = 7

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

    def GetNextMessageForId(self, recipient_id, timeout_seconds = 5):
        LogMethod(LogLevel.Methods, self.idx, "TRabbitClient", "GetNextMessageForId")
        target = self.QueueName(recipient_id)
        self.input.queue_declare(queue=target)
        frame, properties, body = next(self.input.consume(queue=target, inactivity_timeout=timeout_seconds))
        if frame and properties and body:
            self.input.basic_ack(frame.delivery_tag)
            return body.decode()
        return None

class TProcessor:
    def __init__(self, ring_size, idx, leader):
        self.mutex = Lock()
        self.ring_size = ring_size
        self.idx = idx
        self.leader = leader
        self.failed_nodes = set()
        self.nxt = self.GetNextNode(self.idx)
        self.prev = self.GetPrevNode(self.idx)
        self.failure_aware = False

    def Ping(self):
        LogMethod(LogLevel.Ping, self.idx, "TProcessor", "Ping")
        with self.mutex:
            msg = TMsg("PING", self.idx, self.nxt, [])
        return msg

    def RecvNext(self, serialized):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "RecvNext Message: {}".format(serialized))
        if serialized:
            return self.ProcessMessage(serialized)
        else:
            if not self.failure_aware:
                LogMethod(LogLevel.Events, self.idx, "ERROR", "PREVIOUS NODE FAILED")
                self.failure_aware = True
                self.failed_nodes.add(self.prev)
                if self.prev == self.leader:
                    return self.InitiateElection()
                else:
                    return self.NotifyFailure()
        return None

    def ProcessMessage(self, serialized):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessMessage")
        message = TMsg()
        message.FromString(serialized)
        if message.msg_type == "ELECTION":
            return self.ProcessElection(message)
        if message.msg_type == "LEADER":
            return self.UpdateLeader(message)
        if message.msg_type == "PING":
            return self.ProcessPing(message)
        if message.msg_type == "FAILURE":
            return self.ProcessFailure(message)
        raise Exception("[{}] Unknown message type! Serialized message: \"{}\"".format(idx, serialized))

    def GetNextNode(self, cur):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "GetNextNode")
        cand = cur
        while True:
            cand += 1
            if cand >= self.ring_size:
                cand -= self.ring_size
            if not cand in self.failed_nodes:
                return cand

    def GetPrevNode(self, cur):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "GetPrevNode")
        cand = cur
        while True:
            cand -= 1
            if cand < 0:
                cand += self.ring_size
            if not cand in self.failed_nodes:
                return cand

    def InitiateElection(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "InitiateElection")
        LogMethod(LogLevel.Elections, self.idx, "ELECTION", "START")
        return TMsg("ELECTION", self.idx, self.GetNextNode(self.idx), [self.idx])

    def NotifyFailure(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "NotifyFailure")
        LogMethod(LogLevel.Failures, self.idx, "FAILURE", "START")
        return TMsg("FAILURE", self.idx, self.nxt, [self.prev])

    def ProcessFailure(self, message):
        failed = message.numbers[0]
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessFailure")
        LogMethod(LogLevel.Failures, self.idx, "FAILURE", "ACKNOWLEDGED {}".format(failed))
        self.failed_nodes.add(failed)
        if message.sender == self.idx:
            self.failure_aware = False
            LogMethod(LogLevel.Failures, self.idx, "FAILURE", "END {}".format(failed))
            return None
        if self.nxt == failed:
            with self.mutex:
                self.nxt = self.GetNextNode(self.nxt)
        message.receiver = self.nxt
        return message

    def ProcessElection(self, message):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessElection")
        self.failed_nodes.add(self.leader)
        if self.leader == self.nxt and self.leader != self.idx:
            with self.mutex:
                self.nxt = self.GetNextNode(self.nxt)
        if self.idx in message.numbers:
            return self.SelectLeader(message)
        else:
            return self.AddSelfAndForward(message)

    def AddSelfAndForward(self, message):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "AddSelfAndForward")
        message.numbers.append(self.idx)
        message.receiver = self.nxt
        LogMethod(LogLevel.Elections, self.idx, "ELECTION", "ADD SELF AND FORWARD MESSAGE")
        return message

    def SelectLeader(self, message):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "SelectLeader")
        self.leader = max(message.numbers)
        LogMethod(LogLevel.Elections, self.idx, "ELECTION", "NEW LEADER {}".format(self.leader))
        return TMsg("LEADER", self.idx, self.nxt, [self.idx, self.leader])

    def UpdateLeader(self, message):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "UpdateLeader")
        initiator, new_leader = message.numbers
        if new_leader != self.leader:
            self.leader = new_leader
            LogMethod(LogLevel.Elections, self.idx, "ELECTION", "NEW LEADER {}".format(self.leader))
        if initiator != self.idx:
            message.receiver = self.nxt
            return message
        self.failure_aware = False
        LogMethod(LogLevel.Elections, self.idx, "ELECTION", "END")
        return None

    def ProcessPing(self, message):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessPing")
        self.prev = message.sender
        return None

class TReader(Thread):
    def __init__(self, node, should_stop):
        Thread.__init__(self)
        self.client = TRabbitClient(node.idx)
        self.node = node
        self.should_stop = should_stop

    def run(self):
        while not self.should_stop.wait(1):
            serialized = self.client.GetNextMessageForId(self.node.idx)
            response = self.node.RecvNext(serialized)
            if response:
                self.client.SendMessage(response.ToString(), response.receiver)
        LogMethod(LogLevel.Debug, self.node.idx, "TReader", "Stop")

class TPinger(Thread):
    def __init__(self, node, should_stop):
        Thread.__init__(self)
        self.client = TRabbitClient(node.idx)
        self.node = node
        self.should_stop = should_stop

    def run(self):
        while not self.should_stop.wait(1):
            message = self.node.Ping()
            self.client.SendMessage(message.ToString(), message.receiver)
        LogMethod(LogLevel.Debug, self.node.idx, "TPinger", "Stop")

class TNode:
    def __init__(self, ring_size, idx, leader):
        self.idx = idx
        self.node = TProcessor(ring_size = ring_size, idx = idx, leader = leader)

        self.should_stop = Event()

        self.pinger = TPinger(node = self.node, should_stop = self.should_stop)
        self.reader = TReader(node = self.node, should_stop = self.should_stop)

        LogMethod(LogLevel.Events, self.idx, "TNode", "Initialized!")

    def start(self):
        LogMethod(LogLevel.Events, self.idx, "TNode", "Started!")
        self.pinger.start()
        self.reader.start()
        self.alive = True

    def IsAlive(self):
        return self.alive

    def kill(self):
        self.should_stop.set()
        self.pinger.join()
        self.reader.join()
        LogMethod(LogLevel.Events, self.idx, "TNode", "Stop")
        self.alive = False

if __name__ == "__main__":
    if (len(sys.argv) < 4):
        print("Use: python3 node.py log_level ring_size leader")
        exit(0)

    global_log_level = LogLevel(int(sys.argv[1]))
    ring_size = int(sys.argv[2])
    leader = int(sys.argv[3])

    nodes = [TNode(ring_size = ring_size, idx = i, leader = leader) for i in range(ring_size)]

    for node in nodes:
        node.start()

    def Alive(nodes):
        for node in nodes:
            if node.IsAlive():
                return True
        return False

    while Alive(nodes):
        time.sleep(1)
        try:
            with open("victims.txt", 'r') as file:
                victims = [int(x) for x in file.read().strip().split(' ')]
        except:
            continue
        for i in victims:
            node = nodes[i]
            if node.IsAlive():
                node.kill()
            else:
                LogMethod(LogLevel.All, node.idx, "VICTIMS", "Already dead")

    Log(LogLevel.Events, "All nodes are dead, shutting down!")
