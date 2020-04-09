import sys
import pika
import datetime
import time
from enum import IntEnum
from threading import Thread, Lock, Event

class LogLevel(IntEnum):
    Marker = 1
    State = 2
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
    def __init__(self, nodes_count, idx):
        self.nodes_count = nodes_count
        self.idx = idx
        self.time = 0
        self.input_states = None
        self.saved_state = None
        self.seen_inputs = set()

    def PingAll(self):
        LogMethod(LogLevel.Ping, self.idx, "TProcessor", "PingAll")
        return [TMsg("PING", self.idx, receiver, [self.GetNextTime()]) for receiver in range(self.nodes_count)]

    def RecvNext(self, serialized):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "RecvNext Message: {}".format(serialized))
        if serialized:
            return self.ProcessMessage(serialized)
        return None

    def PrintState(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "PrintState")
        if global_log_level <= LogLevel.State:
            LogMethod(LogLevel.State, self.idx, "SAVED STATE", "Logical time: {}".format(self.saved_state))
            for i in range(len(self.input_states)):
                channel = self.input_states[i]
                result = "Channel {} state:".format(i)
                for msg in channel:
                    result += " {}".format(msg.ToString())
                LogMethod(LogLevel.State, self.idx, "SAVED STATE", result)

    def GetNextTime(self):
        self.time += 1
        return self.time

    def ProcessMarker(self, message):
        sender = message.sender
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessMarker".format(sender))
        LogMethod(LogLevel.Marker, self.idx, "MARKER", "Received from {}".format(sender))
        self.seen_inputs.add(sender)
        if self.saved_state is not None:
            self.input_states[sender].append(message)
            self.seen_inputs.add(sender)
            if len(self.seen_inputs) == nodes_count:
                LogMethod(LogLevel.Marker, self.idx, "MARKER", "All done!")
                self.PrintState()
            return None
        else:
            response = self.FirstMarker()
            self.input_states[message.sender].append(message)
            if len(self.seen_inputs) == nodes_count:
                LogMethod(LogLevel.Marker, self.idx, "MARKER", "All done!")
                self.PrintState()
            return response
        return None

    def ProcessMessage(self, serialized):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "ProcessMessage")
        message = TMsg()
        message.FromString(serialized)
        self.time = max(self.time, message.numbers[0]) + 1
        if message.msg_type == "PING":
            return self.ProcessPing(message)
        if message.msg_type == "MARKER":
            return self.ProcessMarker(message)
        raise Exception("[{}] Unknown message type! Serialized message: \"{}\"".format(idx, serialized))

    def ProcessPing(self, message):
        if self.saved_state is not None and message.sender not in self.seen_inputs:
            self.input_states[message.sender].append(message)
        LogMethod(LogLevel.Ping, self.idx, "TProcessor", "ProcessPing")
        return None

    def SendMarkerToAll(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "SendMarkerToAll")
        response = list()
        for receiver in range(self.nodes_count):
            if receiver == self.idx:
                continue
            response.append(TMsg("MARKER", self.idx, receiver, [self.GetNextTime()]))
        return response

    def InitiateMarker(self):
        LogMethod(LogLevel.Marker, self.idx, "MARKER", "Initiate")
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "InitiateMarker")
        return self.FirstMarker()

    def FirstMarker(self):
        LogMethod(LogLevel.Marker, self.idx, "MARKER", "First!")
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "FirstMarker")
        self.SaveState()
        return self.SendMarkerToAll()

    def SaveState(self):
        LogMethod(LogLevel.Methods, self.idx, "TProcessor", "SaveState")
        self.saved_state = self.time
        self.input_states = dict()
        for i in range(self.nodes_count):
            self.input_states[i] = list()
        self.seen_inputs.add(self.idx)

class TReader(Thread):
    def __init__(self, node, should_stop, initiate_snapshot):
        Thread.__init__(self)
        self.client = TRabbitClient(node.idx)
        self.node = node
        self.should_stop = should_stop
        self.initiate_snapshot = initiate_snapshot

    def run(self):
        while not self.should_stop.wait(1):
            if self.initiate_snapshot.wait(1):
                response = self.node.InitiateMarker()
                self.client.Send(response)
                self.initiate_snapshot.clear()
                continue
            time.sleep(1)
            serialized = self.client.GetNextMessageForId(self.node.idx)
            response = self.node.RecvNext(serialized)
            self.client.Send(response)
            self.client.Send(self.node.PingAll())
        LogMethod(LogLevel.Debug, self.node.idx, "TReader", "Stop")

class TNode:
    def __init__(self, nodes_count, idx):
        self.idx = idx
        self.node = TProcessor(nodes_count = nodes_count, idx = idx)
        self.should_stop = Event()
        self.initiate_snapshot = Event()
        self.reader = TReader(node = self.node, should_stop = self.should_stop, initiate_snapshot = self.initiate_snapshot)
        LogMethod(LogLevel.Events, self.idx, "TNode", "Initialized!")

    def start(self):
        LogMethod(LogLevel.Events, self.idx, "TNode", "Started!")
        self.reader.start()
        self.alive = True

    def IsAlive(self):
        return self.alive

    def Kill(self):
        self.should_stop.set()
        self.reader.join()
        LogMethod(LogLevel.Events, self.idx, "TNode", "Stop")
        self.alive = False

    def Initiate(self):
        self.initiate_snapshot.set()
        LogMethod(LogLevel.Events, self.idx, "TNode", "Initiate")

if __name__ == "__main__":
    if (len(sys.argv) < 4):
        print("Use: python3 node.py log_level nodes_count leader")
        exit(0)

    global_log_level = LogLevel(int(sys.argv[1]))
    nodes_count = int(sys.argv[2])
    leader = int(sys.argv[3])

    nodes = [TNode(nodes_count = nodes_count, idx = i) for i in range(nodes_count)]

    for node in nodes:
        node.start()

    def Alive(nodes):
        for node in nodes:
            if node.IsAlive():
                return True
        return False

    def KillVictims(nodes):
        try:
            with open("victims.txt", 'r') as file:
                victims = [int(x) for x in file.read().strip().split(' ')]
        except:
            return

        for i in victims:
            node = nodes[i]
            if node.IsAlive():
                node.Kill()
            else:
                LogMethod(LogLevel.All, node.idx, "VICTIMS", "Already dead")

    cnt = 0
    while Alive(nodes):
        time.sleep(1)
        KillVictims(nodes)
        cnt += 1
        if cnt == 2:
            nodes[leader].Initiate()

    Log(LogLevel.Events, "All nodes are dead, shutting down!")
