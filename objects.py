from typing import Any, TypeVar, Generic

KEYSPACE_MAX = 4096  # Kill me


class Keyspace:
    def __init__(self, start: int, end: int):
        """
        Define a keyspan from a start value to an end value.  Respects the cyclical nature of the key ring.
        :param start: The starting value.
        :param end: The ending value.
        """
        self.start = start
        self.end = end

    def __contains__(self, item):
        assert 0 <= item < KEYSPACE_MAX  # Undefined behavior if the input is unbounded
        if self.start == self.end:
            return True  # When the start and end are equal,the keyspan encompasses all keys and all keys are in range.
        elif self.start < self.end:  # If the start is "less than" the end, we can do a normal range check.
            return self.start < item <= self.end
        else:
            # If the end is "less than" the start, the "end" has cycled back
            # and we need to use some fancy checks to determine ranging.
            return item > self.start or item <= self.end - KEYSPACE_MAX


class Node:
    def __init__(self, address: str, finger_table_size: int):
        self.__finger_table_size = finger_table_size
        self.id = hash(address) % KEYSPACE_MAX
        self.address = address
        self.successor = self
        self.predecessor = None
        self.finger_table = [self] * finger_table_size
        self.hash_table = {}

    def cohere(self):
        self.stabilize()
        self.fix_fingers()

    def join(self, ring):
        self.successor = ring.find_successor(self.id)

    def find_successor(self, id: int):
        if id in Keyspace(self.id, self.successor.id):
            return self.successor
        else:
            node = self.closest_preceding_node(id)
            if node == self:  # Prevent infinite loops before network load
                return node
            return node.find_successor(id)

    def closest_preceding_node(self, id: int):
        """
        Get the closest preceding node in the finger table to the target key.
        :param id: The target key.
        :return: The closest preceding node.
        """
        for node in self.finger_table[::-1]:  # Slice the array so that we iterate backwards over it
            assert node  # Sanity check - we are NEVER permitted a null finger table
            # I'm naming a Keyspace object to check if a value is inside a chunk of the keyspace - this is
            # tricky because of the loopiness of the ring
            if node.id in Keyspace(self.id, id % KEYSPACE_MAX):
                return node
        return self

    def stabilize(self):
        assert self.successor  # Sanity check - if we don't have a successor something is very wrong
        if not self.successor.predecessor:
            self.notify(self)
            return
        x = self.successor.predecessor
        if x.id in Keyspace(self.id, self.successor.id):
            self.successor = x
        x.notify(self)

    def notify(self, parent):
        if not self.predecessor or parent.id in Keyspace(self.predecessor.id, self.id):
            self.predecessor = parent

    def check_predecessor(self):
        return True  # Nonsensical without network!

    def fix_fingers(self):
        # This does not use the incremental formulation in the paper because
        # this is not a real network and it would suck if it were
        for i in range(self.__finger_table_size):
            finger_id = (self.id + 2 ** i) % KEYSPACE_MAX  # For once, infinite-precision integers are BAD
            self.finger_table[i] = self.find_successor(finger_id)

    def find_key_node(self, key: str):
        return self.closest_preceding_node(hash(key))

    def get(self, key: str):
        keyholder = self.find_key_node(key)
        return keyholder.hash_table[key]

    def put(self, key: str, value: Any):
        keyholder = self.find_key_node(key)
        keyholder.hash_table[key] = value

    def print_ring(self, i):
        valid = False
        root_addr = self.address
        print(self.address, self.id)
        if root_addr == str(i):
            valid = True
        active_node = self.successor
        while active_node and active_node.address != root_addr:
            if active_node.address == str(i):
                valid = True
            active_node.cohere()
            print(active_node.address, active_node.id)
            active_node = active_node.successor
        return valid

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.put(key, value)

    def __str__(self):
        return self.address
