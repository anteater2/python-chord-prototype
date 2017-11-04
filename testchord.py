import asyncio

from objects import Node
from time import sleep

# None of what's in this file is really readable at this point - it should just show ten nodes being added, and the debugger should let you
# inspect their successors/predecessors

FINGER_TABLE_SIZE = 3

root = Node("ROOT", FINGER_TABLE_SIZE)
root.cohere()
node_table = [root]
for i in range(10):
    node = Node("{i}".format(i=i), FINGER_TABLE_SIZE)
    node_table.append(node)
    node.join(root)
    print("Added node {i}".format(i=i))
    for r in range(100):
        for x in node_table[::-1]:
            x.cohere()
        for x in node_table[::-1]:
            x.cohere()
    if i == 3:
        print("DEBUG WATCHDOG3")
    assert root.print_ring(i)

    from random import randint

    rvs = []
    for i in range(10000):
        v = randint(-255, 255)
        rvs.append(v)
        root[i] = v

    for i in range(10000):
        assert root[i] == rvs[i]