import collections

class Node(object):
    def __init__(self, key = None, value = None, empty = False):
        self.empty = empty
        if not empty:
            self.key = key
            self.value = value
        # next
        # prev


class LruList(collections.MutableMapping):
    """
    It should act like a dict.
    It should be able to provide the newest and oldest entry.
    No duplicated keys are allowed.
    """
    def __init__(self):
        # for quick retrieving
        # initially empty
        # self.table has key X if and only if the linked list has a node
        # with node.key = X
        # self.table[x] is of Node class
        self.table = {}

        # head is newer than tail
        self.head = Node(empty = True)
        self.head.next = self.head
        self.head.prev = self.head

    def __getitem__(self, key):
        # move to head
        node = self.table[key]
        self.move_to_head(node)

        return node.value

    def get(self, key, default = None):
        if self.table.has_key(key):
            # will affect list order
            return self.__getitem__(key)
        else:
            # will not affect list order
            return default

    def __setitem__(self, key, value):
        if self.table.has_key(key):
            # update
            self.table[key].value = value
        else:
            # create new
            node = Node(key = key, value = value)
            self.put_to_head(node)
            self.table[key] = node

    def move_to_head(self, node):
        "move an existing node to the head of list"
        self._delete_node_from_linked_list(node)
        self.put_to_head(node)

    def put_to_head(self, node):
        "put a node to the head of the list, node should not exist in list"
        old_head = self.head
        old_prev = self.head.prev

        # modify node's pointers
        self.head = node
        node.next = old_head
        node.prev = old_head.prev

        old_head.prev = node

        old_prev.next = node

    def has_key(self, key):
        return self.table.has_key(key)

    def keys(self):
        return self.table.keys()

    def _delete_node_from_linked_list(self, node):
        "this will not delete from self.table"
        prev_node = node.prev
        next_node = node.next

        prev_node.next = node.next
        next_node.prev = node.prev

    def __delitem__(self, key):
        node = self.table[key]
        del self.table[key]

        self._delete_node_from_linked_list(node)

    def __iter__(self):
        return self.table.keys()

    def __len__(self):
        return len(self.table)

    def __repr__(self):
        tablestr = repr(self.table.keys())
        listview = []
        node = self.head
        while not node.empty:
            listview.append( str(node.key)+':'+str(node.value) )
            node = node.next
        listview = '->'.join(listview)

        return tablestr+'\n'+listview

def main():
    lru = LruList()
    lru[3] = 33
    print 'len:', len(lru)
    print lru
    lru[3] = 44
    print 'len:', len(lru)
    print lru
    lru[4] = 55
    print 'len:', len(lru)
    print lru
    lru[5] = 555
    print 'len:', len(lru)
    print lru
    del lru[4]
    print 'len:', len(lru)
    print lru
    print 'default', lru.get(5)
    print 'default', lru.get(88)

    lru.has_key(3)
if __name__ == '__main__':
    main()


