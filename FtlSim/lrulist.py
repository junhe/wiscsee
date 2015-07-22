import collections

class Node(object):
    def __init__(self, key = None, value = None, empty = False):
        self.empty = empty
        if not empty:
            self.key = key
            self.value = value
        # next
        # prev
    def visual(self):
        return "key:{key}\t value:{value}\t prev:{prev}\t next:{next}\t "\
            "empty:{empty}".format(
            key = self.__dict__.get('key', None),
            value = self.__dict__.get('value', None),
            prev = self.__dict__.get('prev', None),
            next = self.__dict__.get('next', None),
            empty = self.__dict__.get('empty', None))
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

        # special case, node is the head
        if node is self.head:
            self.head = node.next

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
        tablestr = 'Table:' + repr(self.table.keys())
        listview = []
        node = self.head
        # while not node.next is self.head:
        # while True:
        while not node.empty:
            listview.append(node.visual())
            if node.empty:
                break
            node = node.next
        listview = '->\n'.join(listview)

        return tablestr+'\n'+listview

def main():
    lru = LruList()
    lru[3] = 33
    print lru
    a = lru[3]
    print lru

    print 'len:', len(lru)
    lru[3] = 33
    print lru
    print 'GGGGGet lru[3]', lru[3]
    print lru
    lru[3] = 44
    lru[8] = 88
    print 'len:', len(lru)
    print lru
    lru[4] = 55
    print 'len:', len(lru)
    print lru
    lru[5] = 555
    print 'lru[4]', lru[4]
    print 'len:', len(lru)
    print lru
    # del lru[4]
    print 'len:', len(lru)
    print lru
    print 'default', lru.get(5)
    print 'default', lru.get(88)

if __name__ == '__main__':
    main()


