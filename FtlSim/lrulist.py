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


"""
Segmented LRU (SLRU)

The motivation of SLRU is to avoid lines that is only used once to flush
out lines that would be used with high probability.

It consists of two segments:
- probationary segment: misses go here
    - line are discard from the LRU end here
- protected segment: hits move here
    - finite
    - line may be evicted to probationary segment

Both ordered.

Parameter:
- size of protected segment

"""

class LinkedList(object):
    """
    Requirement for the node:
        Any vairiable that can set 'prev', and 'next' attributes can be used.
    """

    class _EmptyNode(object):
        pass

    def __init__(self):
        # head is newer than tail
        self._end_guard = self._EmptyNode()
        self._head = self._end_guard
        self._head.next = self._head
        self._head.prev = self._head

        self.size = 0

    def add_before(self, new_node, node):
        """
        add new_node before node
        current: ->node1->node->
        later  : ->node1->new_node->node

        Note: this does not handle list head
        """
        node1 = node.prev

        # modify new_node's pointers
        new_node.next = node
        new_node.prev = node1

        # node1
        node1.next = new_node
        # node
        node.prev = new_node

        self.size += 1

    def add_to_head(self, node):
        old_head = self._head
        self._head = node

        self.add_before(node, old_head)

    def add_to_tail(self, node):
        self.add_before(node, self._end_guard)

    def move_to_head(self, node):
        assert self.size > 0
        self.delete(node)
        self.add_to_head(node)

    def delete(self, node):
        assert self.size > 0

        # special case, node is the head
        if node is self._head:
            self._head = node.next

        prev_node = node.prev
        next_node = node.next

        prev_node.next = node.next
        next_node.prev = node.prev

        self.size -= 1

    def head(self):
        if self.size == 0:
            return None
        else:
            node = self._head
            return node

    def tail(self):
        if self.size == 0:
            return None
        else:
            node = self._end_guard.prev
            return node

    def __iter__(self):
        node = self._head
        while node is not self._end_guard:
            yield node
            node = node.next

    def __len__(self):
        return self.size

    def __repr__(self):
        listview = []
        node = self._head
        while node is not self._end_guard:
            listview.append(str(node))
            node = node.next
        listview = '->\n'.join(listview)

        return 'LinkedList:\n' + listview

class KeyValueStoreInLinkedList(collections.MutableMapping):
    def __init__(self):
        self.table = {}
        # node must be type 'class Node'
        self.linked_list = LinkedList()

    def __getitem__(self, key):
        node = self.table[key]
        return node.value

    def has_key(self, key):
        return self.table.has_key(key)

    def keys(self):
        return self.table.keys()

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
            self.linked_list.add_to_head(node)
            self.table[key] = node

    def __delitem__(self, key):
        node = self.table[key]
        del self.table[key]

        self.linked_list.delete(node)

    def __iter__(self):
        return self.table.keys()

    def __len__(self):
        return len(self.linked_list)

    def __repr__(self):
        listview = []
        node = self.linked_list.head()
        while node != self.linked_list._end_guard:
            listview.append(node.visual())
            if node.empty:
                break
            node = node.next
        listview = '->\n'.join(listview)

        return listview


class LruCache(KeyValueStoreInLinkedList):
    """
    geeting and setting a value will move it to the head of the list
    """
    def __getitem__(self, key):
        node = self.table[key]
        self.linked_list.move_to_head(node)
        return node.value

    def __setitem__(self, key, value):
        if self.table.has_key(key):
            # update
            node = self.table[key]
            node.value = value
            self.linked_list.move_to_head(node)
        else:
            # create new
            node = Node(key = key, value = value)
            self.linked_list.add_to_head(node)
            self.table[key] = node

    def peek(self, key):
        node = self.table[key]
        return node.value


def main():
    ld = KeyValueStoreInLinkedList()
    ld[2] = 22
    ld[3] = 33
    ld[4] = 44

    print ld
    print '-----------'

    lc = LruCache()
    lc[1] = 11


    lc[2] = 22
    lc[3] = 33

    print '-----initial------'
    print lc

    lc[1] = 3
    print '-----lc[1]=3------'
    print lc

    a = lc[2]
    print '-----a = lc[2]------'
    print lc


if __name__ == '__main__':
    main()


