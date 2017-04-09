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

    def add_before2(self, new_node, node):
        """
        add new_node before node
        current: ->node1->node->
        later  : ->node1->new_node->node

        Note: this DOES handle list head
        """
        node1 = node.prev

        # modify new_node's pointers
        new_node.next = node
        new_node.prev = node1

        # node1
        node1.next = new_node
        # node
        node.prev = new_node

        if self._head is node:
            self._head = new_node

        self.size += 1

    def add_to_head(self, node):
        old_head = self._head
        self._head = node

        self.add_before(node, old_head)

    def add_to_tail(self, node):
        self.add_before(node, self._end_guard)

    def move_toward_head_by_one(self, node):
        "Boolean is returned to indicate status"
        if node is not self._head:
            # node is not head
            prev_node = node.prev
            self.delete(node)
            self.add_before2(new_node = node, node = prev_node)
        else:
            return False

    def move_toward_tail_by_one(self, node):
        "Boolean is returned to indicate status"
        if node.next is not self._end_guard:
            # node is not the tail
            next_next_node = node.next.next
            self.delete(node) # _head is handled properly here
            self.add_before2(new_node = node, node = next_next_node)
        else:
            return False

    def move_to_head(self, node):
        if self.size <= 0:
            raise RuntimeError("List size should be larger than 0")
        self.delete(node)
        self.add_to_head(node)

    def delete(self, node):
        if self.size <= 0:
            raise RuntimeError("List size should be larger than 0")

        # special case, node is the head
        if node is self._head:
            self._head = node.next

        prev_node = node.prev
        next_node = node.next

        prev_node.next = node.next
        next_node.prev = node.prev

        self.size -= 1

    def remove(self, node):
        "just another name for delete()"
        self.delete(node)

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

    def __reversed__(self):
        node = self._end_guard.prev
        while node is not self._end_guard:
            yield node
            node = node.prev

    def __len__(self):
        return self.size

    def __str__(self):
        listview = []
        node = self._head
        while node is not self._end_guard:
            listview.append(str(node))
            node = node.next
        listview = '->'.join(listview)

        return listview

class LruCache(collections.MutableMapping):
    """
    Geting and setting (recent use) a value will move it to the head
    of the list. It provides mapping interfaces like dict.
    """
    def __init__(self, data = None, **kwargs):
        self.table = {}
        # node must be type 'class Node'
        self.linked_list = LinkedList()

        if data == None:
            data = {}
        self.update(data, **kwargs)

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

    def __getitem__(self, key):
        node = self.table[key]
        self.linked_list.move_to_head(node)
        return node.value

    def __delitem__(self, key):
        node = self.table[key]
        del self.table[key]

        self.linked_list.delete(node)

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

    def add_as_least_used(self, key, value):
        assert not self.table.has_key(key)
        node = Node(key = key, value = value)
        self.linked_list.add_to_tail(node)
        self.table[key] = node

    def __iter__(self):
        # most recent -> least recent
        for node in self.linked_list:
            yield node.key

    def __reversed__(self):
        for node in reversed(self.linked_list):
            yield node.key

    def items(self):
        return self.least_to_most_items()


    def __len__(self):
        return len(self.linked_list)

    def peek(self, key):
        node = self.table[key]
        return node.value

    def orderless_update(self, key, value):
        node = self.table[key]
        node.value = value

    def least_to_most_items(self):
        for node in reversed(self.linked_list):
            yield node.key, node.value

    def least_recently_used_key(self):
        return self.linked_list.tail().key

    def most_recently_used_key(self):
        return self.linked_list.head().key

    def victim_key(self):
        return self.linked_list.tail().key

    def __repr__(self):
        t = []
        for node in self.linked_list:
            t.append((node.key, node.value))
        return str(t)



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

PROTECTED, PROBATIONARY = ("PROTECTED", "PROBATIONARY")


class LinkedListVisNode(LinkedList):
    """
    It is the same as LinkedList except it calls node.visual()
    in __repr__(). This requires the node in the list to have
    visual() method.
    """
    def __repr__(self):
        listview = []
        node = self._head
        while node is not self._end_guard:
            listview.append(node.visual())
            node = node.next
        listview = '->\n'.join(listview)

        return 'LinkedList:\n' + listview


class SegmentedLruCache(object):
    """
    nodes should have a type attribute: PROTECTED, PROBATIONARY
    It has key-value interface. Node is used internally.
    """
    def __init__(self, max_entries, max_protected_ratio):
        self.protected_list = LinkedListVisNode()
        self.probationary_list = LinkedListVisNode()

        self.max_entries = max_entries
        self.max_protected_entries = max_entries * max_protected_ratio

        self.table = {}

    def has_key(self, key):
        return self.table.has_key(key)

    def keys(self):
        return self.table.keys()

    def _add_new_node(self, node):
        """
        this node does not exist in any list
        add it to the MRU side of the probationary_list
        """
        node.owner_list = self.probationary_list
        self.probationary_list.add_to_head(node)

    def _remove_item(self, key):
        """
        Remove this item from all lists
        """
        node = self.table[key]
        del self.table[key]

        l = node.owner_list
        l.delete(node)

    def _move_from_prob_to_prot(self, node):
        """
        move from probationary to MRU side of protected.
        This is the only gate to get into protected list.
        The size of the overall cache does not change.
        """
        if len(self.protected_list) >= self.max_protected_entries:
            # need to evict
            victim_node = self.protected_list.tail()
            self._move_from_prot_to_prob(victim_node)

        self.probationary_list.delete(node)
        self.protected_list.add_to_head(node) # MRU side
        node.owner_list = self.protected_list

    def _move_from_prot_to_prob(self, node):
        "move from probationary to MRU side of protected"
        self.protected_list.delete(node)
        self.probationary_list.add_to_head(node) # MRU side
        node.owner_list = self.probationary_list

    def hit(self, node):
        "this method should be called when we have a hit"
        if node.owner_list is self.probationary_list:
            self._move_from_prob_to_prot(node)
        elif node.owner_list is self.protected_list:
            self.protected_list.move_to_head(node)

    ############### APIs  ################

    def items(self):
        for key in self.keys():
            yield key, self.table[key].value

    def __getitem__(self, key):
        node = self.table[key]
        self.hit(node)
        return node.value

    def get(self, key, default = None):
        if self.table.has_key(key):
            # will affect list order
            return self.__getitem__(key)
        else:
            # will not affect list order
            return default

    def peek(self, key):
        node = self.table[key]
        return node.value

    def __setitem__(self, key, value):
        """
        This should be the only API to add key-value to the cache!
        """
        if self.table.has_key(key):
            # update
            node = self.table[key]
            node.value = value
            self.hit(node)
        else:
            # create new
            node = Node(key = key, value = value)
            self.table[key] = node
            self._add_new_node(node)

    def victim_key(self):
        """
        Higher level class will handle the eviction.
        """
        node = self.probationary_list.tail()
        if node != None:
            return node.key

        node = self.protected_list.tail()
        if node != None:
            return node.key

        return None

    def is_full(self):
        return len(self.table) == self.max_entries

    def __delitem__(self, key):
        self._remove_item(key)

    def __iter__(self):
        return self.table.keys()

    def __len__(self):
        return len(self.table)

    def __repr__(self):
        return 'Protected List:' + repr(self.protected_list) + '\n' + \
            'Probationary List:' + repr(self.probationary_list)


class LruDict(collections.MutableMapping):
    # __getitem__, __setitem__, __delitem__, __iter__, __len__
    """
    All [] operations will change order of the key

    WARNING: If used with simpy, OrderedDict.items() and related functions
    are very slow (5 secs for a few thousands of items).
    """
    def __init__(self, data=None, **kwargs):
        """
        This is a regular constructor of dict. data can be mapping or an
        iterable. kwargs will become k-v pairs in the dict.
        """
        self._store = collections.OrderedDict()
        # self._store = dict()
        if data is None:
            data = {}
        self.update(data, **kwargs)

    def __getitem__(self, key):
        # will change order
        self._hit(key)
        return self._store[key]

    def __setitem__(self, key, value):
        # will change order
        self._store[key] = value
        self._hit(key)

    def _hit(self, key):
        value = self._store[key]
        del self._store[key]
        self._store[key] = value

    def __delitem__(self, key):
        del self._store[key]

    def least_to_most_iter(self):
        return self.__iter__()

    def __iter__(self):
        for k in self._store:
            yield k

    def most_to_least_iter(self):
        return self.__reversed__()

    def __reversed__(self):
        return reversed(self._store)

    def __len__(self):
        return len(self._store)

    def has_key(self, key):
        return self._store.has_key(key)

    def items(self):
        for k, v in self._store.items():
            yield k, v

    def least_to_most_items(self):
        return self._store.items()

    def victim_key(self):
        return self.least_recent()

    def most_recent(self):
        it = self.most_to_least_iter()
        return it.next()

    def least_recent(self):
        it = self.least_to_most_iter()
        return it.next()

    def peek(self, key):
        return self._store[key]


