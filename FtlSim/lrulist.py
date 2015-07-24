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


class DictByLinkedList(collections.MutableMapping):
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

class LruCache(DictByLinkedList):
    """
    geeting and setting a value will move it to the head of the list
    It provides mapping interfaces like dict.
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

    def orderless_update(self, key, value):
        node = self.table[key]
        node.value = value

    def least_recently_used_key(self):
        return self.linked_list.tail().key

    def most_recently_used_key(self):
        return self.linked_list.tail().key

    def victim_key(self):
        return self.linked_list.tail().key



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

    def add_new_node(self, node):
        """
        this node does not exist in any list
        add it to the MRU side of the probationary_list
        """
        node.owner_list = self.probationary_list
        self.probationary_list.add_to_head(node)

    def remove_item(self, key):
        """
        Remove this item from all lists
        """
        node = self.table[key]
        del self.table[key]

        l = node.owner_list
        l.delete(node)

    def move_from_prob_to_prot(self, node):
        """
        move from probationary to MRU side of protected.
        This is the only gate to get into protected list.
        The size of the overall cache does not change.
        """
        if len(self.protected_list) > self.max_protected_entries:
            # need to evict
            victim_node = self.protected_list.tail()
            self.move_from_prot_to_prob(victim_node)

        self.probationary_list.delete(node)
        self.protected_list.add_to_head(node) # MRU side
        node.owner_list = self.protected_list

    def move_from_prot_to_prob(self, node):
        "move from probationary to MRU side of protected"
        self.protected_list.delete(node)
        self.probationary_list.add_to_head(node) # MRU side
        node.owner_list = self.probationary_list

    def hit(self, node):
        "this method should be called when we have a hit"
        if node.owner_list is self.probationary_list:
            self.move_from_prob_to_prot(node)
        elif node.owner_list is self.protected_list:
            self.protected_list.move_to_head(node)

    ############### APIs  ################
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
            if self.is_full():
                raise RuntimeError("Trying to add key-value to a full cache")

            node = Node(key = key, value = value)
            self.table[key] = node
            self.add_new_node(node)

    def victim_key(self):
        """
        Higher level class will handle the eviction.
        """
        v = self.probationary_list.tail()
        if v != None:
            return v

        v = self.protected_list.tail()
        if v != None:
            return v

        return None

    def is_full(self):
        return len(self.table) == self.max_entries

    def __delitem__(self, key):
        self.remove_item(key)

    def __iter__(self):
        return self.table.keys()

    def __len__(self):
        return len(self.table)

    def __repr__(self):
        return 'Protected List:' + repr(self.protected_list) + '\n' + \
            'Probationary List:' + repr(self.probationary_list)

def main():
    sl = SegmentedLruCache(5, 0.5)
    sl[2] = 22
    sl[2] = 222
    sl[3] = 33
    sl[3] = 33
    sl[4] = 44
    sl[4] = 44
    sl[5] =  55
    sl[5] =  55
    a = sl[2]

    sl[6] =  55
    sl[1] =  11

    print sl


if __name__ == '__main__':
    main()


