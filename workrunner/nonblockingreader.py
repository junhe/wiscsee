import sys, threading, Queue

def enqueue_lines(f, line_queue):
    for line in iter(f.readline, b''):
        line_queue.put(line)

class NonBlockingReader(object):
    def __init__(self, file_path):
        """Note that the thread is started once the instance is created"""
        self.file_path = file_path
        self.f = open(file_path, 'r')
        self.q = Queue.Queue()
        self.t = threading.Thread(target=enqueue_lines,
            args=(self.f, self.q))
        self.t.daemon = True # thread dies with the program
        self.t.start()

    def readline(self):
        """return None is there is nothing"""
        try:
            line = self.q.get_nowait()
        except Queue.Empty:
            return None
        else:
            return line


if __name__ == '__main__':
    nb_reader = NonBlockingReader("/sys/kernel/debug/tracing/trace_pipe")

    while True:
        line = nb_reader.readline()
        print line,


