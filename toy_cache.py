class SSDFramework(object):
    def __init__(self):
        self.datacache = FtlSim.datacache.DataCache(10, None)

    def datacache_process(self):
        """
        This process takes requests from host and serve the portion
        that can be served immediately. If cache eviction is needed,
        it will delete the entries from cache and generate flash
        write requests to write cache entries back to flash. After
        evicting from cache (not written to flash yet), the new
        entries can be added immediately.  This is the case in real
        world because you cannot evict without writing to flash.
        """
        host_event = yield queue.get()

        with datacache.resource.request() as datacache_lock:
            yield datacache_lock

            ssd_requests = split(host_event)

            req_todo = []
            for req in ssd_requests:
                if req.operation == 'read' and rq.in_cache == True:
                    continue
                elif req.operation == 'write' and req.in_cache == True:
                    update_cache(req)
                    if req.sync == True
                        req_todo.append(req)
                elif req.operation == 'write' and req.in_cache == False:
                    evict_reqs = req.evict_req_n(req.n) # will merge here
                    req_todo.append(evict_reqs)

                    add_to_cache(req)
                    if req.sync == True
                        req_todo.append(req)






