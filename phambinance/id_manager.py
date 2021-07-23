import threading


class IdManagerException(Exception): ...

class IdManager:
    def __init__(self):
        self.ident = 0
        self.ident_lock = threading.Lock()
        self.events = {}
        self.responses = {}

    def issue_id(self):
        with self.ident_lock:
            new_ident = self.ident
            self.ident = self.ident + 1

        self.events[new_ident] = threading.Event()

        return new_ident

    def wait(self, ident, timeout=None):
        if ident not in self.events:
            raise IdManagerException(
                f"No outstanding event for id {ident}!")

        if self.events[ident].wait(timeout=timeout):
            del self.events[ident]
            return self.responses.pop(ident)

        return None

    def set_response(self, ident, response):
        if ident not in self.events:
            raise IdManagerException(
                f"No outstanding event for id {ident}!")
        self.responses[ident] = response
        self.events[ident].set()
