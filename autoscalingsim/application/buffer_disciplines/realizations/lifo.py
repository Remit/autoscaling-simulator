from ..discipline import QueuingDiscipline

from ....load.request import Request

@QueuingDiscipline.register('LIFO')
class LIFOQueue(QueuingDiscipline):

    def insert(self,
               req : Request):

        self.requests.append(req)

    def attempt_take(self):

        """ Provides a copy of the earliest request in the queue """

        if len(self.requests) > 0:
            return self.requests[-1]
        else:
            return None

    def take(self):

        req = None
        if len(self.requests) > 0:
            req = self.requests.pop()

        return req

    def shuffle(self):

        req = self.take()
        self.requests.appendleft(req)
