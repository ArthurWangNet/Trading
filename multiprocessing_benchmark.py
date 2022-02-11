import random
import time
import collections
import cProfile

LIST_LEN = 14000

def timefunc(f):
    t = time.time()
    f()
    return time.time() - t


def random_string(length=3):
    """Return a random string of given length"""
    return "".join([chr(random.randint(65, 90)) for i in range(length)])


class Profiler:
    def __init__(self):
        self.original = [[random_string() for i in range(LIST_LEN)]
                            for j in range(4)]

    def old_method(self):
        self.ListVar = self.original[:]
        for b in range(len(self.ListVar)):
            self.list1 = []
            self.temp = []
            for n in range(len(self.ListVar[b])):
                if not self.ListVar[b][n] in self.temp:
                    self.list1.insert(n, self.ListVar[b][n] + '(' +    str(self.ListVar[b].count(self.ListVar[b][n])) + ')')
                    self.temp.insert(0, self.ListVar[b][n])

            self.ListVar[b] = list(self.list1)
        return self.ListVar

    def new_method(self):
        self.ListVar = self.original[:]
        for i, inner_lst in enumerate(self.ListVar):
            freq_dict = collections.defaultdict(int)
            # create frequency dictionary
            for e in inner_lst:
                freq_dict[e] += 1
            temp = set()
            ret = []
            for e in inner_lst:
                if e not in temp:
                    ret.append(e + '(' + str(freq_dict[e]) + ')')
                    temp.add(e)
            self.ListVar[i] = ret
        return self.ListVar

    def time_and_confirm(self):
        """
        Time the old and new methods, and confirm they return the same value
        """
        time_a = time.time()
        l1 = self.old_method()
        time_b = time.time()
        l2 = self.new_method()
        time_c = time.time()

        # confirm that the two are the same
        assert l1 == l2, "The old and new methods don't return the same value"

        return time_b - time_a, time_c - time_b

p = Profiler()
print (p.time_and_confirm())