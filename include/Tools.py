""" 
    Converts integers to common size units used in computing 
    https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
    
    For method overloading an approach is used following the documentation here:
        https://pypi.org/project/pythonlangutil/0.1/
"""
# from multipledispatch import dispatch
from pythonlangutil.overload import Overload, signature
import math
import datetime as dt

class BiteSize:
        
    @Overload
    @signature("int")
    def transform(self, size_bytes:int=0) -> str:
        if size_bytes == 0:
            return "0B"        
        size_name = ("B", "KB", "MB", "GB", "TB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "{0} {1}".format(s, size_name[i])

    @transform.overload
    @signature("int", "str")
    def transform(self, size_bytes:int=0, unit:str="KB") -> float:
        bit_shift = {
                "B": 0,     # Bytes
                "KB": 10,   # KiloBytes
                "MB": 20,   # MegaBytes
                "GB": 30,   # GigaBytes
                "TB": 40}   # TeraBytes
        return round(size_bytes / float(1 << bit_shift[unit]), 2)
    
class TimeUtil:
    @classmethod
    def getDTString(cls, t:dt.datetime=None) -> str:
        if not t:
            t = dt.datetime.now()
        return t.strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def getDuration(cls, t:dt.datetime=None) -> str:
        duration = (dt.datetime.now() - t).total_seconds()
        return "{0:02d}:{1:02d}:{2:02d}".format(
            int(divmod(duration, 3600)[0]),   # hrs
            int(divmod(duration, 60)[0]),     # mins
            int(duration % 60)  
        )