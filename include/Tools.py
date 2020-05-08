""" 
    Converts integers to common size units used in computing 
    https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
    
    For method overloading an approach is used following the documentation here:
        https://pypi.org/project/pythonlangutil/0.1/
"""
# from multipledispatch import dispatch
from pythonlangutil.overload import Overload, signature
import math

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
