import math

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

def bytes_transform(size_bytes, unit:str=None):
    """ 
        Converts integers to common size units used in computing 
        https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
    """
    if unit == None:
        unit = "MB"
    bit_shift = {
            "B": 0,     # Bytes
            "kb": 7,    # Kilobits
            "KB": 10,   # Kilobytes
            "mb": 17,   # Megabits
            "MB": 20,   # Megabytes
            "gb": 27,   # Gigabits
            "GB": 30,   # Gigabytes
            "TB": 40}   # Terabytes
    return round(size_bytes / float(1 << bit_shift[unit]), 2)