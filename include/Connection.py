import re
import datetime as dt

class Connection(object):
    """
        https://manuals.gfi.com/en/kerio/control/content/logs/using-the-connection-log-1457.html
        
        Log example:
        [18/Apr/2013 10:22:47] [ID] 613181 [Rule] NAT [Service] HTTP [User] winston [Connection] TCP 192.168.1.140:1193 -> hit.google.com:80 [Duration] 121 sec [Bytes] 1575/1290/2865 [Packets] 5/9/14
    """
    
    grammar = {
        
        # DATETIME
            "DATETIME": r'\A\[(.+[\d{2}])\]',
        
        # ID
            "ID" : r'\[ID\]\s(.+)\s\[Rule',
        
        # Rule
            "Rule" : r'\[Rule\]\s(.*?)\s(?=\[Service|\[User|\[Connection)',
        
        # Service [OPTIONAL]:
            "Service" : r'(?=.*\[Service)\[Service\]\s(.*?)\s(?=\[User|\[Connection)',
        
        # User [OPTIONAL]:
            "User" : r'(?=.*\[User)\[User\]\s(.*?)\s(?=\[Connection)',
        
        # Connection: protocol src_host (src_ip):src_port -> dst_host (dst_ip):dst_port
            "Connection" : r'\[Connection\]\s(.+)\s\[Duration',
        
        # Duration: n sec
            "Duration" : r'\[Duration\]\s(\d+)\ssec\s\[Bytes',
        
        # Bytes: Transmitted/Accepted/Total
            "Bytes" : r'\[Bytes\]\s(.+)\s\[Packets',
        
        # Packets: Transmitted/Accepted/Total
            "Packets" : r'\[Packets\]\s(.+)[\n|$]' 
        }

    @classmethod
    def parse(cls, s:str) -> ():
        keys = []
        values = []
        for index, key in enumerate(list(cls.grammar.keys())):
            value = re.findall(cls.grammar[key], s)
            if key == "DATETIME":
                keys.append(key)
                values.append(dt.datetime.strptime(value[0], '%d/%b/%Y %H:%M:%S'))
                # https://stackoverflow.com/questions/466345/converting-string-into-datetime
            elif key == "ID":
                keys.append(key)
                values.append(int(value[0]))
            elif key in ["Rule", "Service", "User", "Duration"]:
                keys.append(key)
                values.append((value[0]) if len(value) > 0 else "")
            elif key == "Connection":
                ckeys, cvalues = cls.parseConnectionInfo(value[0])
                for cindex, ckey in enumerate(ckeys):
                    keys.append(ckey)
                    values.append(cvalues[cindex])
            elif key == "Bytes":
                for bindex, bkey in enumerate(["Bytes.Transmitted", "Bytes.Accepted", "Bytes.Total"]):                    
                    keys.append(bkey)
                    values.append(int(value[0].split("/")[bindex]))
            elif key == "Packets":
                for pindex, pkey in enumerate(["Packets.Transmitted", "Packets.Accepted", "Packets.Total"]):                    
                    keys.append(pkey)
                    values.append(int(value[0].split("/")[pindex]))

        return (keys, values)
    
    @classmethod
    def parseConnectionInfo(cls, s:str) -> ():
        # protocol src_host (src_ip):src_port -> dst_host (dst_ip):dst_port
        # connectionGrammar = {
        #     "protocol" : r'\A(\w*?)\s',
        #     "src_host" : r'\A\w*?\s(.*?)\s',
        # }
        keys = ["Protocol", 
                "SourceHost", "SourceIp", "SourcePort",
                "DestinationHost", "DestinationIp", "DestinationPort"]
        values = re.findall(r'\A(\w*?)\s(\S*?)?\s?\(?((?:[0-9]{1,3}\.){3}[0-9]{1,3})\)?:?(\d+)?\s\-\>\s(\S*?)?\s?\(?((?:[0-9]{1,3}\.){3}[0-9]{1,3})\)?:?(\d+)?$', s)[0]
        return (keys, values)