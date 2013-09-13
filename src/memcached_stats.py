import re, telnetlib, sys
import socket
import os

TIMEOUT = 3

class MemcachedStats(object):

    _client = None
    _key_regex = re.compile(ur'ITEM (.*) \[(.*); (.*)\]')
    _slab_regex = re.compile(ur'STAT items:(.*):number')
    _stat_regex = re.compile(ur"STAT (.*) (.*)\r")

    def __init__(self, host='localhost', port='11211', sock=False):
        self._host = host
        self._port = port
        if sock:
            self._sock = True

    @property
    def client(self):
        if self._client is None and not self._sock:
            self._client = telnetlib.Telnet(self._host, self._port)
        elif self._client is None and self._sock:
            if os.path.exists(self._host):
                self._client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._client.settimeout(TIMEOUT)
                self._client.connect(self._host)
            else:
                raise Exception('The specified socket file could not be found')
        return self._client

    def command(self, cmd):
        ' Write a command to telnet and return the response '
        if self._sock:
            tmp = ''
            self.client.send('%s\n' % cmd)
            while True:
                resp = self.client.recv(1024)
                if not resp:
                    break
                else:
                    tmp = ''.join([tmp, resp])
                    if 'END' in resp.strip():
                        break
            return tmp
        else:
            self.client.write("%s\n" % cmd)
            return self.client.read_until('END')

    def key_details(self, sort=True, limit=100):
        ' Return a list of tuples containing keys and details '
        cmd = 'stats cachedump %s %s'
        keys = [key for id in self.slab_ids()
            for key in self._key_regex.findall(self.command(cmd % (id, limit)))]
        if sort:
            return sorted(keys)
        else:
            return keys

    def keys(self, sort=True, limit=100):
        ' Return a list of keys in use '
        return [key[0] for key in self.key_details(sort=sort, limit=limit)]

    def slab_ids(self):
        ' Return a list of slab ids in use '
        return self._slab_regex.findall(self.command('stats items'))

    def stats(self):
        ' Return a dict containing memcached stats '
        return dict(self._stat_regex.findall(self.command('stats')))

def main(argv=None):
    """
    usage  python src/memcached_stats.py '/tmp/memcached.sock' 1234 True
    or usage  python src/memcached_stats.py '192.168.53.34' 11211
    """
    if not argv:
        argv = sys.argv
    host = argv[1] if len(argv) >= 2 else '127.0.0.1'
    port = argv[2] if len(argv) >= 3 else '11211'
    sock = argv[3] if len(argv) >= 4 else None
    import pprint
    m = MemcachedStats(host, port, sock)
    pprint.pprint(m.keys())
    if sock:
        m.client.close()

if __name__ == '__main__':
    main()
