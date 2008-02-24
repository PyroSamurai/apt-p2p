
"""Functions for bencoding and bdecoding data.

@type decode_func: C{dictionary} of C{function}
@var decode_func: a dictionary of function calls to be made, based on data,
    the keys are the first character of the data and the value is the
    function to use to decode that data
@type bencached_marker: C{list}
@var bencached_marker: mutable type to ensure class origination
@type encode_func: C{dictionary} of C{function}
@var encode_func: a dictionary of function calls to be made, based on data,
    the keys are the type of the data and the value is the
    function to use to encode that data
@type BencachedType: C{type}
@var BencachedType: the L{Bencached} type
"""

from types import IntType, LongType, StringType, ListType, TupleType, DictType, BooleanType
try:
    from types import UnicodeType
except ImportError:
    UnicodeType = None
from datetime import datetime
import time

from twisted.python import log
from twisted.trial import unittest

class BencodeError(ValueError):
    pass

def decode_int(x, f):
    """Bdecode an integer.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{int}, C{int}
    @return: the bdecoded integer, and the offset to read next
    @raise BencodeError: if the data is improperly encoded
    
    """
    
    f += 1
    newf = x.index('e', f)
    try:
        n = int(x[f:newf])
    except:
        n = long(x[f:newf])
    if x[f] == '-':
        if x[f + 1] == '0':
            raise BencodeError, "integer has a leading zero after a negative sign"
    elif x[f] == '0' and newf != f+1:
        raise BencodeError, "integer has a leading zero"
    return (n, newf+1)
  
def decode_string(x, f):
    """Bdecode a string.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{string}, C{int}
    @return: the bdecoded string, and the offset to read next
    @raise BencodeError: if the data is improperly encoded
    
    """
    
    colon = x.index(':', f)
    try:
        n = int(x[f:colon])
    except (OverflowError, ValueError):
        n = long(x[f:colon])
    if x[f] == '0' and colon != f+1:
        raise BencodeError, "string length has a leading zero"
    colon += 1
    return (x[colon:colon+n], colon+n)

def decode_unicode(x, f):
    """Bdecode a unicode string.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{int}, C{int}
    @return: the bdecoded unicode string, and the offset to read next
    
    """
    
    s, f = decode_string(x, f+1)
    return (s.decode('UTF-8'),f)

def decode_datetime(x, f):
    """Bdecode a datetime value.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{datetime.datetime}, C{int}
    @return: the bdecoded integer, and the offset to read next
    @raise BencodeError: if the data is improperly encoded
    
    """
    
    f += 1
    newf = x.index('e', f)
    try:
        date = datetime(*(time.strptime(x[f:newf], '%Y-%m-%dT%H:%M:%S')[0:6]))
    except:
        raise BencodeError, "datetime value could not be decoded: %s" % x[f:newf]
    return (date, newf+1)
  
def decode_list(x, f):
    """Bdecode a list.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{list}, C{int}
    @return: the bdecoded list, and the offset to read next
    
    """
    
    r, f = [], f+1
    while x[f] != 'e':
        v, f = decode_func[x[f]](x, f)
        r.append(v)
    return (r, f + 1)

def decode_dict(x, f):
    """Bdecode a dictionary.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{dictionary}, C{int}
    @return: the bdecoded dictionary, and the offset to read next
    @raise BencodeError: if the data is improperly encoded
    
    """
    
    r, f = {}, f+1
    lastkey = None
    while x[f] != 'e':
        k, f = decode_string(x, f)
        if lastkey >= k:
            raise BencodeError, "dictionary keys must be in sorted order"
        lastkey = k
        r[k], f = decode_func[x[f]](x, f)
    return (r, f + 1)

decode_func = {}
decode_func['l'] = decode_list
decode_func['d'] = decode_dict
decode_func['i'] = decode_int
decode_func['0'] = decode_string
decode_func['1'] = decode_string
decode_func['2'] = decode_string
decode_func['3'] = decode_string
decode_func['4'] = decode_string
decode_func['5'] = decode_string
decode_func['6'] = decode_string
decode_func['7'] = decode_string
decode_func['8'] = decode_string
decode_func['9'] = decode_string
decode_func['u'] = decode_unicode
decode_func['t'] = decode_datetime
  
def bdecode(x, sloppy = 0):
    """Bdecode a string of data.
    
    @type x: C{string}
    @param x: the data to decode
    @type sloppy: C{boolean}
    @param sloppy: whether to allow errors in the decoding
    @rtype: unknown
    @return: the bdecoded data
    @raise BencodeError: if the data is improperly encoded
    
    """
    
    try:
        r, l = decode_func[x[0]](x, 0)
#    except (IndexError, KeyError):
    except (IndexError, KeyError, ValueError):
        raise BencodeError, "bad bencoded data"
    if not sloppy and l != len(x):
        raise BencodeError, "bad bencoded data, all could not be decoded"
    return r

bencached_marker = []

class Bencached(object):
    """Dummy data structure for storing bencoded data in memory.
    
    @type marker: C{list}
    @ivar marker: mutable type to make sure the data was encoded by this class
    @type bencoded: C{string}
    @ivar bencoded: the bencoded data stored in a string
    
    """
    
    def __init__(self, s):
        """
        
        @type s: C{string}
        @param s: the new bencoded data to store
        
        """
        
        self.marker = bencached_marker
        self.bencoded = s

BencachedType = type(Bencached('')) # insufficient, but good as a filter

def encode_bencached(x,r):
    """Bencode L{Bencached} data.
    
    @type x: L{Bencached}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    assert x.marker == bencached_marker
    r.append(x.bencoded)

def encode_int(x,r):
    """Bencode an integer.
    
    @type x: C{int}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    r.extend(('i',str(x),'e'))

def encode_bool(x,r):
    """Bencode a boolean.
    
    @type x: C{boolean}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    encode_int(int(x),r)

def encode_string(x,r):    
    """Bencode a string.
    
    @type x: C{string}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    r.extend((str(len(x)),':',x))

def encode_unicode(x,r):
    """Bencode a unicode string.
    
    @type x: C{unicode}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    #r.append('u')
    encode_string(x.encode('UTF-8'),r)

def encode_datetime(x,r):
    """Bencode a datetime value in UTC.
    
    If the datetime object has time zone info, it is converted to UTC time.
    Otherwise it is assumed that the time is already in UTC time.
    Microseconds are removed.
    
    @type x: C{datetime.datetime}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    date = x.replace(microsecond = 0)
    offset = date.utcoffset()
    if offset is not None:
        utcdate = date.replace(tzinfo = None) + offset
    else:
        utcdate = date
    r.extend(('t',utcdate.isoformat(),'e'))

def encode_list(x,r):
    """Bencode a list.
    
    @type x: C{list}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    r.append('l')
    for e in x:
        encode_func[type(e)](e, r)
    r.append('e')

def encode_dict(x,r):
    """Bencode a dictionary.
    
    @type x: C{dictionary}
    @param x: the data to encode
    @type r: C{list}
    @param r: the currently bencoded data, to which the bencoding of x
        will be appended
    
    """
    
    r.append('d')
    ilist = x.items()
    ilist.sort()
    for k,v in ilist:
        r.extend((str(len(k)),':',k))
        encode_func[type(v)](v, r)
    r.append('e')

encode_func = {}
encode_func[BencachedType] = encode_bencached
encode_func[IntType] = encode_int
encode_func[LongType] = encode_int
encode_func[StringType] = encode_string
encode_func[ListType] = encode_list
encode_func[TupleType] = encode_list
encode_func[DictType] = encode_dict
encode_func[BooleanType] = encode_bool
encode_func[datetime] = encode_datetime
if UnicodeType:
    encode_func[UnicodeType] = encode_unicode
    
def bencode(x):
    """Bencode some data.
    
    @type x: unknown
    @param x: the data to encode
    @rtype: string
    @return: the bencoded data
    @raise BencodeError: if the data contains a type that cannot be encoded
    
    """
    r = []
    try:
        encode_func[type(x)](x, r)
    except:
        raise BencodeError, "failed to bencode the data"
    return ''.join(r)

class TestBencode(unittest.TestCase):
    """Test the bencoding and bdecoding of data."""

    timeout = 2

    def test_bdecode_string(self):
        self.failUnlessRaises(BencodeError, bdecode, '0:0:')
        self.failUnlessRaises(BencodeError, bdecode, '')
        self.failUnlessRaises(BencodeError, bdecode, '35208734823ljdahflajhdf')
        self.failUnlessRaises(BencodeError, bdecode, '2:abfdjslhfld')
        self.failUnlessEqual(bdecode('0:'), '')
        self.failUnlessEqual(bdecode('3:abc'), 'abc')
        self.failUnlessEqual(bdecode('10:1234567890'), '1234567890')
        self.failUnlessRaises(BencodeError, bdecode, '02:xy')
        self.failUnlessRaises(BencodeError, bdecode, '9999:x')

    def test_bdecode_int(self):
        self.failUnlessRaises(BencodeError, bdecode, 'ie')
        self.failUnlessRaises(BencodeError, bdecode, 'i341foo382e')
        self.failUnlessEqual(bdecode('i4e'), 4L)
        self.failUnlessEqual(bdecode('i0e'), 0L)
        self.failUnlessEqual(bdecode('i123456789e'), 123456789L)
        self.failUnlessEqual(bdecode('i-10e'), -10L)
        self.failUnlessRaises(BencodeError, bdecode, 'i-0e')
        self.failUnlessRaises(BencodeError, bdecode, 'i123')
        self.failUnlessRaises(BencodeError, bdecode, 'i6easd')
        self.failUnlessRaises(BencodeError, bdecode, 'i03e')

    def test_bdecode_list(self):
        self.failUnlessRaises(BencodeError, bdecode, 'l')
        self.failUnlessEqual(bdecode('le'), [])
        self.failUnlessRaises(BencodeError, bdecode, 'leanfdldjfh')
        self.failUnlessEqual(bdecode('l0:0:0:e'), ['', '', ''])
        self.failUnlessRaises(BencodeError, bdecode, 'relwjhrlewjh')
        self.failUnlessEqual(bdecode('li1ei2ei3ee'), [1, 2, 3])
        self.failUnlessEqual(bdecode('l3:asd2:xye'), ['asd', 'xy'])
        self.failUnlessEqual(bdecode('ll5:Alice3:Bobeli2ei3eee'), [['Alice', 'Bob'], [2, 3]])
        self.failUnlessRaises(BencodeError, bdecode, 'l01:ae')
        self.failUnlessRaises(BencodeError, bdecode, 'l0:')

    def test_bdecode_dict(self):
        self.failUnlessRaises(BencodeError, bdecode, 'd')
        self.failUnlessRaises(BencodeError, bdecode, 'defoobar')
        self.failUnlessEqual(bdecode('de'), {})
        self.failUnlessEqual(bdecode('d3:agei25e4:eyes4:bluee'), {'age': 25, 'eyes': 'blue'})
        self.failUnlessEqual(bdecode('d8:spam.mp3d6:author5:Alice6:lengthi100000eee'),
                             {'spam.mp3': {'author': 'Alice', 'length': 100000}})
        self.failUnlessRaises(BencodeError, bdecode, 'd3:fooe')
        self.failUnlessRaises(BencodeError, bdecode, 'di1e0:e')
        self.failUnlessRaises(BencodeError, bdecode, 'd1:b0:1:a0:e')
        self.failUnlessRaises(BencodeError, bdecode, 'd1:a0:1:a0:e')
        self.failUnlessRaises(BencodeError, bdecode, 'd0:0:')
        self.failUnlessRaises(BencodeError, bdecode, 'd0:')

    def test_bdecode_unicode(self):
        self.failUnlessRaises(BencodeError, bdecode, 'u0:0:')
        self.failUnlessRaises(BencodeError, bdecode, 'u')
        self.failUnlessRaises(BencodeError, bdecode, 'u35208734823ljdahflajhdf')
        self.failUnlessRaises(BencodeError, bdecode, 'u2:abfdjslhfld')
        self.failUnlessEqual(bdecode('u0:'), '')
        self.failUnlessEqual(bdecode('u3:abc'), 'abc')
        self.failUnlessEqual(bdecode('u10:1234567890'), '1234567890')
        self.failUnlessRaises(BencodeError, bdecode, 'u02:xy')
        self.failUnlessRaises(BencodeError, bdecode, 'u9999:x')

    def test_bencode_int(self):
        self.failUnlessEqual(bencode(4), 'i4e')
        self.failUnlessEqual(bencode(0), 'i0e')
        self.failUnlessEqual(bencode(-10), 'i-10e')
        self.failUnlessEqual(bencode(12345678901234567890L), 'i12345678901234567890e')

    def test_bencode_string(self):
        self.failUnlessEqual(bencode(''), '0:')
        self.failUnlessEqual(bencode('abc'), '3:abc')
        self.failUnlessEqual(bencode('1234567890'), '10:1234567890')

    def test_bencode_list(self):
        self.failUnlessEqual(bencode([]), 'le')
        self.failUnlessEqual(bencode([1, 2, 3]), 'li1ei2ei3ee')
        self.failUnlessEqual(bencode([['Alice', 'Bob'], [2, 3]]), 'll5:Alice3:Bobeli2ei3eee')

    def test_bencode_dict(self):
        self.failUnlessEqual(bencode({}), 'de')
        self.failUnlessEqual(bencode({'age': 25, 'eyes': 'blue'}), 'd3:agei25e4:eyes4:bluee')
        self.failUnlessEqual(bencode({'spam.mp3': {'author': 'Alice', 'length': 100000}}), 
                             'd8:spam.mp3d6:author5:Alice6:lengthi100000eee')
        self.failUnlessRaises(BencodeError, bencode, {1: 'foo'})

    def test_bencode_unicode(self):
        self.failUnlessEqual(bencode(u''), '0:')
        self.failUnlessEqual(bencode(u'abc'), '3:abc')
        self.failUnlessEqual(bencode(u'1234567890'), '10:1234567890')

    def test_bool(self):
        self.failUnless(bdecode(bencode(True)))
        self.failIf(bdecode(bencode(False)))

    def test_datetime(self):
        date = datetime.utcnow()
        self.failUnlessEqual(bdecode(bencode(date)), date.replace(microsecond = 0))

    if UnicodeType == None:
        test_bencode_unicode.skip = "Python was not compiled with unicode support"
        test_bdecode_unicode.skip = "Python was not compiled with unicode support"
