
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
from cStringIO import StringIO

from twisted.python import log

def decode_int(x, f):
    """Bdecode an integer.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{int}, C{int}
    @return: the bdecoded integer, and the offset to read next
    @raise ValueError: if the data is improperly encoded
    
    """
    
    f += 1
    newf = x.index('e', f)
    try:
        n = int(x[f:newf])
    except:
        n = long(x[f:newf])
    if x[f] == '-':
        if x[f + 1] == '0':
            raise ValueError
    elif x[f] == '0' and newf != f+1:
        raise ValueError
    return (n, newf+1)
  
def decode_string(x, f):
    """Bdecode a string.
    
    @type x: C{string}
    @param x: the data to decode
    @type f: C{int}
    @param f: the offset in the data to start at
    @rtype: C{string}, C{int}
    @return: the bdecoded string, and the offset to read next
    @raise ValueError: if the data is improperly encoded
    
    """
    
    colon = x.index(':', f)
    try:
        n = int(x[f:colon])
    except (OverflowError, ValueError):
        n = long(x[f:colon])
    if x[f] == '0' and colon != f+1:
        raise ValueError
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
    @raise ValueError: if the data is improperly encoded
    
    """
    
    r, f = {}, f+1
    lastkey = None
    while x[f] != 'e':
        k, f = decode_string(x, f)
        if lastkey >= k:
            raise ValueError
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
  
def bdecode(x, sloppy = 0):
    """Bdecode a string of data.
    
    @type x: C{string}
    @param x: the data to decode
    @type sloppy: C{boolean}
    @param sloppy: whether to allow errors in the decoding
    @rtype: unknown
    @return: the bdecoded data
    @raise ValueError: if the data is improperly encoded
    
    """
    
    try:
        r, l = decode_func[x[0]](x, 0)
#    except (IndexError, KeyError):
    except (IndexError, KeyError, ValueError):
        log.err()
        raise ValueError, "bad bencoded data"
    if not sloppy and l != len(x):
        raise ValueError, "bad bencoded data"
    return r

def test_bdecode():
    """A test routine for the bdecoding functions."""
    try:
        bdecode('0:0:')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('ie')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('i341foo382e')
        assert 0
    except ValueError:
        pass
    assert bdecode('i4e') == 4L
    assert bdecode('i0e') == 0L
    assert bdecode('i123456789e') == 123456789L
    assert bdecode('i-10e') == -10L
    try:
        bdecode('i-0e')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('i123')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('i6easd')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('35208734823ljdahflajhdf')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('2:abfdjslhfld')
        assert 0
    except ValueError:
        pass
    assert bdecode('0:') == ''
    assert bdecode('3:abc') == 'abc'
    assert bdecode('10:1234567890') == '1234567890'
    try:
        bdecode('02:xy')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('l')
        assert 0
    except ValueError:
        pass
    assert bdecode('le') == []
    try:
        bdecode('leanfdldjfh')
        assert 0
    except ValueError:
        pass
    assert bdecode('l0:0:0:e') == ['', '', '']
    try:
        bdecode('relwjhrlewjh')
        assert 0
    except ValueError:
        pass
    assert bdecode('li1ei2ei3ee') == [1, 2, 3]
    assert bdecode('l3:asd2:xye') == ['asd', 'xy']
    assert bdecode('ll5:Alice3:Bobeli2ei3eee') == [['Alice', 'Bob'], [2, 3]]
    try:
        bdecode('d')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('defoobar')
        assert 0
    except ValueError:
        pass
    assert bdecode('de') == {}
    assert bdecode('d3:agei25e4:eyes4:bluee') == {'age': 25, 'eyes': 'blue'}
    assert bdecode('d8:spam.mp3d6:author5:Alice6:lengthi100000eee') == {'spam.mp3': {'author': 'Alice', 'length': 100000}}
    try:
        bdecode('d3:fooe')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('di1e0:e')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('d1:b0:1:a0:e')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('d1:a0:1:a0:e')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('i03e')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('l01:ae')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('9999:x')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('l0:')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('d0:0:')
        assert 0
    except ValueError:
        pass
    try:
        bdecode('d0:')
        assert 0
    except ValueError:
        pass

bencached_marker = []

class Bencached:
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
if BooleanType:
    encode_func[BooleanType] = encode_bool
if UnicodeType:
    encode_func[UnicodeType] = encode_unicode
    
def bencode(x):
    """Bencode some data.
    
    @type x: unknown
    @param x: the data to encode
    @rtype: string
    @return: the bencoded data
    @raise ValueError: if the data contains a type that cannot be encoded
    
    """
    r = []
    try:
        encode_func[type(x)](x, r)
    except:
        log.err()
        assert 0
    return ''.join(r)

def test_bencode():
    """A test routine for the bencoding functions."""
    assert bencode(4) == 'i4e'
    assert bencode(0) == 'i0e'
    assert bencode(-10) == 'i-10e'
    assert bencode(12345678901234567890L) == 'i12345678901234567890e'
    assert bencode('') == '0:'
    assert bencode('abc') == '3:abc'
    assert bencode('1234567890') == '10:1234567890'
    assert bencode([]) == 'le'
    assert bencode([1, 2, 3]) == 'li1ei2ei3ee'
    assert bencode([['Alice', 'Bob'], [2, 3]]) == 'll5:Alice3:Bobeli2ei3eee'
    assert bencode({}) == 'de'
    assert bencode({'age': 25, 'eyes': 'blue'}) == 'd3:agei25e4:eyes4:bluee'
    assert bencode({'spam.mp3': {'author': 'Alice', 'length': 100000}}) == 'd8:spam.mp3d6:author5:Alice6:lengthi100000eee'
    try:
        bencode({1: 'foo'})
        assert 0
    except AssertionError:
        pass

  
try:
    import psyco
    psyco.bind(bdecode)
    psyco.bind(bencode)
except ImportError:
    pass
