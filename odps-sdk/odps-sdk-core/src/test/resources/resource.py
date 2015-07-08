from test_sandbox import HACKED

S = ''
try:
    # Test 3rd party module import another 3rd party module.
    import os
except ImportError, e:
    S = str(e)

FakeDis = 'FakeDis: ' + HACKED + ' ' + S
