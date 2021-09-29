import pathlib

CONFIG = {
    'local': True,
    'release': False,
}

def figure_out_binary():
    # When run on NayDuck we end up with a binary called neard in target/debug
    # but when run locally the binary might be near-sandbox instead.  Try to
    # figure out whichever binary is available and use that.
    for release in ('release', 'debug'):
        root = pathlib.Path('../target') / release
        for exe in ('near-sandbox', 'neard'):
            if (root / exe).exists():
                CONFIG['near_root'] = str(root)
                CONFIG['binary_name'] = exe
                return
    assert False, ('Unable to figure out location of near-sandbox binary; '
                   'Did you forget to run `make sandbox`?')
