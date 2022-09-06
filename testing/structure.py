import os, fnmatch

def get_directory_structure(path):
    return sorted([os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(path))
     for f in fn])