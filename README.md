FSPR
====

FSPR is a collection of the interesting fs.* functionality
promisified.  Most of the interfaces are just simply converted by
replacing callbacks with promise resolve or reject.

However, readfile and writefile are more elaborate. In addition to
static buffers or strings, they can also handle input and output from
streams and also make optional run time constraint checks and hash
calculations etc.

This is still work in progress.


Author
======

Timo J. Rinne <tri@iki.fi>


License
=======

GPL-2.0
