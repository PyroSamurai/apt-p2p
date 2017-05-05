### Rotate DNS entries for mirrors more reliably.
Currently the mirrors are accessed by DNS name, which can cause some
issues when there are mirror differences and the DNS gets rotated.
Instead, the HTTP Downloader should handle DNS lookups itself, store
the resulting addresses, and send requests to IP addresses. If there
is an error from the mirror (hash check or 404 response), the next IP
address in the rotation should be used.


### Use GPG signatures as a hash for files.
A detached GPG signature, such as is found in Release.gpg, can be used
as a hash for the file. This hash can be used to verify the file when
it is downloaded, and a shortened version can be added to the DHT to
look up peers for the file. To get the hash into a binary form from
the ascii-armored detached file, use the command
`gpg --no-options --no-default-keyring --output - --dearmor -`. The
hash should be stored as the reverse of the resulting binary string,
as the bytes at the beginning are headers that are the same for most
signatures. That way the shortened hash stored in the DHT will have a
better chance of being unique and being stored on different peers. To
verify a file, first the binary hash must be re-reversed, armored, and
written to a temporary file with the command
`gpg --no-options --no-default-keyring --output $tempfile --enarmor -`.
Then the incoming file can be verified with the command
`gpg --no-options --no-default-keyring --keyring /etc/apt/trusted.gpg
--verify $tempfile -`.

All communication with the command-line gpg should be done using pipes
and the python module python-gnupginterface. There needs to be a new
module for GPG verification and hashing, which will make this easier.
In particular, it would need to support hashlib-like functionality
such as new(), update(), and digest(). Note that the verification
would not involve signing the file again and comparing the signatures,
as this is not possible. Instead, the verify() function would have to
behave differently for GPG hashes, and check that the verification
resulted in a VALIDSIG. CAUTION: the detached signature can have a
variable length, though it seems to be usually 65 bytes, 64 bytes has
also been observed.


### Consider what happens when multiple requests for a file are received.
When another request comes in for a file already being downloaded,
the new request should wait for the old one to finish. This should
also be done for multiple requests for peer downloads of files with
the same hash.


### Packages.diff files need to be considered.
The Packages.diff/Index files contain hashes of Packages.diff/rred.gz 
files, which themselves contain diffs to the Packages files previously 
downloaded. Apt will request these files for the testing/unstable 
distributions. They need to be dealt with properly by 
adding them to the tracking done by the AptPackages module.


### Improve the estimation of the total number of nodes
The current total nodes estimation is based on the number of buckets.
A better way is to look at the average inter-node spacing for the K
closest nodes after a find_node/value completes. Be sure to measure
the inter-node spacing in log2 space to dampen any ill effects. This
can be used in the formula:
        nodes = 2^160 / 2^(average of log2 spacing)
The average should also be saved using an exponentially weighted
moving average (of the log2 distance) over separate find_node/value
actions to get a better calculation over time.


### Improve the downloaded and uploaded data measurements.
There are 2 places that this data is measured: for statistics, and for
limiting the upload bandwidth. They both have deficiencies as they
sometimes miss the headers or the requests sent out. The upload
bandwidth calculation only considers the stream in the upload and not
the headers sent, and it also doesn't consider the upload bandwidth
from requesting downloads from peers (though that may be a good thing).
The statistics calculations for downloads include the headers of
downloaded files, but not the requests received from peers for upload
files. The statistics for uploaded data only includes the files sent
and not the headers, and also misses the requests for downloads sent to
other peers.


### Rehash changed files instead of removing them.
When the modification time of a file changes but the size does not,
the file could be rehased to verify it is the same instead of
automatically removing it. The DB would have to be modified to return
deferred's for a lot of its functions.


### Consider storing deltas of packages.
Instead of downloading full package files when a previous version of
the same package is available, peers could request a delta of the
package to the previous version. This would only be done if the delta
is significantly (>50%) smaller than the full package, and is not too
large (absolutely). A peer that has a new package and an old one would
add a list of deltas for the package to the value stored in the DHT.
The delta information would specify the old version (by hash), the
size of the delta, and the hash of the delta. A peer that has the same
old package could then download the delta from the peer by requesting
the hash of the delta. Alternatively, very small deltas could be
stored directly in the DHT.


### Consider tracking security issues with packages.
Since sharing information with others about what packages you have
downloaded (and probably installed) is a possible security
vulnerability, it would be advantageous to not share that information
for packages that have known security vulnerabilities. This would
require some way of obtaining a list of which packages (and versions)
are vulnerable, which is not currently available.


### Consider adding peer characteristics to the DHT.
Bad peers could be indicated in the DHT by adding a new value that is
the NOT of their ID (so they are guaranteed not to store it) indicating
information about the peer. This could be bad votes on the peer, as
otherwise a peer could add good info about itself.


### Consider adding pieces to the DHT instead of files.
Instead of adding file hashes to the DHT, only piece hashes could be
added. This would allow a peer to upload to other peers while it is
still downloading the rest of the file. It is not clear that this is
needed, since peer's will not be uploading and downloading ery much of
the time.
