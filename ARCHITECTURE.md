# GLFS Architecture

GLFS is modeled after Git.
Regular files get turned into `blob` type objects.
Directories get turned into `tree` type objects, with cryptographic references to the blobs or trees for each entry.

Blob type objects do not actually correspond 1:1 to a single content addressable blob.
Large blobs will be broken up into fixed sized chunks.
Since the chunks are fixed sized, the entire tree structure can be determined based on the size of the object.
The `bigblob` package handles chunking large streams into blobs which can be stored in content-addressable storage.

Instead of the custom encoding format used by Git for Trees, GLFS uses JSON.
Trees are implemented as sorted lists of `TreeEntry` objects, serialized using JSON lines.
A TreeEntry contains the name, mode, and a reference to the object.

In Git, the type of the object is prepended to the actual object, so that it is possible to create a Blob that looks like a Tree.
GLFS does not do this, instead it ensures that `tree` type objects will never be encrypted with the same key as `blob` type objects.
So a Blob with the same serialized representation as a Tree will produce a distinct object.

