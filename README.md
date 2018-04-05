# iRODS-Tar-Untar
Collection Level Tar/Untar rules that can be automatically called.

Each of these rules are set up to be called by an 'irule -F FILENAME' command. However, I built them with the intention of using them for automatic ingest. So only the target collection needs to be input, and the untar only needs the tar file (and surrounding cksum file if desired for verification).
A handful of variables should be self-defined prior to execution. Such as: Where to compile tarballs, do we need a checksum double-check after untarring, and what resources in iRODS to store the data on after processing.
