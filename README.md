# dupe

Don't run this on important files. Or really any files you even
remotely care about.

This is a deduplicator for btrfs or other COW-enabled filesystems.
It's not very smart yet. It operates at the file-level, as opposed
to other implementations that are smarter than operate at the extent
level.
