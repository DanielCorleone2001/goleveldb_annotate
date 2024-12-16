#!/bin/bash

echo "# LevelDB MemDB Documentation" > memdb_doc.md
echo "" >> memdb_doc.md
go doc ./leveldb/memdb >> memdb_doc.md
