CREATE USER roacher;

--
-- Create necessary objects.
--
CREATE DATABASE IF NOT EXISTS sqlfs;
CREATE SEQUENCE IF NOT EXISTS sqlfs.inode_seq START 2;

CREATE TABLE IF NOT EXISTS sqlfs.tree (
  inode  INT DEFAULT nextval('inode_seq'),
  parent INT NOT NULL,
  name STRING NOT NULL,
  UNIQUE (name, parent),
  INDEX inode_idx (inode),
  INDEX parent_idx (parent)
);

CREATE TABLE IF NOT EXISTS sqlfs.inodes (
  inode INT,
  struct_data STRING,
  PRIMARY KEY (inode)
);

CREATE TABLE IF NOT EXISTS sqlfs.data_blocks (
  inode    INT,
  sequence INT,
  data     BYTES,
  PRIMARY KEY (inode, sequence)
);

GRANT ALL ON DATABASE sqlfs TO roacher;
GRANT ALL ON TABLE sqlfs.* TO roacher;
