package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

func CreateLink(ctx context.Context, db *sql.DB, parent uint64, n *fileNode) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	q1 := "UPSERT INTO tree(inode, parent, name) VALUES ($1, $2, $3)"
	if _, err := tx.ExecContext(ctx, q1, n.Inode, parent, n.Name); err != nil {
		_ = tx.Rollback()
		return errors.Wrapf(err, "failed to upsert row into tree in parent %d", parent)
	}
	toUpdate, err := GetNodeByID(ctx, db, n.Inode)
	if err != nil {
		_ = tx.Rollback()
		return errors.Wrapf(err, "failed to retrieve node for update %d", n.Inode)
	}
	toUpdate.Nlink += 1
	toUpdate.Name = n.Name
	q2 := "UPSERT INTO inodes(inode, struct_data) VALUES ($1, $2)"
	if _, err := tx.ExecContext(ctx, q2, n.Inode, toUpdate.toJSON()); err != nil {
		_ = tx.Rollback()
		return errors.Wrapf(err, "failed to upsert into inodes for inode %d", n.Inode)
	}
	return tx.Commit()
}

func UpsertNode(ctx context.Context, db *sql.DB, parent uint64, n *fileNode) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	n.Mtime = time.Now()
	n.Ctime = time.Now()
	var lastId uint64
	q1 := "UPSERT INTO tree(parent, name) VALUES ($1, $2) RETURNING inode"
	if err := tx.QueryRowContext(ctx, q1, parent, n.Name).Scan(&lastId); err != nil {
		_ = tx.Rollback()
		return errors.Wrapf(err, "failed to upsert row into tree in parent %d", parent)
	}
	n.Inode = lastId
	q2 := "UPSERT INTO inodes(inode, struct_data) VALUES ($1, $2)"
	if _, err := tx.ExecContext(ctx, q2, lastId, n.toJSON()); err != nil {
		_ = tx.Rollback()
		return errors.Wrapf(err, "failed to upsert into inodes for inode %d", lastId)
	}
	return tx.Commit()
}

func RenameNode(
	ctx context.Context, db *sql.DB,
	oldParent uint64, oldName string, newParent uint64, newName string,
) error {
	q := "UPDATE tree SET name = $1, parent = $2 WHERE name = $3 AND parent = $4"
	if _, err := db.ExecContext(ctx, q, newName, newParent, oldName, oldParent); err != nil {
		return errors.Wrapf(err, "failed to rename node")
	}
	return nil
}

func CountNodesInDir(ctx context.Context, db *sql.DB, inode uint64) (int, error) {
	var count int
	q := "SELECT COUNT(*) FROM tree WHERE parent = $1"
	if err := db.QueryRowContext(ctx, q, inode).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountInodes(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	q := "SELECT COUNT(*) FROM inodes"
	if err := db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountDataBlocks(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	q := "SELECT COUNT(*) FROM data_blocks"
	if err := db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// ListNodesInDir obtains all nodes in the directory with Inode number `inode`.
func ListNodesInDir(ctx context.Context, db *sql.DB, inode uint64) ([]*fileNode, error) {
	if inode != rootInode {
		dir, err := GetNodeByID(ctx, db, inode)
		if err != nil {
			return nil, err
		}
		if !dir.IsDirectory() {
			return nil, errors.New("ListNodesInDir can only be called on directories")
		}
	}

	q := `SELECT
    tree.inode,
    tree.name,
    inodes.struct_data
  FROM tree JOIN inodes ON tree.inode = inodes.inode WHERE parent = $1`
	rows, err := db.QueryContext(ctx, q, inode)
	if err != nil {
		return nil, errors.Wrapf(err, "could not query entries in directory inode %d", inode)
	}
	defer rows.Close()

	var nodes []*fileNode
	for rows.Next() {
		var inode uint64
		var name string
		var struct_data string

		if err := rows.Scan(&inode, &name, &struct_data); err != nil {
			return nil, errors.Wrapf(err, "failed to scan files in directory inode %d", inode)
		}

		n := &fileNode{Name: name, Inode: inode}
		err := json.Unmarshal([]byte(struct_data), n)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshall inode %d struct", inode)
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func RemoveNodeByName(ctx context.Context, db *sql.DB, parent uint64, name string, inode uint64) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	q1 := "DELETE FROM tree WHERE parent = $1 and name = $2"
	if _, err := tx.ExecContext(ctx, q1, parent, name); err != nil {
		_ = tx.Rollback()
		return err
	}

	// Check if anything is still referencing inode.
	var count int
	q2 := "SELECT COUNT(*) FROM tree WHERE inode = $1"
	if err := tx.QueryRowContext(ctx, q2, inode).Scan(&count); err != nil {
		_ = tx.Rollback()
		return err
	}
	// Do not delete anything else.
	if count > 0 {
		return nil
	}

	// Remove references.
	q3 := "DELETE FROM inodes WHERE inode = $1"
	if _, err := tx.ExecContext(ctx, q3, inode); err != nil {
		_ = tx.Rollback()
		return err
	}
	q4 := "DELETE FROM data_blocks WHERE inode = $1"
	if _, err := tx.ExecContext(ctx, q4, inode); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// WriteData attempts to store `data` into the contents of file with Inode
// `inode`. This uses an inefficient implementation by deleting existing
// contents and adding them back again. We can definitely improve this
// if we were to focus on Offset and Size, but I'll skip that for now.
func WriteData(ctx context.Context, db *sql.DB, n *fileNode, data []byte) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	input := data

	q1 := "DELETE FROM data_blocks WHERE inode = $1"
	if _, err := tx.ExecContext(ctx, q1, n.Inode); err != nil {
		_ = tx.Rollback()
		return err
	}

	q2 := "INSERT INTO data_blocks (inode, sequence, data) VALUES ($1, $2, $3)"
	sequence := 1
	for {
		if len(input) == 0 {
			break
		}
		// Write chunks, or write everything.
		if len(input) > BLOCK_SIZE {
			if _, err = tx.ExecContext(ctx, q2, n.Inode, sequence, input[:BLOCK_SIZE]); err != nil {
				_ = tx.Rollback()
				return err
			}
			input = input[BLOCK_SIZE:]
		} else {
			if _, err = tx.ExecContext(ctx, q2, n.Inode, sequence, input); err != nil {
				_ = tx.Rollback()
				return err
			}
			input = input[:0] // Drop everything remaining.
		}
		sequence += 1
	}

	n.Size = uint64(len(data))
	q3 := "UPSERT INTO inodes(inode, struct_data) VALUES ($1, $2)"
	if _, err := tx.ExecContext(ctx, q3, n.Inode, n.toJSON()); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func ReadData(ctx context.Context, db *sql.DB, inode uint64) ([]byte, error) {
	q := "SELECT data FROM data_blocks WHERE inode = $1 ORDER BY sequence"
	rows, err := db.QueryContext(ctx, q, inode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []byte
	for rows.Next() {
		// TODO(imjching): This is costly. Fix this.
		currBlock := make([]byte, BLOCK_SIZE)
		if err := rows.Scan(&currBlock); err != nil {
			return nil, err
		}
		data = append(data, currBlock...)
	}
	return data, nil
}

func UpdateNode(ctx context.Context, db *sql.DB, n *fileNode) error {
	q := "UPSERT INTO inodes(inode, struct_data) VALUES ($1, $2)"
	if _, err := db.ExecContext(ctx, q, n.Inode, n.toJSON()); err != nil {
		return err
	}
	return nil
}

func GetNodeByName(ctx context.Context, db *sql.DB, parent uint64, name string) (*fileNode, error) {
	var inode uint64
	q := "SELECT inode FROM tree WHERE parent = $1 and name = $2 LIMIT 1"
	if err := db.QueryRowContext(ctx, q, parent, name).Scan(&inode); err != nil {
		return nil, err
	}
	return GetNodeByID(ctx, db, inode)
}

// GetNodeByID retrieves a node with Inode number `inode`.
func GetNodeByID(ctx context.Context, db *sql.DB, inode uint64) (*fileNode, error) {
	var struct_data string
	const q = "SELECT struct_data FROM inodes WHERE inode = $1 LIMIT 1"
	if err := db.QueryRowContext(ctx, q, inode).Scan(&struct_data); err != nil {
		// sql.ErrNoRows if no rows found.
		return nil, err
	}
	n := &fileNode{Inode: inode}
	err := json.Unmarshal([]byte(struct_data), n)
	return n, err
}
