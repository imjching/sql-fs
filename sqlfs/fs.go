package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	fuseFS "bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
)

type fileSystem struct {
	db *sql.DB
}

const (
	rootInode = 1

	// Note that reading or writing will become slower is block size is smaller.
	BLOCK_SIZE = 1024
)

// Error types:
//   ENOSYS  // Function not implemented
//   ESTALE  // Stale NFS file handle
//   ENOENT  // No such file or directory
//   EIO     // I/O error
//   EPERM   // Operation not permitted
//   EINTR   // Interrupted system call
//   ERANGE  // Math result not representable
//   ENOTSUP // Not supported
//   EEXIST  // File exists
//   ENOATTR // Attribute not found

// Obtains the fuseFS.Node for the file system root.
// Root implements the fuseFS.FS interface.
func (fs fileSystem) Root() (fuseFS.Node, error) {
	return &fileNode{
		Inode: rootInode,
		Mode:  os.ModeDir | 0555,
		fs:    &fs,
	}, nil
}

// Used to obtain file system metadata. (e.g. by `df`)
// References:
// - https://github.com/coreutils/coreutils/blob/master/src/df.c
// - http://man7.org/linux/man-pages/man2/statfs.2.html
// Statfs implements the fuseFS.FSStatfser interface.
func (fs fileSystem) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	// resp.Bsize = 1024  // Optimal file system block size
	resp.Bsize = BLOCK_SIZE // Optimal file system block size
	blockCount, err := CountDataBlocks(ctx, fs.db)
	if err == nil {
		resp.Blocks = uint64(blockCount) // Total data blocks in file system of size `Bsize` each.
		// log.Printf("Blocks: %d", blockCount)
	} else {
		log.Println(err)
	}
	// resp.Bfree = 200  // Free blocks in file system.
	// resp.Bavail = 100 // Free blocks in file system for use by unprivileged users.

	// Since we are using a SQL database, the total number of file nodes in
	// the file system would be the maximum number that the `id` column could
	// support. However, if we were to delete inodes, there would be "holes"
	// in the list of available inode IDs. We will ignore that for now.
	//
	// Note that files like empty files, devices, fifos, sockets, and symlinks
	// consume inodes, but don't consume blocks.
	// References:
	// - https://scoutapm.com/blog/understanding-disk-inodes
	inodeCount, err := CountInodes(ctx, fs.db)
	if err == nil {
		resp.Files = uint64(inodeCount) // Total number of file nodes in file system.
	} else {
		log.Println(err)
	}
	// resp.Ffree = 200 // Free file nodes in file system.
	// resp.Namelen = 600 // Maximum file name length

	// Fragment size, smallest addressable data size in the file system.
	// Usually the same as `Bsize` and is dependent on VFS.
	// See https://stackoverflow.com/q/54823541/3281979 on Frsize vs Bsize.
	resp.Frsize = resp.Bsize

	return nil
}

// Triggered by unmounting. Usually used to free memory associated with the
// FUSE system or allow file systems to flush writes to the disk before the
// unmounting completes.
//
// There are two main types of devices: character device and block device.
// Character devices communicate by sending and receiving single characters
// whereas block devices communicate by sending blocks of data. In Linux
// file systems, this will only be called for block devices.
// Destroy implements the fuseFS.FSDestroyer interface.
func (fs fileSystem) Destroy() {
	log.Println("Destroy called")
}

// Picks a dynamic inode number when it would otherwise be 0.
// GenerateInode implements the fuseFS.FSInodeGenerator interface.
func (fs fileSystem) GenerateInode(parentInode uint64, name string) uint64 {
	log.Printf("GenerateInode called - parentInode: %d, name %q\n", parentInode, name)
	return fuseFS.GenerateDynamicInode(parentInode, name)
}

// Methods that are not implemented:
//
// Forget()
// - Node will not receive further method calls. Not necessarily seen on
//   unmount. Might be useful for caching.
//
// Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error
// - Gets an extended attribute by the given name from the node.
// - If there is no xattr by that name, returns fuse.ErrNoXattr.
//
// Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error
// - Lists the extended attributes recorded for the node.
//
// Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error
// - Sets an extended attribute with the given name and value for the node.
//
// Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error
// - Removes an extended attribute for the name.
// - If there is no xattr by that name, returns fuse.ErrNoXattr.
//
// Flush(ctx context.Context, req *fuse.FlushRequest) error
// - Called each time the file or directory is closed.
// - Because there can be multiple file descriptors referring to a single
//   opened file, Flush can be called multiple times.
//
// Release(ctx context.Context, req *fuse.ReleaseRequest) error
//
// Access(ctx context.Context, req *fuse.AccessRequest) error
//
// Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fuseFS.Handle, error)
//
// Getattr(ctx context.Context, req *fuse.GetattrRequest, resp *fuse.GetattrResponse) error
//
type fileNode struct {
	fs *fileSystem

	// Values needed by fuse.Attr().
	Valid     time.Duration // how long Attr can be cached
	Inode     uint64        `json:"-"` // inode number
	Size      uint64        // size in bytes
	Blocks    uint64        // size in 512-byte units
	Atime     time.Time     // time of last access
	Mtime     time.Time     // time of last modification
	Ctime     time.Time     // time of last inode change
	Crtime    time.Time     // time of creation (OS X only)
	Mode      os.FileMode   // file mode
	Nlink     uint32        // number of links (usually 1)
	Uid       uint32        // owner uid
	Gid       uint32        // group gid
	Rdev      uint32        // device numbers
	Flags     uint32        // chflags(2) flags (OS X only)
	BlockSize uint32        // preferred blocksize for filesystem I/O

	// Custom values used by filesystem.
	Name          string `json:"-"`
	SymlinkTarget string
}

func (n *fileNode) toJSON() string {
	out, err := json.Marshal(n)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func (n *fileNode) IsRegular() bool {
	return n.Mode.IsRegular()
}

func (n *fileNode) IsDirectory() bool {
	return n.Mode.IsDir()
}

func (n *fileNode) IsSymlink() bool {
	return n.Mode&os.ModeSymlink != 0
}

// Fsync implements the fuseFS.NodeFsyncer interface.
func (n *fileNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// If we don't implement this, some applications like vim would not work.
	return nil
}

// Fills `attr` with the standard metadata for the node.
// Attr implements the fuseFS.Node interface.
func (n *fileNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = n.Inode
	attr.Size = n.Size
	if n.IsSymlink() {
		attr.Size = uint64(len(n.SymlinkTarget))
	}
	attr.Blocks = n.Size / 512
	attr.Atime = n.Atime
	attr.Mtime = n.Mtime
	attr.Ctime = n.Ctime
	attr.Crtime = n.Crtime
	attr.Mode = n.Mode
	updated, err := GetNodeByID(ctx, n.fs.db, n.Inode)
	if err == nil {
		attr.Nlink = updated.Nlink
	} else {
		attr.Nlink = n.Nlink // How many entries using the same inode number.
	}
	attr.Uid = n.Uid
	attr.Gid = n.Gid
	attr.Rdev = n.Rdev
	attr.Flags = n.Flags
	attr.BlockSize = BLOCK_SIZE
	return nil
}

// Setattr sets the standard metadata for the receiver.
// Note, this is also used to communicate changes in the size of
// the file, outside of Writes.
// req.Valid is a bitmask of what fields are actually being set.
// For example, the method should not change the mode of the file
// unless req.Valid.Mode() is true.
// Setattr implements the fuseFS.NodeSetattrer interface.
func (n *fileNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Mode() {
		n.Mode = req.Mode
		resp.Attr.Mode = req.Mode
	}
	if req.Valid.Uid() {
		n.Uid = req.Uid
		resp.Attr.Uid = req.Uid
	}
	if req.Valid.Gid() {
		n.Gid = req.Gid
		resp.Attr.Gid = req.Gid
	}
	if req.Valid.Size() {
		n.Size = req.Size
		resp.Attr.Size = req.Size
	}
	if req.Valid.Atime() {
		n.Atime = req.Atime
		resp.Attr.Atime = req.Atime
	}
	if req.Valid.Mtime() {
		n.Mtime = req.Mtime
		resp.Attr.Mtime = req.Mtime
	}
	if req.Valid.Crtime() {
		n.Crtime = req.Crtime
		resp.Attr.Crtime = req.Crtime
	}
	if err := UpdateNode(ctx, n.fs.db, n); err != nil {
		log.Println(err)
		return fuse.EIO
	}
	return nil
}

// Symlink creates a new symbolic link in the receiver, which must be a directory.
// Symlink implements the fuseFS.NodeSymlinker interface.
func (n *fileNode) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fuseFS.Node, error) {
	if n.fs == nil {
		return nil, fuse.EIO
	}
	if !n.IsDirectory() {
		return nil, fuse.EIO
	}
	newNode := &fileNode{
		fs:            n.fs,
		Name:          req.NewName,
		Mode:          os.ModeSymlink | 0755, // lrwxr-xr-x
		SymlinkTarget: req.Target,
		Nlink:         1,
	}
	if err := UpsertNode(ctx, n.fs.db, n.Inode, newNode); err != nil {
		log.Println(err)
		return nil, fuse.EIO
	}
	return newNode, nil
}

// This optional request will be called only for symbolic link nodes, and will
// be used to retrieve the target path.
// Readlink implements the fuseFS.NodeReadlinker interface.
func (n *fileNode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	if n.IsSymlink() {
		return n.SymlinkTarget, nil
	}
	return "", fuse.EIO
}

// Used to create hardlinks.
// Link implements the fuseFS.NodeLinker interface.
func (n *fileNode) Link(ctx context.Context, req *fuse.LinkRequest, old fuseFS.Node) (fuseFS.Node, error) {
	if n.fs == nil {
		return nil, fuse.EIO
	}
	if !n.IsDirectory() {
		return nil, fuse.EIO
	}
	attr := &fuse.Attr{}
	if err := old.Attr(ctx, attr); err != nil {
		log.Printf("failed to get attr of old while linking: %s\n", err)
		return nil, fuse.EIO
	}
	newNode := &fileNode{
		Inode: attr.Inode,
		Name:  req.NewName,
	}
	// TODO(imjching): Should copy all the attributes and do an upsert.
	if err := CreateLink(ctx, n.fs.db, n.Inode, newNode); err != nil {
		log.Println(err)
		return nil, fuse.EIO
	}
	var err error
	newNode, err = GetNodeByID(ctx, n.fs.db, attr.Inode)
	if err != nil {
		log.Println(err)
		return nil, fuse.EIO
	}
	newNode.Name = req.NewName
	newNode.fs = n.fs
	return newNode, nil
}

// Remove implements the fuseFS.NodeRemover interface.
func (n *fileNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if n.fs == nil {
		return fuse.EIO
	}
	toRemove, err := GetNodeByName(ctx, n.fs.db, n.Inode, req.Name)
	if err != nil {
		log.Println(err)
		return fuse.EIO
	}

	// Ensure that directory is not empty.
	if req.Dir {
		count, err := CountNodesInDir(ctx, n.fs.db, toRemove.Inode)
		if err != nil {
			log.Println(err)
			return fuse.EIO
		}
		if count > 0 {
			return fuse.Errno(syscall.ENOTEMPTY) // Directory is not empty.
		}
	}

	if err := RemoveNodeByName(ctx, n.fs.db, n.Inode, req.Name, toRemove.Inode); err != nil {
		log.Println(err)
		return fuse.EIO
	}
	return nil
}

// Searches for a file named `name` in the current fileNode directory.
// There's also another interface for Lookup:
//     Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fuseFS.Node, error)
//
// Note: Will only be called for a directory.
// Should return a fuseFS.Node based on `name`.
// Lookup implements the fuseFS.NodeStringLookuper interface.
func (n *fileNode) Lookup(ctx context.Context, name string) (fuseFS.Node, error) {
	if n.fs == nil {
		return nil, fuse.EIO
	}
	if !n.IsDirectory() {
		return nil, fuse.EIO
	}

	lookupNode, err := GetNodeByName(ctx, n.fs.db, n.Inode, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	lookupNode.fs = n.fs

	// TODO(imjching): When returning fuseFS.Node, return the same instance.
	// Will need to somewhat cache fileNode on first create to avoid spurious
	// cache invalidation. Perhaps this is linked to the Forget() call.
	return lookupNode, nil
}

// Mkdir implements the fuseFS.NodeMkdirer interface.
func (n *fileNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fuseFS.Node, error) {
	if n.fs == nil {
		return nil, fuse.EIO
	}
	// req.Umask is not supported on OSX.
	// See https://github.com/bazil/fuse/blob/65cc252bf6691cb3c7014bcb2c8dc29de91e3a7e/fuse.go#L1704-L1711.
	newNode := &fileNode{
		fs:   n.fs,
		Name: req.Name,
		Mode: req.Mode,
		// New directories have no entries in it except . and ..
		Nlink: 2,
	}
	if err := UpsertNode(ctx, n.fs.db, n.Inode, newNode); err != nil {
		log.Println(err)
		return nil, fuse.EIO
	}
	return newNode, nil
}

// Note: Will only be called for a file.
// Asks to create and open a file (not a directory).
// Create implements the fuseFS.NodeCreater interface.
func (n *fileNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fuseFS.Node, fuseFS.Handle, error) {
	if n.fs == nil {
		return nil, nil, fuse.EIO
	}
	// TODO(imjching): req.Flags corresponds to OpenFlags. Maybe this is useful
	// for caching / in-memory buffer. Note that Fsync will be called before
	// file system closes.
	newNode := &fileNode{
		fs:    n.fs,
		Name:  req.Name,
		Mode:  req.Mode,
		Nlink: 1,
	}
	if err := UpsertNode(ctx, n.fs.db, n.Inode, newNode); err != nil {
		log.Println(err)
		// If we send back ENOSYS, FUSE will try mknod+open.
		return nil, nil, fuse.EIO
	}
	return newNode, newNode, nil
}

// Rename implements the fuseFS.NodeRenamer interface.
func (n *fileNode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fuseFS.Node) error {
	if n.fs == nil {
		return fuse.EIO
	}
	attr := &fuse.Attr{}
	if err := newDir.Attr(ctx, attr); err != nil {
		log.Printf("failed to get attr of newDir while renaming: %s\n", err)
		return fuse.EIO
	}
	if err := RenameNode(ctx, n.fs.db, n.Inode, req.OldName, attr.Inode, req.NewName); err != nil {
		log.Println(err)
		return fuse.EIO
	}
	return nil
}

// Mknod creates a node (file, device special, or named pipe. However, read
// and write to these special files are not allowed in FUSE.
//
// Mknod implements the fuseFS.NodeMknoder interface.
func (n *fileNode) Mknod(ctx context.Context, req *fuse.MknodRequest) (fuseFS.Node, error) {
	if n.fs == nil {
		return nil, fuse.EIO
	}
	// req.Rdev // desired device number if type is device.
	newNode := &fileNode{
		fs:    n.fs,
		Name:  req.Name,
		Mode:  req.Mode,
		Nlink: 1,
	}
	if err := UpsertNode(ctx, n.fs.db, n.Inode, newNode); err != nil {
		log.Println(err)
		return nil, fuse.EIO
	}
	return newNode, nil
}

// Used to list available files in a directory.
// Note: Will only be called for a directory.
// ReadDirAll implements the fuseFS.HandleReadDirAller interface.
func (n *fileNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if n.fs == nil {
		return nil, fuse.EIO
	}
	nodes, err := ListNodesInDir(ctx, n.fs.db, n.Inode)
	if err != nil {
		log.Println(err)
		return nil, fuse.EIO
	}
	var entries []fuse.Dirent
	for _, node := range nodes {
		dirent := fuse.Dirent{
			Inode: node.Inode,
			Name:  node.Name,

			// Provide a type to speed up operations and avoid additional Getattr calls.
			Type: GetDirentTypeFromMode(node.Mode),
		}
		entries = append(entries, dirent)
	}
	return entries, nil
}

func GetDirentTypeFromMode(mode os.FileMode) fuse.DirentType {
	// See list of available types here:
	// https://github.com/bazil/fuse/blob/65cc252bf6691cb3c7014bcb2c8dc29de91e3a7e/fuse.go#L1868-L1886
	// DT_Socket: socket
	// DT_Link: symlink
	// DT_File: regular file
	// DT_Bloc: block device
	// DT_Dir: directory
	// DT_Char: char device
	// DT_FIFO: named pipes
	if mode&os.ModeDir != 0 {
		return fuse.DT_Dir
	}
	if mode&os.ModeSymlink != 0 {
		return fuse.DT_Link
	}
	if mode&os.ModeDevice != 0 {
		return fuse.DT_Block
	}
	if mode&os.ModeCharDevice != 0 {
		return fuse.DT_Char
	}
	if mode&os.ModeNamedPipe != 0 {
		return fuse.DT_FIFO
	}
	if mode&os.ModeSocket != 0 {
		return fuse.DT_Socket
	}
	if mode&os.ModeType == 0 { // Same as IsRegular
		return fuse.DT_File
	}
	return fuse.DT_Unknown
}

// Read is called whenever kernel attempt to read contents of the file.
// req.Offset corresponds to bytes that are already read, and req.Size
// corresponds to remaining bytes left to read. If we return a byte slice
// that has length less than req.Size, Read will be called again by FUSE,
// but with an updated offset.
//
// There is also a `ReadAll` method presumely supposed to read all the bytes
// at once (?): ReadAll(ctx context.Context) ([]byte, error)
//
// TODO(imjching): Look into req.Flags and req.FileFlags. Concurrency?
//
// Read implements the fuseFS.HandleReader interface.
func (n *fileNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// Read everything. This is problematic when it comes to large file sizes.
	data, err := ReadData(ctx, n.fs.db, n.Inode)
	if err != nil {
		log.Println(err)
		return fuse.EIO
	}
	fuseutil.HandleRead(req, resp, data)
	return nil
}

// Write requests to write data into the handle at the given offset.
// Store the amount of data written in resp.Size.
//
// Writes that grow the file are expected to update the file size
// (as seen through Attr). Note that file size changes are
// communicated also through Setattr.
// Write implements the fuseFS.HandleWriter interface.
func (n *fileNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// We will attempt to write everything here.
	err := WriteData(ctx, n.fs.db, n, req.Data)
	if err != nil {
		log.Println(err)
		return fuse.EIO
	}
	resp.Size = len(req.Data)
	return nil
}
