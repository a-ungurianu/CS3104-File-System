
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>

#include "myfs.h"

unqlite* pDb;

FileControlBlock root_directory;

DirectoryEntry emptyDirectory[MAX_DIRECTORY_ENTRIES];

static void setTimespecToNow(struct timespec* tm) {
    struct timespec now;
    timespec_get(&now, TIME_UTC);

    memcpy(tm, &now, sizeof(now));
}

static void createDirectoryNode(FileControlBlock* blockToFill) {
    blockToFill->st_mode = DEFAULT_DIR_MODE;

    setTimespecToNow(&blockToFill->st_ctim);
    setTimespecToNow(&blockToFill->st_atim);
    setTimespecToNow(&blockToFill->st_mtim);

    struct fuse_context* ctx = fuse_get_context();

    blockToFill->user_id = ctx->uid;
    blockToFill->group_id = ctx->gid;

    uuid_t directoryDataBlockUUID;
    uuid_generate(directoryDataBlockUUID);

    int rc = unqlite_kv_store(pDb, directoryDataBlockUUID, KEY_SIZE, emptyDirectory, sizeof emptyDirectory);
    error_handler(rc);

    blockToFill->size = sizeof(emptyDirectory);

    memcpy(&blockToFill->data_ref, &directoryDataBlockUUID, sizeof directoryDataBlockUUID);
}

static void init_fs() {
    int rc = unqlite_open(&pDb,DATABASE_NAME,UNQLITE_OPEN_CREATE);
    if( rc != UNQLITE_OK ) error_handler(rc);
    
    unqlite_int64 bytesRead;

    rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &root_directory, &bytesRead);

    if(rc == UNQLITE_NOTFOUND) {
        perror("Root of filesystem not found. Creating it...\n");

        createDirectoryNode(&root_directory);

        unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &root_directory, sizeof root_directory);
    }
    else {
        perror("Root of filesystem found. Using it as the root folder...\n");
        error_handler(rc);

        if(bytesRead != sizeof root_directory) {
            perror("!!! Database is corrupted, exiting...\n");
            exit(-1);
        }
    }
}

// The functions which follow are handler functions for various things a filesystem needs to do:
// reading, getting attributes, truncating, etc. They will be called by FUSE whenever it needs
// your filesystem to do something, so this is where functionality goes.

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
static int myfs_getattr(const char *path, struct stat *stbuf) {
    write_log("myfs_getattr(path=\"%s\")\n", path);

    if(strcmp(path,"/") == 0) {
        stbuf->st_mode = DEFAULT_DIR_MODE;	/* File mode.  */
        stbuf->st_nlink = 2;	/* Link count.  */
        stbuf->st_uid = root_directory.user_id;		/* User ID of the file's owner.  */
        stbuf->st_gid = root_directory.group_id;		/* Group ID of the file's group. */
        stbuf->st_size = root_directory.size;	/* Size of file, in bytes.  */
        stbuf->st_atime = root_directory.st_atim.tv_sec;	/* Time of last access.  */
        stbuf->st_mtime = root_directory.st_mtim.tv_sec;	/* Time of last modification.  */
        stbuf->st_ctime = root_directory.st_ctim.tv_sec;	/* Time of last status change.  */
        return 0;
    }
    
    write_log("myfs_getattr(path=\"%s\"): Path not found\n", path);    
    return -ENOENT;
}

// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    write_log("myfs_readdir(path=\"%s\")\n", path);
    return -ENOENT;
}

// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    write_log("mtfs_read(path=\"%s\", size=%zu, offset=%zu", path, size, offset);

    return -ENOENT;
}

// This file system only supports one file. Create should fail if a file has been created. Path must be '/<something>'.
// Read 'man 2 creat'.
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi){   
    write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);
    
    return -ENOENT;
}

// Set update the times (actime, modtime) for a file. This FS only supports modtime.
// Read 'man 2 utime'.
static int myfs_utime(const char *path, struct utimbuf *ubuf){
    write_log("myfs_utime(path=\"%s\")\n", path, ubuf);
    
    return -ENOENT;
}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){   
    write_log("myfs_write(path=\"%s\", size=%d, offset=%lld,)\n", path, size, offset);
    
    return -ENOENT;
}

// Set the size of a file.
// Read 'man 2 truncate'.
int myfs_truncate(const char *path, off_t newsize){    
    write_log("myfs_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);
    
    return -ENOENT;
}

// Set permissions.
// Read 'man 2 chmod'.
int myfs_chmod(const char *path, mode_t mode){
    write_log("myfs_chmod(path=\"%s\", mode=0%03o)\n", path, mode);
    
    return -ENOENT;
}

// Set ownership.
// Read 'man 2 chown'.
int myfs_chown(const char *path, uid_t uid, gid_t gid){   
    write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
    
    return -ENOENT;
}

// Create a directory.
// Read 'man 2 mkdir'.
int myfs_mkdir(const char *path, mode_t mode){
    write_log("myfs_mkdir(path=\"%s\", mode=0%03o)\n", path, mode);	
    
    return -ENOENT;
}

// Delete a file.
// Read 'man 2 unlink'.
int myfs_unlink(const char *path){
    write_log("myfs_unlink(path=\"%s\")\n",path);	
    
    return -ENOENT;
}

// Delete a directory.
// Read 'man 2 rmdir'.
int myfs_rmdir(const char *path){
    write_log("myfs_rmdir(path=\"%s\")\n",path);	
    
    return 0;
}

// OPTIONAL - included as an example
// Flush any cached data.
int myfs_flush(const char *path, struct fuse_file_info *fi){
    write_log("myfs_flush(path=\"%s\")\n", path, fi);
    
    return -ENOENT;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
int myfs_release(const char *path, struct fuse_file_info *fi){
    write_log("myfs_release(path=\"%s\")\n", path);
    
    return -ENOENT;
}

// OPTIONAL - included as an example
// Open a file. Open should check if the operation is permitted for the given flags (fi->flags).
// Read 'man 2 open'.
static int myfs_open(const char *path, struct fuse_file_info *fi){  
    write_log("myfs_open(path\"%s\")\n", path);
    
    return -ENOENT;
}

// This struct contains pointers to all the functions defined above
// It is used to pass the function pointers to fuse
// fuse will then execute the methods as required 
static struct fuse_operations myfs_oper = {
    .getattr	= myfs_getattr,
    .readdir	= myfs_readdir,
    .open		= myfs_open,
    .read		= myfs_read,
    .create		= myfs_create,
    .utime 		= myfs_utime,
    .write		= myfs_write,
    .truncate	= myfs_truncate,
    .flush		= myfs_flush,
    .release	= myfs_release,
};

void shutdown_fs() {
	unqlite_close(pDb);
}

int main(int argc, char *argv[]){	
	int fuserc;
	struct myfs_state *myfs_internal_state;

	//Setup the log file and store the FILE* in the private data object for the file system.	
	myfs_internal_state = malloc(sizeof(struct myfs_state));
    myfs_internal_state->logfile = init_log_file();
	
	//Initialise the file system. This is being done outside of fuse for ease of debugging.
	init_fs();
		
    // Now pass our function pointers over to FUSE, so they can be called whenever someone
    // tries to interact with our filesystem. The internal state contains a file handle
    // for the logging mechanism
	fuserc = fuse_main(argc, argv, &myfs_oper, myfs_internal_state);
	
	//Shutdown the file system.
	shutdown_fs();
	
	return fuserc;
}

