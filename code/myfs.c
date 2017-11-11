
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <libgen.h>

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
    blockToFill->mode = DEFAULT_DIR_MODE;

    setTimespecToNow(&blockToFill->st_ctim);
    setTimespecToNow(&blockToFill->st_atim);
    setTimespecToNow(&blockToFill->st_mtim);

    blockToFill->user_id = getuid();
    blockToFill->group_id = getgid();

    uuid_t directoryDataBlockUUID;
    uuid_generate(directoryDataBlockUUID);

    int rc = unqlite_kv_store(pDb, directoryDataBlockUUID, KEY_SIZE, emptyDirectory, sizeof emptyDirectory);
    error_handler(rc);

    blockToFill->size = sizeof(emptyDirectory);

    memcpy(&blockToFill->data_ref, &directoryDataBlockUUID, sizeof directoryDataBlockUUID);
}

static int addFCBToDirectory(FileControlBlock* dirBlock, const char* name, uuid_t fcb_ref) {
    if(dirBlock->mode & S_IFDIR) {
        DirectoryEntry entries[MAX_DIRECTORY_ENTRIES];
        unqlite_int64 bytesRead = sizeof entries;
        int rc = unqlite_kv_fetch(pDb, dirBlock->data_ref, KEY_SIZE, entries, &bytesRead);

        error_handler(rc);

        if(bytesRead != sizeof entries) {
            write_log("Directory data block is corrupted. Exiting...\n");
            exit(-1);
        }

        for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
            if(strcmp(entries[i].name,name) == 0) {
                return -EEXIST;
            }
        }

        for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
            if(strcmp(entries[i].name,"") == 0) {
                strcpy(entries[i].name, name);
                uuid_copy(entries[i].fcb_ref, fcb_ref);

                rc = unqlite_kv_store(pDb, dirBlock->data_ref, KEY_SIZE, entries, sizeof entries);
                error_handler(rc);
                return 0;
            }
        }

        return -ENOSPC;
    }

    return -ENOTDIR;
}

static int getFCBInDirectory(const FileControlBlock* dirBlock, const char* name, FileControlBlock* toFill) {
    if(dirBlock->mode & S_IFDIR) {
        DirectoryEntry entries[MAX_DIRECTORY_ENTRIES];
        unqlite_int64 bytesRead = sizeof entries;
        int rc = unqlite_kv_fetch(pDb, dirBlock->data_ref, KEY_SIZE, entries, &bytesRead);

        error_handler(rc);

        if(bytesRead != sizeof entries) {
            write_log("Directory data block is corrupted. Exiting...\n");
            exit(-1);
        }

        for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
            if(strcmp(entries[i].name, name) == 0) {
                bytesRead = sizeof(FileControlBlock);
                rc = unqlite_kv_fetch(pDb, entries[i].fcb_ref, KEY_SIZE, toFill, &bytesRead);

                error_handler(rc);
                
                if(bytesRead != sizeof(FileControlBlock)) {
                    write_log("FCB is corrupted. Exiting...\n");
                    exit(-1);
                }

                return 0;
            }
        }
        return -ENOENT;
    }
    return -ENOTDIR;
}

static void init_fs() {
    int rc = unqlite_open(&pDb,DATABASE_NAME,UNQLITE_OPEN_CREATE);
    if( rc != UNQLITE_OK ) error_handler(rc);
    
    unqlite_int64 bytesRead = sizeof root_directory;

    rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &root_directory, &bytesRead);

    if(rc == UNQLITE_NOTFOUND) {
        perror("Root of filesystem not found. Creating it...\n");

        createDirectoryNode(&root_directory);

        rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &root_directory, sizeof root_directory);

        error_handler(rc);
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

    char* pathCopy = strdup(path);
    
    char* savePtr;

    char* p = strtok_r(pathCopy,"/",&savePtr);

    FileControlBlock currentDir = root_directory;

    write_log("\tPath: [");
    while(p != NULL) {
        write_log("%s,",p);

        int rc = getFCBInDirectory(&currentDir,p,&currentDir);
        if(rc != 0) return rc;

        p = strtok_r(NULL, "/",&savePtr);
    }
    write_log("\b]\n");

    free(pathCopy);

    stbuf->st_mode  = currentDir.mode;	               /* File mode.  */
    stbuf->st_nlink = 2;	                            /* Link count.  */
    stbuf->st_uid   = currentDir.user_id;		    /* User ID of the file's owner.  */
    stbuf->st_gid   = currentDir.group_id;		    /* Group ID of the file's group. */
    stbuf->st_size  = currentDir.size;	            /* Size of file, in bytes.  */
    stbuf->st_atime = currentDir.st_atim.tv_sec;	/* Time of last access.  */
    stbuf->st_mtime = currentDir.st_mtim.tv_sec;	/* Time of last modification.  */
    stbuf->st_ctime = currentDir.st_ctim.tv_sec;	/* Time of last status change.  */

    return 0;
}

// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    write_log("myfs_readdir(path=\"%s\")\n", path);

    FileControlBlock currentDirectory = root_directory;

    char* pathCopy = strdup(path);
    
    char* savePtr;
    
    char* p = strtok_r(pathCopy,"/", &savePtr);

    FileControlBlock currentDir = root_directory;

    write_log("\tPath: [");
    while(p != NULL) {
        write_log("%s,",p);

        int rc = getFCBInDirectory(&currentDirectory,p,&currentDirectory);
        if(rc != 0) return rc;

        p = strtok_r(NULL, "/", &savePtr);
    }
    write_log("\b]\n");

    free(pathCopy);

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    DirectoryEntry entries[MAX_DIRECTORY_ENTRIES];

    unqlite_int64 bytesRead = sizeof entries;
    int rc = unqlite_kv_fetch(pDb, currentDirectory.data_ref, KEY_SIZE, entries, &bytesRead);

    error_handler(rc);

    for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
        if(strcmp(entries[i].name,"") != 0) {
            filler(buf, entries[i].name, NULL, 0);
        }
    }
    
    return 0;
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
    
    char* pathCopy = strdup(path);
    char* pathCopy2 = strdup(path);

    char* name = basename(pathCopy);
    char* pathToDir = dirname(pathCopy2);
    char* p = strtok(pathToDir,"/");

    FileControlBlock currentDir = root_directory;

    write_log("\tPath: [");
    while(p != NULL) {
        write_log("%s,",p);
        int rc = getFCBInDirectory(&currentDir, p, &currentDir);

        if(rc != 0) return rc;

        p = strtok(NULL, "/");
    }
    write_log("\b]\n");


    FileControlBlock newDirectory;
    createDirectoryNode(&newDirectory);

    uuid_t newDirectoryRef;

    uuid_generate(newDirectoryRef);

    int rc = unqlite_kv_store(pDb,newDirectoryRef, KEY_SIZE, &newDirectory, sizeof newDirectory);

    error_handler(rc);

    rc = addFCBToDirectory(&currentDir,name,newDirectoryRef);

    free(pathCopy);
    free(pathCopy2);
    return rc;
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
    .mkdir      = myfs_mkdir,
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

