
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <libgen.h>

#include "myfs.h"

unqlite* pDb;

FileControlBlock root_directory;

DirectoryDataBlock emptyDirectory;

static void setTimespecToNow(struct timespec* tm) {
    struct timespec now;
    timespec_get(&now, TIME_UTC);

    memcpy(tm, &now, sizeof(now));
}

static void createDirectoryNode(FileControlBlock* blockToFill, mode_t mode) {
    blockToFill->mode = S_IFDIR | mode;

    setTimespecToNow(&blockToFill->st_ctim);
    setTimespecToNow(&blockToFill->st_atim);
    setTimespecToNow(&blockToFill->st_mtim);

    blockToFill->user_id = getuid();
    blockToFill->group_id = getgid();

    uuid_t directoryDataBlockUUID = {0};
    uuid_generate(directoryDataBlockUUID);

    int rc = unqlite_kv_store(pDb, directoryDataBlockUUID, KEY_SIZE, &emptyDirectory, sizeof emptyDirectory);
    error_handler(rc);

    blockToFill->size = sizeof(emptyDirectory);

    memcpy(&blockToFill->data_ref, &directoryDataBlockUUID, sizeof directoryDataBlockUUID);
}

static int addFCBToDirectory(FileControlBlock* dirBlock, const char* name, uuid_t fcb_ref) {
    if(strlen(name) >= MAX_FILENAME_SIZE) return -ENAMETOOLONG;

    if(dirBlock->mode & S_IFDIR) {
        DirectoryDataBlock entries;
        unqlite_int64 bytesRead = sizeof entries;
        int rc = unqlite_kv_fetch(pDb, dirBlock->data_ref, KEY_SIZE, &entries, &bytesRead);

        error_handler(rc);

        if(bytesRead != sizeof entries) {
            write_log("Directory data block is corrupted. Exiting...\n");
            exit(-1);
        }

        if(entries.usedEntries == MAX_DIRECTORY_ENTRIES) {
            return -ENOSPC;
        }

        for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
            if(strcmp(entries.entries[i].name,name) == 0) {
                return -EEXIST;
            }
        }

        for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
            if(strcmp(entries.entries[i].name,"") == 0) {
                strcpy(entries.entries[i].name, name);
                uuid_copy(entries.entries[i].fcb_ref, fcb_ref);

                entries.usedEntries += 1;
                rc = unqlite_kv_store(pDb, dirBlock->data_ref, KEY_SIZE, &entries, sizeof entries);
                error_handler(rc);
                return 0;
            }
        }

        write_log("Directory size says it has room, but it doesn't...");
        return -ENOSPC;
    }

    return -ENOTDIR;
}

static int getFCBInDirectory(const FileControlBlock* dirBlock, const char* name, FileControlBlock* toFill, uuid_t *uuidToFill) {
    if(strlen(name) >= MAX_FILENAME_SIZE) return -ENAMETOOLONG;
    
    if(dirBlock->mode & S_IFDIR) {
        DirectoryDataBlock entries;
        unqlite_int64 bytesRead = sizeof entries;
        int rc = unqlite_kv_fetch(pDb, dirBlock->data_ref, KEY_SIZE, &entries, &bytesRead);

        error_handler(rc);

        if(bytesRead != sizeof entries) {
            write_log("Directory data block is corrupted. Exiting...\n");
            exit(-1);
        }

        if(entries.usedEntries == 0) {
            return -ENOENT;
        }

        for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
            if(strcmp(entries.entries[i].name, name) == 0) {
                bytesRead = sizeof(FileControlBlock);
                rc = unqlite_kv_fetch(pDb, entries.entries[i].fcb_ref, KEY_SIZE, toFill, &bytesRead);

                error_handler(rc);

                if(uuidToFill != NULL) {
                    uuid_copy(*uuidToFill,entries.entries[i].fcb_ref);
                }
                
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

static int getFCBAtPath(const char* path, FileControlBlock* toFill, uuid_t *uuidToFill) {
    char* pathCopy = strdup(path);
    
    char* savePtr;

    char* p = strtok_r(pathCopy,"/",&savePtr);

    FileControlBlock currentDir = root_directory;

    while(p != NULL) {

        int rc = getFCBInDirectory(&currentDir,p,&currentDir, uuidToFill);
        if(rc != 0) return rc;

        p = strtok_r(NULL, "/",&savePtr);
    }

    free(pathCopy);

    memcpy(toFill, &currentDir, sizeof currentDir);

    return 0;
}

static int removeFCBInDirectory(const FileControlBlock* dirBlock, const char* name) {
    if(dirBlock->mode & S_IFDIR) {
        DirectoryDataBlock entries;
        unqlite_int64 bytesRead = sizeof entries;

        int rc = unqlite_kv_fetch(pDb, dirBlock->data_ref, KEY_SIZE, &entries, &bytesRead);
        error_handler(rc);

        if(bytesRead != sizeof entries) {
            write_log("Directory data block is corrupted. Exiting...\n");
            exit(-1);
        }

        if(entries.usedEntries == 0) return -ENOENT;

        for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
            if(strcmp(entries.entries[i].name, name) == 0) {
                uuid_t fcb_uuid;
                FileControlBlock fcb;
                memcpy(&fcb_uuid, entries.entries[i].fcb_ref, sizeof(uuid_t));

                bytesRead = sizeof(fcb);

                rc = unqlite_kv_fetch(pDb, fcb_uuid, KEY_SIZE, &fcb, &bytesRead);
                error_handler(rc);
                
                if(fcb.mode & S_IFDIR) {
                    
                    DirectoryDataBlock childEntries;

                    bytesRead = sizeof(childEntries);
                    rc = unqlite_kv_fetch(pDb, fcb.data_ref, KEY_SIZE, &childEntries, &bytesRead);
                    error_handler(rc);

                    if(bytesRead != sizeof childEntries) {
                        write_log("Directory data block is corrupted. Exiting...\n");
                        exit(-1);
                    }

                    if(childEntries.usedEntries != 0) {
                        return -ENOTEMPTY;
                    }

                    unqlite_kv_delete(pDb, fcb.data_ref, KEY_SIZE);
                    unqlite_kv_delete(pDb, fcb_uuid,     KEY_SIZE);

                    memset(&entries.entries[i], 0, sizeof(entries.entries[i]));

                    entries.usedEntries -= 1;
                    rc = unqlite_kv_store(pDb, dirBlock->data_ref, KEY_SIZE, &entries, sizeof entries);
                    error_handler(rc);
                    
                    return 0;
                }
                else {
                    return -ENOTDIR;
                }

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

        createDirectoryNode(&root_directory, DEFAULT_DIR_MODE);

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

    FileControlBlock currentDirectory;

    int rc = getFCBAtPath(path, &currentDirectory, NULL);

    if(rc != 0) return rc;

    stbuf->st_mode  = currentDirectory.mode;            /* File mode.  */
    stbuf->st_nlink = 2;                                /* Link count.  */
    stbuf->st_uid   = currentDirectory.user_id;		    /* User ID of the file's owner.  */
    stbuf->st_gid   = currentDirectory.group_id;        /* Group ID of the file's group. */
    stbuf->st_size  = currentDirectory.size;            /* Size of file, in bytes.  */
    stbuf->st_atime = currentDirectory.st_atim.tv_sec;	/* Time of last access.  */
    stbuf->st_mtime = currentDirectory.st_mtim.tv_sec;	/* Time of last modification.  */
    stbuf->st_ctime = currentDirectory.st_ctim.tv_sec;	/* Time of last status change.  */

    return 0;
}

// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    write_log("myfs_readdir(path=\"%s\")\n", path);

    FileControlBlock currentDirectory;

    int rc = getFCBAtPath(path, &currentDirectory, NULL);
    if(rc != 0) return rc;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    DirectoryDataBlock entries;

    unqlite_int64 bytesRead = sizeof entries;
    rc = unqlite_kv_fetch(pDb, currentDirectory.data_ref, KEY_SIZE, &entries, &bytesRead);

    error_handler(rc);

    for(int i = 0; i < MAX_DIRECTORY_ENTRIES; ++i) {
        if(strcmp(entries.entries[i].name,"") != 0) {
            filler(buf, entries.entries[i].name, NULL, 0);
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
    
    char* basenameCopy = strdup(path);
    char* dirnameCopy = strdup(path);

    char* name = basename(basenameCopy);
    
    if(strlen(name) >= MAX_FILENAME_SIZE) {
        free(basenameCopy);
        free(dirnameCopy);
        return -ENAMETOOLONG;
    }
    
    char* pathToDir = dirname(dirnameCopy);
    
    FileControlBlock currentDir;

    int rc = getFCBAtPath(pathToDir, &currentDir, NULL);

    if(rc != 0){ 
        free(basenameCopy);
        free(dirnameCopy);
        return rc;
    }

    FileControlBlock newDirectory;
    createDirectoryNode(&newDirectory, mode);

    uuid_t newDirectoryRef = {0};

    uuid_generate(newDirectoryRef);

    rc = unqlite_kv_store(pDb,newDirectoryRef, KEY_SIZE, &newDirectory, sizeof newDirectory);

    error_handler(rc);

    rc = addFCBToDirectory(&currentDir,name,newDirectoryRef);

    // TODO: Add error handling for when the name is already used. Currently, the DB is populated with something that has no 
    // reference.

    free(basenameCopy);
    free(dirnameCopy);
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
    
    char* pathCopy = strdup(path);
    char* pathCopy2 = strdup(path);

    char* name = basename(pathCopy);
    char* pathToDir = dirname(pathCopy2);

    FileControlBlock parentDir;

    int rc = getFCBAtPath(pathToDir, &parentDir, NULL);

    if(rc != 0) {
        free(pathCopy);
        free(pathCopy2);
        return rc;
    }

    rc = removeFCBInDirectory(&parentDir,name);

    free(pathCopy);
    free(pathCopy2);
    return rc;
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
    .rmdir      = myfs_rmdir,
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

