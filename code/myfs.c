
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <libgen.h>

#include "myfs.h"

#define MAX_INDIRECTION_LEVEL 1

typedef struct {
    FileControlBlock *fcb;
    size_t idx;
    size_t level;
    int extends; // set to not 0 if this should allocate blocks that it doesn't find
} FCBBlockIterator;

static FCBBlockIterator makeBlockIterator(FileControlBlock* fcb, int extends) {
    FCBBlockIterator result = {
        .fcb = fcb,
        .idx = 0,
        .level = 0,
        .extends = extends
    };

    return result;
}

static void* getNextBlock(FCBBlockIterator* iterator, void* block, size_t blockSize, uuid_t* blockUUID) {
    uuid_t toFetch = {0};

    if(iterator->level > MAX_INDIRECTION_LEVEL) return NULL;

    switch(iterator->level) {
        case 0: {
            if(iterator->extends && uuid_compare(iterator->fcb->data_blocks[iterator->idx], zero_uuid) == 0) {
                uuid_generate(toFetch);
                char* nothing = calloc(sizeof(char), blockSize);
                int rc = unqlite_kv_store(pDb, toFetch, KEY_SIZE, nothing, (ssize_t) blockSize);
                free(nothing);
                error_handler(rc);
                uuid_copy(iterator->fcb->data_blocks[iterator->idx], toFetch);
            }
            else {
                uuid_copy(toFetch, iterator->fcb->data_blocks[iterator->idx]);
            }

            iterator->idx += 1;
            if(iterator->idx >= NO_DIRECT_BLOCKS) {
                iterator->idx = 0;
                iterator->level += 1;
            }
        } break;
        case 1: {

            uuid_t blocks[UUIDS_PER_BLOCK] = {{0}};
            if(uuid_compare(iterator->fcb->indirectBlock, zero_uuid) == 0)  {
                if(iterator->extends) {
                    uuid_generate(iterator->fcb->indirectBlock);
                    int rc = unqlite_kv_store(pDb, iterator->fcb->indirectBlock, KEY_SIZE, &blocks, sizeof(uuid_t) * UUIDS_PER_BLOCK);
                    error_handler(rc);
                }
                else {
                    return NULL;
                }
            }
            unqlite_int64 nBytes = UUIDS_PER_BLOCK * sizeof(uuid_t);
            int rc = unqlite_kv_fetch(pDb, iterator->fcb->indirectBlock, KEY_SIZE, *blocks, &nBytes);
            error_handler(rc);

            if(iterator->extends && uuid_compare(blocks[iterator->idx], zero_uuid) == 0) {
                uuid_generate(toFetch);
                char* nothing = calloc(sizeof(char), blockSize);
                rc = unqlite_kv_store(pDb, toFetch, KEY_SIZE, nothing, (ssize_t) blockSize);
                free(nothing);
                error_handler(rc);
                uuid_copy(blocks[iterator->idx], toFetch); 
                rc = unqlite_kv_store(pDb, iterator->fcb->indirectBlock, KEY_SIZE, *blocks, UUIDS_PER_BLOCK * sizeof(uuid_t));
                error_handler(rc);
            }
            else {
                uuid_copy(toFetch, blocks[iterator->idx]);
            }

            iterator->idx += 1;

            if(iterator->idx >= UUIDS_PER_BLOCK) return NULL;
        } break;
        default: return NULL;
    }

    if(uuid_compare(toFetch, zero_uuid) == 0) return NULL;

    if(blockUUID != NULL) {
        uuid_copy(*blockUUID, toFetch);
    }

    unqlite_int64 nBytes = (unqlite_int64) blockSize;
    int rc = unqlite_kv_fetch(pDb, toFetch, KEY_SIZE, block, &nBytes);
    error_handler(rc);

    return block;
}

typedef struct {
    char* parent;
    char* name;
    char* savedPtrs[2];
} SplitPath;

unqlite* pDb;

uuid_t zero_uuid;

DirectoryDataBlock emptyDirectory;
FileDataBlock emptyFile;

static ssize_t min(ssize_t a, ssize_t b) {
    if(a < b) return a;
    return b;
}

static ssize_t max(ssize_t a, ssize_t b) {
    if(a > b) return a;
    return b;
}

static SplitPath splitPath(const char* path) {
    char* basenameCopy = strdup(path);
    char* dirnameCopy = strdup(path);

    char* bn = basename(basenameCopy);
    char* dn = dirname(dirnameCopy);

    SplitPath result = {
        .parent = dn,
        .name = bn,
        .savedPtrs = {dirnameCopy, basenameCopy}
    };

    return result;
}

static void freeSplitPath(SplitPath* path) {
    free(path->savedPtrs[0]);
    free(path->savedPtrs[1]);
}

static void setTimespecToNow(struct timespec* tm) {
    struct timespec now;
    timespec_get(&now, TIME_UTC);

    memcpy(tm, &now, sizeof(now));
}

static int fetchRootFCB(FileControlBlock *rootBlock) {
    
    unqlite_int64 nBytes = sizeof(FileControlBlock);
    int rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, KEY_SIZE, rootBlock, &nBytes);
    error_handler(rc);
    return 0;
}

static void createDirectoryNode(FileControlBlock* blockToFill, mode_t mode) {
    memset(blockToFill, 0, sizeof(FileControlBlock));
    blockToFill->mode = S_IFDIR | mode;

    setTimespecToNow(&blockToFill->st_ctim);
    setTimespecToNow(&blockToFill->st_atim);
    setTimespecToNow(&blockToFill->st_mtim);

    blockToFill->user_id = getuid();
    blockToFill->group_id = getgid();
}

static void createFileNode(FileControlBlock* blockToFill, mode_t mode) {
    memset(blockToFill, 0, sizeof(FileControlBlock));
    blockToFill->mode = S_IFREG | mode;

    setTimespecToNow(&blockToFill->st_ctim);
    setTimespecToNow(&blockToFill->st_atim);
    setTimespecToNow(&blockToFill->st_mtim);

    blockToFill->user_id = getuid();
    blockToFill->group_id = getgid();

}

static int addFCBToDirectory(FileControlBlock* dirBlock, const char* name, const uuid_t fcb_ref) {
    if(strlen(name) >= MAX_FILENAME_SIZE) return -ENAMETOOLONG;

    if(S_ISDIR(dirBlock->mode)) {
        FCBBlockIterator iter = makeBlockIterator(dirBlock, 1);
        DirectoryDataBlock entries;

        uuid_t blockUUID;

        while(getNextBlock(&iter, &entries, sizeof(entries), &blockUUID) != NULL) {
            if(entries.usedEntries == DIRECTORY_ENTRIES_PER_BLOCK) {
                continue;
            }

            for(int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i) {
                if(strcmp(entries.entries[i].name,name) == 0) {
                    return -EEXIST;
                }
            }

            for(int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i) {
                if(strcmp(entries.entries[i].name,"") == 0) {
                    strcpy(entries.entries[i].name, name);
                    uuid_copy(entries.entries[i].fcb_ref, fcb_ref);

                    entries.usedEntries += 1;
                    int rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                    error_handler(rc);
                    return 0;
                }
            }
        }

        return -ENOSPC;
    }

    return -ENOTDIR;
}

static int getFCBInDirectory(const FileControlBlock* dirBlock, const char* name, FileControlBlock* toFill, uuid_t *uuidToFill) {
    if(strlen(name) >= MAX_FILENAME_SIZE) return -ENAMETOOLONG;
    
    if(S_ISDIR(dirBlock->mode)) {
        FCBBlockIterator iter = makeBlockIterator(dirBlock, 0);
        DirectoryDataBlock entries;

        uuid_t blockUUID;

        while(getNextBlock(&iter, &entries, sizeof(entries), &blockUUID) != NULL) {
            
            for(int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i) {
                if(strcmp(entries.entries[i].name, name) == 0) {
                    unqlite_int64 bytesRead = sizeof(FileControlBlock);
                    int rc = unqlite_kv_fetch(pDb, entries.entries[i].fcb_ref, KEY_SIZE, toFill, &bytesRead);

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
        }
        return -ENOENT;
    }
    return -ENOTDIR;
}

static int getFCBAtPath(const char* path, FileControlBlock* toFill, uuid_t *uuidToFill) { 
    
    char* pathCopy = strdup(path);
    
    char* savePtr;

    char* p = strtok_r(pathCopy,"/",&savePtr);

    if(uuidToFill != NULL) {
        uuid_copy(*uuidToFill, ROOT_OBJECT_KEY);
    }
    FileControlBlock currentDir;
    fetchRootFCB(&currentDir);

    while(p != NULL) {

        int rc = getFCBInDirectory(&currentDir,p,&currentDir, uuidToFill);
        if(rc != 0) return rc;

        p = strtok_r(NULL, "/",&savePtr);
    }

    free(pathCopy);

    memcpy(toFill, &currentDir, sizeof currentDir);

    return 0;
}


static void removeNodeData(FileControlBlock *fcb) {
    char fakeBlock[BLOCK_SIZE];
    uuid_t blockUUID;

    FCBBlockIterator iter = makeBlockIterator(fcb, 0);

    while(getNextBlock(&iter, fakeBlock, BLOCK_SIZE, &blockUUID)) {
        int rc = unqlite_kv_delete(pDb, blockUUID, KEY_SIZE);
        error_handler(rc);
    }
}

static int unlinkLinkInDirectory(const FileControlBlock *dirBlock, const char *name) {
    if(strlen(name) >= MAX_FILENAME_SIZE) return -ENAMETOOLONG;
    
    if(dirBlock->mode & S_IFDIR) {
        FCBBlockIterator iter = makeBlockIterator(dirBlock, 0);
        DirectoryDataBlock entries;

        uuid_t blockUUID;

        while(getNextBlock(&iter, &entries, sizeof(entries), &blockUUID) != NULL) {
            for(int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i) {
                if(strcmp(entries.entries[i].name, name) == 0) {
                    memset(&entries.entries[i], 0, sizeof(entries.entries[i]));
                    entries.usedEntries -= 1;
                    int rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                    error_handler(rc);
                    
                    return 0;
                }
            }
        }
        return -ENOENT;
    }
    return -ENOTDIR;
}

static int removeFileFCBinDirectory(const FileControlBlock* dirBlock, const char* name) {
    if(strlen(name) >= MAX_FILENAME_SIZE) return -ENAMETOOLONG;
    
    if(dirBlock->mode & S_IFDIR) {
        FCBBlockIterator iter = makeBlockIterator(dirBlock, 0);
        DirectoryDataBlock entries;

        uuid_t blockUUID;

        while(getNextBlock(&iter, &entries, sizeof(entries), &blockUUID) != NULL) {

            for(int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i) {
                if(strcmp(entries.entries[i].name, name) == 0) {
                    uuid_t fcb_uuid;
                    FileControlBlock fcb;
                    memcpy(&fcb_uuid, entries.entries[i].fcb_ref, sizeof(uuid_t));

                    unqlite_int64 bytesRead = sizeof(fcb);

                    int rc = unqlite_kv_fetch(pDb, fcb_uuid, KEY_SIZE, &fcb, &bytesRead);
                    error_handler(rc);
                        
                    if(S_ISREG(fcb.mode)) {

                        removeNodeData(&fcb);

                        unqlite_kv_delete(pDb, fcb_uuid, KEY_SIZE);

                        memset(&entries.entries[i], 0, sizeof(entries.entries[i]));

                        entries.usedEntries -= 1;
                        rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                        error_handler(rc);
                    }
                    else {
                        return -EISDIR;
                    }
                    
                    return 0;
                }
            }
        }
        return -ENOENT;
    }
    return -ENOTDIR;
}

static ssize_t numberOfChildren(const FileControlBlock* directory) {
    if(S_ISDIR(directory->mode)) {
        ssize_t noDirectories = 0;
        FCBBlockIterator iter = makeBlockIterator(directory, 0);
        DirectoryDataBlock entries;

        uuid_t blockUUID;

        while(getNextBlock(&iter, &entries, sizeof(entries), &blockUUID) != NULL) {
            noDirectories += entries.usedEntries;
        }

        return noDirectories;
    }

    return -EISDIR;
}

static int removeDirectoryFCBinDirectory(const FileControlBlock* dirBlock, const char* name) {
    if(strlen(name) >= MAX_FILENAME_SIZE) return -ENAMETOOLONG;
    
    if(dirBlock->mode & S_IFDIR) {
        FCBBlockIterator iter = makeBlockIterator(dirBlock, 0);
        DirectoryDataBlock entries;

        uuid_t blockUUID;

        while(getNextBlock(&iter, &entries, sizeof(entries), &blockUUID) != NULL) {
            for(int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i) {
                if(strcmp(entries.entries[i].name, name) == 0) {
                    uuid_t fcb_uuid;
                    FileControlBlock fcb;
                    memcpy(&fcb_uuid, entries.entries[i].fcb_ref, sizeof(uuid_t));

                    unqlite_int64 bytesRead = sizeof(fcb);

                    int rc = unqlite_kv_fetch(pDb, fcb_uuid, KEY_SIZE, &fcb, &bytesRead);
                    error_handler(rc);
                    
                    if(S_ISDIR(fcb.mode)) {

                        ssize_t noChildren = numberOfChildren(&fcb);

                        if(noChildren != 0) {
                            return -ENOTEMPTY;
                        }

                        removeNodeData(&fcb);
                        unqlite_kv_delete(pDb, fcb_uuid, KEY_SIZE);

                        memset(&entries.entries[i], 0, sizeof(entries.entries[i]));

                        entries.usedEntries -= 1;
                        rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                        error_handler(rc);
                        
                        return 0;
                    }
                    else {
                        return -ENOTDIR;
                    }
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
    
    unqlite_int64 bytesRead;

    rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, KEY_SIZE, NULL, &bytesRead);

    if(rc == UNQLITE_NOTFOUND) {
        perror("Root of filesystem not found. Creating it...\n");
        FileControlBlock rootDirectory;

        createDirectoryNode(&rootDirectory, DEFAULT_DIR_MODE);

        rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, KEY_SIZE, &rootDirectory, sizeof rootDirectory);

        error_handler(rc);
    }
    else {
        perror("Root of filesystem found. Using it as the root folder...\n");
        error_handler(rc);

        if(bytesRead != sizeof(FileControlBlock)) {
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

    FCBBlockIterator iter = makeBlockIterator(&currentDirectory, 0);
    DirectoryDataBlock entries;

    uuid_t blockUUID;

    while(getNextBlock(&iter, &entries, sizeof(entries), &blockUUID) != NULL) {
        for(int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i) {
            if(strcmp(entries.entries[i].name,"") != 0) {
                filler(buf, entries.entries[i].name, NULL, 0);
            }
        }
    }
    
    return 0;
}

static int readFromBlock(char* dest, FileDataBlock* block,  off_t offset, size_t size) {
    off_t start = offset, end = start + size;

    if(start >= block->size) return -1;

    unsigned bytesToRead = (unsigned) min(block->size - start, end - start);

    memcpy(dest,&block->data[offset], bytesToRead);
    return bytesToRead;
}

// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    write_log("myfs_read(path=\"%s\", size=%zu, offset=%zu", path, size, offset);
    
    FileControlBlock fcb;

    int rc = getFCBAtPath(path, &fcb, NULL);

    if(rc != 0) return rc;

    unsigned bytesRead = 0;
    if(S_ISREG(fcb.mode)) {

        FCBBlockIterator iter = makeBlockIterator(&fcb, 0);
        FileDataBlock dataBlock;

        while(getNextBlock(&iter, &dataBlock, sizeof(dataBlock), NULL)) {
            if(size == 0) break;
            off_t bR = readFromBlock(&buf[bytesRead], &dataBlock, offset, size);

            if(bR < 0) {
                offset -= BLOCK_SIZE;
            }
            else {
                bytesRead += bR;
                offset = max(0, offset - bR);
                size -= bR;
            }
        }

        return bytesRead;
    }
    else {
        return -EISDIR;
    }
}

// Create a file
// Read 'man 2 creat'.
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi){   
    write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);
    
    SplitPath newPath = splitPath(path);
    
    if(strlen(newPath.name) >= MAX_FILENAME_SIZE) {
        freeSplitPath(&newPath); 
        return -ENAMETOOLONG;
    }
    
    FileControlBlock currentDir;

    uuid_t parentFCBUUID;
    int rc = getFCBAtPath(newPath.parent, &currentDir, &parentFCBUUID);

    if(rc != 0){ 
        freeSplitPath(&newPath);
        return rc;
    }

    FileControlBlock newFCB;

    createFileNode(&newFCB, mode);

    uuid_t newFileRef = {0};

    uuid_generate(newFileRef);

    rc = unqlite_kv_store(pDb,newFileRef, KEY_SIZE, &newFCB, sizeof newFCB);

    error_handler(rc);

    rc = addFCBToDirectory(&currentDir,newPath.name,newFileRef);
    
    // In case new blocks were added.
    int dbRc = unqlite_kv_store(pDb, parentFCBUUID, KEY_SIZE, &currentDir, sizeof(currentDir));
    error_handler(dbRc);

    // TODO: Add error handling for when the name is already used. Currently, the DB is populated with something that has no 
    // reference.

    freeSplitPath(&newPath);
    return rc;
}

// Set update the times (actime, modtime) for a file. This FS only supports modtime.
// Read 'man 2 utime'.
static int myfs_utimens(const char *path, const struct timespec tv[2]){
    write_log("myfs_utimens(path=\"%s\")\n", path);

    FileControlBlock fcb;
    uuid_t fcbUUID;

    int rc = getFCBAtPath(path, &fcb, &fcbUUID);

    if(rc != 0) return rc;

    memcpy(&fcb.st_atim, &tv[0], sizeof(struct timespec));
    memcpy(&fcb.st_mtim, &tv[1], sizeof(struct timespec));

    rc = unqlite_kv_store(pDb,fcbUUID,KEY_SIZE,&fcb, sizeof(fcb));

    error_handler(rc);

    return 0;
}

static int writeToBlock(FileDataBlock *dest, const char* buf, off_t offset, size_t size) {
    off_t start = offset, end = start + size;

    if(start >= BLOCK_SIZE) return -1;

    unsigned bytesWritten = (unsigned) (min(end, BLOCK_SIZE) - start);

    memcpy(&dest->data[start], buf, bytesWritten);
    
    dest->size = (int)(start + bytesWritten);

    return bytesWritten;

}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){   
    write_log("myfs_write(path=\"%s\", size=%d, offset=%lld,)\n", path, size, offset);

    FileControlBlock fcb;
    uuid_t fileUUID;
    int rc = getFCBAtPath(path, &fcb, &fileUUID);

    if(rc != 0) return rc;

    off_t savedOffset = offset;

    int bytesWritten = 0;
    if(S_ISREG(fcb.mode)) {
        FileDataBlock dataBlock;
        for(int blockIdx = 0; blockIdx < NO_DIRECT_BLOCKS; ++blockIdx) {
            if(size == 0) break;

            if(uuid_compare(fcb.data_blocks[blockIdx], zero_uuid) == 0) {
                uuid_generate(fcb.data_blocks[blockIdx]);
                dataBlock = emptyFile;
            }
            else {
                unqlite_int64 nBytes = sizeof(dataBlock);
                rc = unqlite_kv_fetch(pDb, fcb.data_blocks[blockIdx], KEY_SIZE, &dataBlock, &nBytes);
                error_handler(rc);
            }

            int bW = writeToBlock(&dataBlock, &buf[bytesWritten], offset, size);
            if(bW == -1) {
                offset -= BLOCK_SIZE;
            }
            else {
                rc = unqlite_kv_store(pDb, fcb.data_blocks[blockIdx], KEY_SIZE, &dataBlock, sizeof(dataBlock));
                error_handler(rc);
                offset = max(0, offset - bW);
                size -= bW;
                bytesWritten += bW;
            }
        }

        if(size != 0) {
            uuid_t blocks[UUIDS_PER_BLOCK] = {{0}};
            if(uuid_compare(fcb.indirectBlock, zero_uuid) == 0) {
                uuid_t indirectBlockUUID = {0};
                uuid_generate(indirectBlockUUID);

                rc = unqlite_kv_store(pDb, indirectBlockUUID, KEY_SIZE, &blocks, sizeof(uuid_t) * UUIDS_PER_BLOCK);
                error_handler(rc);

                uuid_copy(fcb.indirectBlock, indirectBlockUUID);
            }

            unqlite_int64 nBytes = sizeof(uuid_t) * UUIDS_PER_BLOCK;
            rc = unqlite_kv_fetch(pDb, fcb.indirectBlock, KEY_SIZE, &blocks, &nBytes);
            error_handler(rc);

            for(int blockIdx = 0; blockIdx < UUIDS_PER_BLOCK; ++blockIdx) {
                if(size == 0) break;

                if(uuid_compare(blocks[blockIdx], zero_uuid) == 0) {
                    uuid_generate(blocks[blockIdx]);
                    dataBlock = emptyFile;
                }
                else {
                    nBytes = sizeof(dataBlock);
                    rc = unqlite_kv_fetch(pDb, blocks[blockIdx], KEY_SIZE, &dataBlock, &nBytes);
                    error_handler(rc);
                }

                int bW = writeToBlock(&dataBlock, &buf[bytesWritten], offset, size);
                if(bW == -1) {
                    offset -= BLOCK_SIZE;
                }
                else {
                    rc = unqlite_kv_store(pDb, blocks[blockIdx], KEY_SIZE, &dataBlock, sizeof(dataBlock));
                    error_handler(rc);
                    offset = max(0, offset - bW);
                    size -= bW;
                    bytesWritten += bW;
                }
            }
            rc = unqlite_kv_store(pDb, fcb.indirectBlock, KEY_SIZE, &blocks, sizeof(uuid_t) * UUIDS_PER_BLOCK);
            error_handler(rc);
        }

        fcb.size = max(fcb.size, savedOffset + bytesWritten);

        rc = unqlite_kv_store(pDb, fileUUID, KEY_SIZE, &fcb, sizeof(fcb));
        error_handler(rc);


        return bytesWritten;
    }
    else {
        return -EISDIR;
    }
}

// Set the size of a file.
// Read 'man 2 truncate'.
int myfs_truncate(const char *path, off_t newSize){    
    write_log("myfs_truncate(path=\"%s\", newSize=%lld)\n", path, newSize);

    off_t remainingSize = newSize;

    if(newSize > MAX_FILE_SIZE) {
        return -EFBIG;
    }

    uuid_t fileUUID;
    FileControlBlock fileToResize;

    int rc = getFCBAtPath(path, &fileToResize, &fileUUID);

    if(rc != 0) return rc;

    for(int blockIdx = 0; blockIdx < NO_DIRECT_BLOCKS; ++blockIdx) {
        if(remainingSize == 0) {
            if(uuid_compare(fileToResize.data_blocks[blockIdx], zero_uuid) != 0) {
                unqlite_kv_delete(pDb,fileToResize.data_blocks[blockIdx], KEY_SIZE);
                memset(&fileToResize.data_blocks[blockIdx], 0, KEY_SIZE);
            }
        }
        else {
            if(uuid_compare(fileToResize.data_blocks[blockIdx], zero_uuid) == 0) {
                uuid_t newBlockUUID = {0};
                uuid_generate(newBlockUUID);

                FileDataBlock block = emptyFile;
                block.size = (int) min(remainingSize, BLOCK_SIZE);
                remainingSize -= block.size;

                rc = unqlite_kv_store(pDb, newBlockUUID, KEY_SIZE, &block, sizeof(block));
                error_handler(rc);

                uuid_copy(fileToResize.data_blocks[blockIdx], newBlockUUID);
            }
        }
    }

    // Handle first level of indirection
    if(remainingSize == 0) {
        if(uuid_compare(fileToResize.indirectBlock, zero_uuid) != 0) {
            uuid_t blocks[UUIDS_PER_BLOCK];
            unqlite_int64 nBytes = sizeof(uuid_t) * UUIDS_PER_BLOCK;
            rc = unqlite_kv_fetch(pDb, fileToResize.indirectBlock, KEY_SIZE, &blocks, &nBytes);
            error_handler(rc);

            for(int blockIdx = 0; blockIdx < UUIDS_PER_BLOCK; ++blockIdx) {
                if(uuid_compare(blocks[blockIdx], zero_uuid) != 0) {
                    unqlite_kv_delete(pDb,blocks[blockIdx], KEY_SIZE);
                    memset(&blocks[blockIdx], 0, KEY_SIZE);
                }
            }
        }
        unqlite_kv_delete(pDb, fileToResize.indirectBlock, KEY_SIZE);
        memset(fileToResize.indirectBlock, 0, KEY_SIZE);
    }
    else {
        uuid_t blocks[UUIDS_PER_BLOCK] = {{0}};
        if(uuid_compare(fileToResize.indirectBlock, zero_uuid) == 0) {
            uuid_t indirectBlockUUID = {0};
            uuid_generate(indirectBlockUUID);

            rc = unqlite_kv_store(pDb, indirectBlockUUID, KEY_SIZE, &blocks, sizeof(uuid_t) * UUIDS_PER_BLOCK);
            error_handler(rc);

            uuid_copy(fileToResize.indirectBlock, indirectBlockUUID);
        }

        unqlite_int64 nBytes = sizeof(uuid_t) * UUIDS_PER_BLOCK;
        rc = unqlite_kv_fetch(pDb, fileToResize.indirectBlock, KEY_SIZE, &blocks, &nBytes);
        error_handler(rc);

        for(int blockIdx = 0; blockIdx < UUIDS_PER_BLOCK; ++blockIdx) {
            if(remainingSize == 0) {
                if(uuid_compare(blocks[blockIdx], zero_uuid) != 0) {
                    unqlite_kv_delete(pDb,blocks[blockIdx], KEY_SIZE);
                    memset(&blocks[blockIdx], 0, KEY_SIZE);
                }
            }
            else {
                if(uuid_compare(blocks[blockIdx], zero_uuid) == 0) {
                    uuid_t newBlockUUID = {0};
                    uuid_generate(newBlockUUID);

                    FileDataBlock block = emptyFile;
                    block.size = (int) min(remainingSize, BLOCK_SIZE);
                    remainingSize -= block.size;

                    rc = unqlite_kv_store(pDb, newBlockUUID, KEY_SIZE, &block, sizeof(block));
                    error_handler(rc);
                    
                    uuid_copy(blocks[blockIdx], newBlockUUID);
                }
            }
        }
        rc = unqlite_kv_store(pDb, fileToResize.indirectBlock, KEY_SIZE, &blocks, sizeof(blocks));
        error_handler(rc);
    }


    fileToResize.size = newSize;

    unqlite_int64 nBytes = sizeof(fileToResize);
    rc = unqlite_kv_store(pDb,fileUUID, KEY_SIZE, &fileToResize, nBytes);

    error_handler(rc);

    return 0;
}

// Set permissions.
// Read 'man 2 chmod'.
static int myfs_chmod(const char *path, mode_t mode){
    write_log("myfs_chmod(path=\"%s\", mode=0%03o)\n", path, mode);
    
    FileControlBlock block;
    uuid_t blockUUID;
    int rc = getFCBAtPath(path, &block, &blockUUID);

    if(rc != 0) return rc;

    block.mode |= mode;

    rc = unqlite_kv_store(pDb,blockUUID, KEY_SIZE, &block, sizeof(block));

    error_handler(rc);

    return 0;
}

// Set ownership.
// Read 'man 2 chown'.
static int myfs_chown(const char *path, uid_t uid, gid_t gid) {   
    write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
    
    FileControlBlock block;
    uuid_t blockUUID;

    int rc = getFCBAtPath(path, &block, &blockUUID);

    if(rc != 0) return rc;

    block.user_id = uid;
    block.group_id = gid;

    rc = unqlite_kv_store(pDb,blockUUID, KEY_SIZE, &block, sizeof(block));

    error_handler(rc);

    return 0;
}

// Create a directory.
// Read 'man 2 mkdir'.
int myfs_mkdir(const char *path, mode_t mode){
    write_log("myfs_mkdir(path=\"%s\", mode=0%03o)\n", path, mode);	
    
    SplitPath newPath = splitPath(path);
   
    if(strlen(newPath.name) >= MAX_FILENAME_SIZE) {
        freeSplitPath(&newPath);
        return -ENAMETOOLONG;
    }
    
    FileControlBlock currentDir;
    uuid_t parentFCBUUID;
    int rc = getFCBAtPath(newPath.parent, &currentDir, &parentFCBUUID);

    if(rc != 0){ 
        freeSplitPath(&newPath);
        return rc;
    }

    FileControlBlock newDirectory;
    createDirectoryNode(&newDirectory, mode);

    uuid_t newDirectoryRef = {0};

    uuid_generate(newDirectoryRef);

    rc = unqlite_kv_store(pDb,newDirectoryRef, KEY_SIZE, &newDirectory, sizeof newDirectory);

    error_handler(rc);

    rc = addFCBToDirectory(&currentDir, newPath.name, newDirectoryRef);

    // In case new blocks were added.
    int dbRc = unqlite_kv_store(pDb, parentFCBUUID, KEY_SIZE, &currentDir, sizeof(currentDir));
    error_handler(dbRc);

    // TODO: Add error handling for when the name is already used. Currently, the DB is populated with something that has no 
    // reference.

    freeSplitPath(&newPath);
    return rc;
}

// Delete a file.
// Read 'man 2 unlink'.
int myfs_unlink(const char *path){
    write_log("myfs_unlink(path=\"%s\")\n",path);	
    
    SplitPath pathToRemove = splitPath(path);

    FileControlBlock parentDir;

    int rc = getFCBAtPath(pathToRemove.parent, &parentDir, NULL);

    if(rc != 0) {
        freeSplitPath(&pathToRemove);
        return rc;
    }

    rc = removeFileFCBinDirectory(&parentDir,pathToRemove.name);

    freeSplitPath(&pathToRemove);

    return rc;    
}

// Delete a directory.
// Read 'man 2 rmdir'.
int myfs_rmdir(const char *path){
    write_log("myfs_rmdir(path=\"%s\")\n",path);	
    
   SplitPath pathToRemove = splitPath(path);

    FileControlBlock parentDir;

    int rc = getFCBAtPath(pathToRemove.parent, &parentDir, NULL);

    if(rc != 0) {
        freeSplitPath(&pathToRemove);
        return rc;
    }

    rc = removeDirectoryFCBinDirectory(&parentDir,pathToRemove.name);

    freeSplitPath(&pathToRemove);
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
    write_log("myfs_open(path=\"%s\")\n", path);
    
    return -ENOENT;
}

static int myfs_rename(const char* from, const char* to) {
    write_log("myfs_rename(from=\"%s\", to=\"%s\")", from, to);

    SplitPath fromPath = splitPath(from);

    uuid_t sourceDirUUID;
    FileControlBlock sourceDirFCB;

    int rc = getFCBAtPath(fromPath.parent, &sourceDirFCB, &sourceDirUUID);
    if(rc != 0) {
        freeSplitPath(&fromPath);
        return rc;
    }
    uuid_t sourceUUID;
    FileControlBlock sourceFCB;

    rc = getFCBInDirectory(&sourceDirFCB, fromPath.name, &sourceFCB, &sourceUUID);
    if(rc != 0) {
        freeSplitPath(&fromPath);
        return rc;
    }

    SplitPath toPath = splitPath(to);

    uuid_t targetDirUUID = {0};
    FileControlBlock targetParentFCB;
    FileControlBlock* targetParentFCBPtr;

    if(strcmp(toPath.parent, fromPath.parent) == 0) {
        targetParentFCBPtr = &sourceDirFCB;
        uuid_copy(targetDirUUID, sourceDirUUID);
    }
    else {
        targetParentFCBPtr = & targetParentFCB;
        rc = getFCBAtPath(toPath.parent, targetParentFCBPtr, &targetDirUUID);
        if(rc != 0) {
            freeSplitPath(&fromPath);
            freeSplitPath(&toPath);
            return rc;
        }
    }

    FileControlBlock targetFcb;

    rc = getFCBInDirectory(targetParentFCBPtr, toPath.name, &targetFcb, NULL);

    if(rc == 0) {
        if(S_ISDIR(targetFcb.mode)) {
            rc = removeDirectoryFCBinDirectory(targetParentFCBPtr, toPath.name);
            if(rc != 0) {
                freeSplitPath(&fromPath);
                freeSplitPath(&toPath);
                return rc;
            }
        }
        else {
            rc = removeFileFCBinDirectory(targetParentFCBPtr, toPath.name);
            if(rc != 0) {
                freeSplitPath(&fromPath);
                freeSplitPath(&toPath);
                return rc;
            }
        }

        rc = addFCBToDirectory(targetParentFCBPtr, toPath.name, sourceUUID);
        if(rc != 0) {
            freeSplitPath(&fromPath);
            freeSplitPath(&toPath);
            return rc;
        }
    }
    else if(rc == -ENOENT) {
        rc = addFCBToDirectory(targetParentFCBPtr, toPath.name, sourceUUID);
        if(rc != 0) {
            freeSplitPath(&fromPath);
            freeSplitPath(&toPath);
            return rc;
        }
    }
    else {
        freeSplitPath(&fromPath);
        freeSplitPath(&toPath);
        return rc;
    }
    
    rc = unlinkLinkInDirectory(&sourceDirFCB, fromPath.name);
    if(rc != 0) {
        freeSplitPath(&fromPath);
        freeSplitPath(&toPath);
        return rc;
    }

    rc = unqlite_kv_store(pDb, targetDirUUID, KEY_SIZE, targetParentFCBPtr, sizeof(targetParentFCB));
    error_handler(rc);

    rc = unqlite_kv_store(pDb, sourceDirUUID, KEY_SIZE, &sourceDirFCB, sizeof(sourceDirFCB));
    error_handler(rc);

    freeSplitPath(&fromPath);
    freeSplitPath(&toPath);
    return 0;
}

// This struct contains pointers to all the functions defined above
// It is used to pass the function pointers to fuse
// fuse will then execute the methods as required 
static struct fuse_operations myfs_oper = {
    .getattr	= myfs_getattr,
    .readdir	= myfs_readdir,
    .mkdir      = myfs_mkdir,
    .rmdir      = myfs_rmdir,
    .read		= myfs_read,
    .create		= myfs_create,
    .utimens 	= myfs_utimens,
    .write		= myfs_write,
    .truncate	= myfs_truncate,
    .unlink     = myfs_unlink,
    .chown      = myfs_chown,
    .chmod      = myfs_chmod,
    .rename     = myfs_rename
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

    uuid_clear(zero_uuid);
	
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

