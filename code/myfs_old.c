/*
  MyFS. One directory, one file, 1000 bytes of storage. What more do you need?
  
  This Fuse file system is based largely on the HelloWorld example by Miklos Szeredi <miklos@szeredi.hu> (http://fuse.sourceforge.net/helloworld.html). Additional inspiration was taken from Joseph J. Pfeiffer's "Writing a FUSE Filesystem: a Tutorial" (http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/).
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <fcntl.h>

#include "myfs.h"

// The one and only fcb that this implmentation will have. We'll keep it in memory. A better 
// implementation would, at the very least, cache it's root directroy in memory. 
myfcb the_root_fcb;
unqlite_int64 root_object_size_value = sizeof(myfcb);

// This is the pointer to the database we will use to store all our files
unqlite *pDb;
uuid_t zero_uuid;


// The functions which follow are handler functions for various things a filesystem needs to do:
// reading, getting attributes, truncating, etc. They will be called by FUSE whenever it needs
// your filesystem to do something, so this is where functionality goes.

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
static int myfs_getattr(const char *path, struct stat *stbuf) {

	write_log("myfs_getattr(path=\"%s\", statbuf=0x%08x)\n", path, stbuf);

	memset(stbuf, 0, sizeof(struct stat));
	if(strcmp(path, "/")==0){
		stbuf->st_mode = the_root_fcb.root_mode;
		stbuf->st_nlink = 2;
		stbuf->st_uid = the_root_fcb.root_uid;
		stbuf->st_gid = the_root_fcb.root_gid;
	}else{
		if (strcmp(path, the_root_fcb.path) == 0) {
			stbuf->st_mode = the_root_fcb.mode;
			stbuf->st_nlink = 1;
			stbuf->st_mtime = the_root_fcb.mtime;
			stbuf->st_ctime = the_root_fcb.ctime;
			stbuf->st_size = the_root_fcb.size;
			stbuf->st_uid = the_root_fcb.uid;
			stbuf->st_gid = the_root_fcb.gid;
		}else{
			write_log("myfs_getattr - ENOENT");
			return -ENOENT;
		}
	}
	
	return 0;
}

// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

    (void) offset;  // This prevents compiler warnings
	(void) fi;

	write_log("write_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n", path, buf, filler, offset, fi);
	
	// This implementation supports only a root directory so return an error if the path is not '/'.
	if (strcmp(path, "/") != 0){
		write_log("myfs_readdir - ENOENT");
		return -ENOENT;
	}

    // We always output . and .. first, by convention. See documentation for more info on filler()
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

    // The root FCB is in memory, so we simply read the name of the file from the path variable inside it
	char *pathP = (char*)&(the_root_fcb.path);

	if(*pathP!='\0'){
		// drop the leading '/';
		pathP++;
		filler(buf, pathP, NULL, 0);
	}

    // Only one file, so nothing else to do
	
	return 0;
}

// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
	size_t len;
	(void) fi;
	
	write_log("myfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);
	
	if(strcmp(path, the_root_fcb.path) != 0){
		write_log("myfs_read - ENOENT");
		return -ENOENT;
	}
	
	len = the_root_fcb.size;
	
	uint8_t data_block[MY_MAX_FILE_SIZE];
	
	memset(&data_block, 0, MY_MAX_FILE_SIZE);
	uuid_t *data_id = &(the_root_fcb.file_data_id);
	// Is there a data block?
	if(uuid_compare(zero_uuid,*data_id)!=0){
		unqlite_int64 nBytes;  //Data length.
		int rc = unqlite_kv_fetch(pDb,data_id,KEY_SIZE,NULL,&nBytes);
		if( rc != UNQLITE_OK ){
		  error_handler(rc);
		}
		if(nBytes!=MY_MAX_FILE_SIZE){
			write_log("myfs_read - EIO");
			return -EIO;
		}
	
		// Fetch the fcb the root data block from the store.
		unqlite_kv_fetch(pDb,data_id,KEY_SIZE,&data_block,&nBytes);
	}
	
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, &data_block + offset, size);
	} else
		size = 0;

	return size;
}

// This file system only supports one file. Create should fail if a file has been created. Path must be '/<something>'.
// Read 'man 2 creat'.
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi){   
    write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);
	    
    if(the_root_fcb.path[0] != '\0'){
		write_log("myfs_create - ENOSPC");
		return -ENOSPC;
	}
		
	int pathlen = strlen(path);
	if(pathlen>=MY_MAX_PATH){
		write_log("myfs_create - ENAMETOOLONG");
		return -ENAMETOOLONG;
	}
	sprintf(the_root_fcb.path,path);
	struct fuse_context *context = fuse_get_context();
	the_root_fcb.uid=context->uid;
	the_root_fcb.gid=context->gid;
	the_root_fcb.mode=mode|S_IFREG;
	
	int rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb));
	if( rc != UNQLITE_OK ){
		write_log("myfs_create - EIO");
		return -EIO;
	}
    
    return 0;
}

// Set update the times (actime, modtime) for a file. This FS only supports modtime.
// Read 'man 2 utime'.
static int myfs_utime(const char *path, struct utimbuf *ubuf){
    write_log("myfs_utime(path=\"%s\", ubuf=0x%08x)\n", path, ubuf);
    
	if(strcmp(path, the_root_fcb.path) != 0){
		write_log("myfs_utime - ENOENT");
		return -ENOENT;
	}
	the_root_fcb.mtime=ubuf->modtime;
	
	// Write the fcb to the store.
    int rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb));
	if( rc != UNQLITE_OK ){
		write_log("myfs_write - EIO");
		return -EIO;
	}
    
    return 0;
}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){   
    write_log("myfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);
    
	if(strcmp(path, the_root_fcb.path) != 0){
		write_log("myfs_write - ENOENT");
		return -ENOENT;
    }
	
	if(size >= MY_MAX_FILE_SIZE){
		write_log("myfs_write - EFBIG");
		return -EFBIG;
	}

	uint8_t data_block[MY_MAX_FILE_SIZE];
	
	memset(&data_block, 0, MY_MAX_FILE_SIZE);
	uuid_t *data_id = &(the_root_fcb.file_data_id);
	// Is there a data block?
	if(uuid_compare(zero_uuid,*data_id)==0){
		// GEnerate a UUID fo rhte data blocl. We'll write the block itself later.
		uuid_generate(the_root_fcb.file_data_id);	
	}else{
		// First we will check the size of the obejct in the store to ensure that we won't overflow the buffer.
		unqlite_int64 nBytes;  // Data length.
		int rc = unqlite_kv_fetch(pDb,data_id,KEY_SIZE,NULL,&nBytes);
		if( rc!=UNQLITE_OK || nBytes!=MY_MAX_FILE_SIZE){
			write_log("myfs_write - EIO");
			return -EIO;
		}
	
		// Fetch the data block from the store. 
		unqlite_kv_fetch(pDb,data_id,KEY_SIZE,&data_block,&nBytes);
		// Error handling?
	}
	
	// Write the data in-memory.
    int written = snprintf(data_block, MY_MAX_FILE_SIZE, buf);
	
	// Write the data block to the store.
	int rc = unqlite_kv_store(pDb,data_id,KEY_SIZE,&data_block,MY_MAX_FILE_SIZE);
	if( rc != UNQLITE_OK ){
		write_log("myfs_write - EIO");
		return -EIO;
	}
	
	// Update the fcb in-memory.
	the_root_fcb.size=written;
	time_t now = time(NULL);
	the_root_fcb.mtime=now;
	the_root_fcb.ctime=now;
	
	// Write the fcb to the store.
    rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb));
	if( rc != UNQLITE_OK ){
		write_log("myfs_write - EIO");
		return -EIO;
	}
	
    return written;
}

// Set the size of a file.
// Read 'man 2 truncate'.
int myfs_truncate(const char *path, off_t newsize){    
    write_log("myfs_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);
    
    // Check that the size is acceptable
	if(newsize >= MY_MAX_FILE_SIZE){
		write_log("myfs_truncate - EFBIG");
		return -EFBIG;
	}
	
    // Update the FCB in-memory
	the_root_fcb.size = newsize;
	
	// Write the fcb to the store.
    int rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb));
	if( rc != UNQLITE_OK ){
		write_log("myfs_write - EIO");
		return -EIO;
	}
    
	return 0;
}

// Set permissions.
// Read 'man 2 chmod'.
int myfs_chmod(const char *path, mode_t mode){
    write_log("myfs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);
    
    return 0;
}

// Set ownership.
// Read 'man 2 chown'.
int myfs_chown(const char *path, uid_t uid, gid_t gid){   
    write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
   
    return 0;
}

// Create a directory.
// Read 'man 2 mkdir'.
int myfs_mkdir(const char *path, mode_t mode){
	write_log("myfs_mkdir: %s\n",path);	
	
    return 0;
}

// Delete a file.
// Read 'man 2 unlink'.
int myfs_unlink(const char *path){
	write_log("myfs_unlink: %s\n",path);	
	
    return 0;
}

// Delete a directory.
// Read 'man 2 rmdir'.
int myfs_rmdir(const char *path){
    write_log("myfs_rmdir: %s\n",path);	
	
    return 0;
}

// OPTIONAL - included as an example
// Flush any cached data.
int myfs_flush(const char *path, struct fuse_file_info *fi){
    int retstat = 0;
    
    write_log("myfs_flush(path=\"%s\", fi=0x%08x)\n", path, fi);
	
    return retstat;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
int myfs_release(const char *path, struct fuse_file_info *fi){
    int retstat = 0;
    
    write_log("myfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
    
    return retstat;
}

// OPTIONAL - included as an example
// Open a file. Open should check if the operation is permitted for the given flags (fi->flags).
// Read 'man 2 open'.
static int myfs_open(const char *path, struct fuse_file_info *fi){
	if (strcmp(path, the_root_fcb.path) != 0)
		return -ENOENT;
		
	write_log("myfs_open(path\"%s\", fi=0x%08x)\n", path, fi);
	
	//return -EACCES if the access is not permitted.

	return 0;
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


// Initialise the in-memory data structures from the store. If the root object (from the store) is empty then create a root fcb (directory)
// and write it to the store. Note that this code is executed outide of fuse. If there is a failure then we have failed toi initlaise the 
// file system so exit with an error code.
void init_fs(){
	int rc;
	printf("init_fs\n");
	//Initialise the store.
    
	uuid_clear(zero_uuid);
	
	// Open the database.
	rc = unqlite_open(&pDb,DATABASE_NAME,UNQLITE_OPEN_CREATE);
	if( rc != UNQLITE_OK ) error_handler(rc);

	unqlite_int64 nBytes;  // Data length

	// Try to fetch the root element
    // The last parameter is a pointer to a variable which will hold the number of bytes actually read
	rc = unqlite_kv_fetch(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,&nBytes);

    // if it doesn't exist, we need to create one and put it into the database. This will be the root
    // directory of our filesystem i.e. "/"
	if(rc==UNQLITE_NOTFOUND) {      

		printf("init_store: root object was not found\n");

        // clear everything in the_root_fcb
		memset(&the_root_fcb, 0, sizeof(myfcb));
				
        // Sensible initialisation for the root FCB
		//See 'man 2 stat' and 'man 2 chmod'.
		the_root_fcb.root_mode |= S_IFDIR|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH; 
		the_root_fcb.root_mtime = time(0);
		the_root_fcb.root_uid = getuid();
		the_root_fcb.root_gid = getgid();
		
        // Write the root FCB
		printf("init_fs: writing root fcb\n");
		rc = unqlite_kv_store(pDb,ROOT_OBJECT_KEY,ROOT_OBJECT_KEY_SIZE,&the_root_fcb,sizeof(myfcb));

        if( rc != UNQLITE_OK ) error_handler(rc);
    } 
    else
    {
     	if(rc==UNQLITE_OK) { 
	 		printf("init_store: root object was found\n"); 
        }
	 	if(nBytes!=sizeof(myfcb)) { 
			printf("Data object has unexpected size. Doing nothing.\n");
			exit(-1);
        }
    }
}

void shutdown_fs(){
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

