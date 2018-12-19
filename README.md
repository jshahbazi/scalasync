# scalasync
Simple Scala app to sync up files in a directory to an Amazon S3 bucket

### First time setup:
If you're running this for the first time, run this command:
```
scalasync setup
```
Running this will ask you a few questions and create a scalasync.conf file that will contain the following settings:
- Bucketname (you can provide a name or a random one will be created for you)
- Sync mode (push or pull)
- Directory to monitor (the default is the current directory)

### How to run:
Simply run:
```
scalasync
```
