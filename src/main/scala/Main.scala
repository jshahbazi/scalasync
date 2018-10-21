package scalasync

import scala.io.Source
import java.io.PrintWriter
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import java.security.MessageDigest
import java.security.DigestInputStream
import awscala._, s3._

import com.amazonaws.services.{ s3 => aws }

import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{FileSystems, Files}
import scala.collection.JavaConverters._
import java.nio.file.Paths.get
import java.nio.file.Paths

import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import scala.concurrent.{Future, future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.util.Random
    
import scala.collection.mutable.HashMap    
    
object sync {
    
    def checkForS3Bucket(bucketName: String, s3: S3): Boolean =  if (s3.buckets.find(_.name == bucketName) == None) true else false
    
    def computeHash(path: String): String = {
        val buffer = new Array[Byte](8192)
        val md5 = MessageDigest.getInstance("MD5")
        
        val dis = new DigestInputStream(new FileInputStream(new File(path)), md5)
        try { 
            while (dis.read(buffer) != -1) { } 
        } finally { dis.close() }
        
        md5.digest.map("%02x".format(_)).mkString
    }
    
    def createS3Bucket(bucketName: String, s3: S3): Boolean = {
        val buckets: Seq[Bucket] = s3.buckets
        var myBucket = None: Option[Bucket]
        var bucketFound = buckets.find(_.name == bucketName)

        try{
            if(bucketFound == None){
                try{
                    myBucket = Some(s3.createBucket(bucketName))
                    true
                }
                catch{
                    case genericError: Exception => {
                        println("Error creating " + bucketName + " bucket: " + genericError);
                        false                    
                    }
                    case s3Error: com.amazonaws.services.s3.model.AmazonS3Exception => {
                        println("Error creating " + bucketName + " bucket: " + s3Error);
                        false
                    }
                }
            }
            else{
                println("Bucket already exists.")
                true
            }
        }
        catch{
            case genericError: Exception => {
                println("Error: " + genericError)
                false
            }
            case s3Error: com.amazonaws.services.s3.model.AmazonS3Exception => {
                if(s3Error.toString.contains("AuthorizationHeaderMalformed")){
                    print("\nError (S3): Bucket already exists");                    
                } else{
                    print("\nError (S3): " + s3Error);                        
                }
                false
            }
        }
    }    

    def getBucketLocation(bucketName: Bucket, s3: S3): String = s3.location(bucketName)
    
    def getFiles(dir: String): List[_] = (new File(dir)).listFiles.filter(_.isFile).toList
    
    def getS3Bucket(bucketName: String, s3: S3): Bucket = {
        val buckets: Seq[Bucket] = s3.buckets
        var bucketFound = buckets.find(_.name == bucketName)
        val myBucket = (if (bucketFound == None) Some(s3.createBucket(bucketName)) else bucketFound).get
        myBucket
    }
    
    def printList(inputList: TraversableOnce[_]) = inputList.foreach{println}
    
    def updateConfigFile(fileName: String, bucketName: String, syncMode: String, directoryToMonitor: String): Boolean = {
        try{
            val pw = new PrintWriter(new File(fileName), "UTF-8")
            pw.println("bucketName = " + bucketName)
            pw.println("syncMode = " + syncMode)
            pw.println("directoryToMonitor = " + directoryToMonitor)
            pw.close      
            true
        }
        catch{
            case e: Exception => {
                println("Error (updateConfigFile): " + e);
                false
            }
        }
    }
    
    class FileChecker(var directoryToMonitor: String, var syncMode: String, var bucketName: String, implicit val s3: S3) extends Runnable {
        val bucket = getS3Bucket(bucketName,s3)

        def uploadFile(fileName: String, s3bucket: Bucket)(implicit s3: S3) = Future {
            try{
                var fileToUpload = new java.io.File(fileName)
                var S3FileName = fileName.trim.stripPrefix("./")
                println("Uploading " + fileName + "...")
                val result = s3bucket.put(S3FileName, fileToUpload) 
                if(result.key != null){
                    result.key
                } else {
                    println("Error uploading " + fileName)
                }
            } catch {
                case e: Exception => println("Error: " + e);
            }
        }
        
        def downloadFile(fileKey: String, s3bucket: Bucket)(implicit s3: S3) = Future {
            val in: InputStream = s3bucket.get(fileKey).get.getObjectContent
            try{
                println("Downloading ./" + fileKey + "...")
                val localFileName = "./" + fileKey
                Files.copy(in, Paths.get(localFileName));
            } catch {
                case e: Exception => println("Error (downloadFile): " + e);
            } finally {
                in.close() 
            }
        }   
        
        def loadS3Files: HashMap[String, String] = {
                var s3filesSet: HashMap[String, String] = scala.collection.mutable.HashMap()
                s3.ls(bucket, directoryToMonitor.stripPrefix("./") + "/").foreach {
                    case Left(directoryPrefix) => println(directoryPrefix)
                    case Right(s3ObjectSummary) => {
                        var fileName = s3ObjectSummary.getKey
                        var fileHash = s3ObjectSummary.getETag
                        // println("S3: File: " + fileName + " - Hash: " + s3ObjectSummary.getETag)
                        s3filesSet += ((fileName,fileHash))
                    }
                }      
                s3filesSet
        }

        def loadLocalFiles: HashMap[String, String] = {
            var localfilesSet: HashMap[String, String] = scala.collection.mutable.HashMap()
            var filesInCurrentDirectory = getFiles(directoryToMonitor)
            filesInCurrentDirectory.foreach({ obj =>
                var fileName = obj.toString.stripPrefix("./")
                var fileHash = computeHash(fileName)
                // println("LO: File: " + fileName + " - Hash: " + fileHash)
                localfilesSet += ((fileName,fileHash))
            })
            localfilesSet
        }
        
        def syncFilesToS3(s3filesSet: HashMap[String, String], localfilesSet: HashMap[String, String]) = {
            localfilesSet.foreach{ (hashtuple) =>
                var fileName = hashtuple._1
                var fileHash = hashtuple._2
            
                if(s3filesSet.contains(fileName)){
                    // println(fileName + " - Uploaded")
                } else{
                    // println("Uploading " + fileName + "...")
                    uploadFile(fileName,bucket)
                    .onComplete{
                        case Success(fileThatWasUploaded) => Unit // println("Successfully uploaded " + fileThatWasUploaded)
                        case Failure(e) => {
                            println("Error: Failed to upload " + fileName)
                            e.printStackTrace                        
                        }
                    }
                }            
            }
        }

        def syncS3ToLocal(s3filesSet: HashMap[String, String], localfilesSet: HashMap[String, String]) = {
            s3filesSet.foreach{ (hashtuple) =>
                var fileName = hashtuple._1
                var fileHash = hashtuple._2
            
                if(localfilesSet.contains(fileName)){
                    // println(fileName + " - Uploaded")
                } else{
                    // println("Uploading " + fileName + "...")
                    downloadFile(fileName,bucket)
                    .onComplete{
                        case Success(fileThatWasDownloaded) => Unit // println("Successfully downloaded " + fileThatWasDownloaded)
                        case Failure(e) => {
                            println("Error: Failed to download " + fileName)
                            e.printStackTrace                        
                        }
                    }
                }            
            }
        }
        
        def removeFilesFromS3(s3filesSet: HashMap[String, String], localfilesSet: HashMap[String, String]) = {
            s3filesSet.foreach{ (hashtuple) =>
                var fileName = hashtuple._1
                var fileHash = hashtuple._2
            
                if(localfilesSet.contains(fileName)){
                    // println(fileName + " - Uploaded")
                } else{
                    // var objToDelete = s3.get(bucket,fileName)
                    println("Deleting " + fileName + "...")
                    bucket.delete(fileName)
                }            
            }
        }
        
        def removeFilesFromLocal(s3filesSet: HashMap[String, String], localfilesSet: HashMap[String, String]) = {
            localfilesSet.foreach{ (hashtuple) =>
                var fileName = hashtuple._1
                var fileHash = hashtuple._2
            
                if(s3filesSet.contains(fileName)){
                    // println(fileName + " - Uploaded")
                } else{
                    // var objToDelete = s3.get(bucket,fileName)
                    println("Deleting " + fileName + "...")
                    //bucket.delete(fileName)
                    if(new File(fileName).delete()){
                    }
                    else{
                        println("Error deleting " + fileName)
                    }
                }            
            }
        }        

        def run {
            try{
                while(true){
                    var s3filesSet = loadS3Files
                    var localfilesSet = loadLocalFiles

                    syncMode match{
                        case "push" => {
                            syncFilesToS3(s3filesSet,localfilesSet)
                            removeFilesFromS3(s3filesSet,localfilesSet)                            
                        }
                        case "pull" => {
                            // println("Pull mode in development...")
                            syncS3ToLocal(s3filesSet,localfilesSet)
                            removeFilesFromLocal(s3filesSet,localfilesSet)                             
                        }
                        case _ => println("Error: Unknown syncMode")
                    }
                    
                    Thread.sleep(10000)
                }
            } catch {
                case interrupted: InterruptedException => Unit;
                case genericError: Exception => println("Error: " + genericError);
                case s3Error: com.amazonaws.services.s3.model.AmazonS3Exception => {
                    if(s3Error.toString.contains("NoSuchBucket")){
                        print("\nError (S3): Bucket not found");                    
                    } else{
                        print("\nError (S3): " + s3Error);                        
                    }
                }               
            }
            
        }
    }
    
    def main(args: Array[String]) {

        try{
            var directoryToMonitor = "."
            var syncMode = "push"
            var bucketName = ""
            var doSetup = false
            implicit val s3 = S3.at(Region.NorthernVirginia)

            if (args.length != 0){
                if(args(0).toLowerCase == "setup"){
                    doSetup = true                    
                }
                else {
                    println("The only available argument is 'setup', i.e., scalasync setup")
                    return 
                }
            }
            
            val config = ConfigFactory.parseFile(new File("./scalasync.conf"))
            if(config.isEmpty){
                println("scalasync.conf configuration file does not exist.\nCreating a file with a random bucket name and default settings.\nRun 'scalasync setup' to change these values.")
                var localhostname = java.net.InetAddress.getLocalHost().getHostName()
                var rn =  scala.util.Random
                bucketName = localhostname + rn.nextInt
                doSetup = true
            }
            else{
                bucketName = config.getString("bucketName")
                if(checkForS3Bucket(bucketName,s3) == false){
                    println("Error finding S3 bucket.  Running setup...")
                    doSetup = true
                }
                syncMode = config.getString("syncMode")
                directoryToMonitor = config.getString("directoryToMonitor")
                if(!(new java.io.File(directoryToMonitor).exists)){
                    println("Error reading " + directoryToMonitor)
                    return
                }                
            }

            if(doSetup == true){
                println("Entering setup for scalasync...")
                var bucketGood = false
                var input = ""
                while(bucketGood == false){
                    print("Please enter name for your S3 bucket [" + bucketName + "]: ")
                    input = readLine()
                    input match {
                        case "" => {
                            bucketGood = createS3Bucket(bucketName,s3)
                        }
                        case _  => {
                            bucketName = input
                            bucketGood = createS3Bucket(bucketName,s3)
                        }
                    }
                    if(bucketGood == false){println("Bucket creation/retrieval issue... (hit Ctrl-C to exit or continue trying)")}
                }
                print("Please enter the sync mode (push or pull) [" + syncMode + "]: ")
                input = readLine()
                input match {
                    case "" => {}
                    case "pull"  => syncMode = "pull"
                    case "push"  => syncMode = "push"
                    case _  => println("Error")
                }                     
                print("Please enter the directory to monitor [" + directoryToMonitor + "]: ")
                input = readLine()
                input match {
                    case "" => {}
                    case _  => directoryToMonitor = input
                }
                val configUpdated = updateConfigFile("./scalasync.conf",bucketName,syncMode,directoryToMonitor)
                if(configUpdated == false){
                    println("Config file updated failed.  Exiting...")
                    return
                }                    
            }
            
            if(!(new java.io.File(directoryToMonitor).exists)){
                println("Error reading " + directoryToMonitor)
                return
            }

            val pool = java.util.concurrent.Executors.newFixedThreadPool(1,
                new ThreadFactory() {
                    def newThread(r: Runnable) = {
                        var t = Executors.defaultThreadFactory().newThread(r)
                        t.setDaemon(true)
                        t
                    }
            })
            
            pool.execute(new FileChecker(directoryToMonitor,syncMode,bucketName,s3))

            Iterator.continually({
                    readLine().toUpperCase
                }).foreach{
                case "Q" => {
                    println("Quitting...")
                    return
                }
                case "L" => {printList(getFiles(directoryToMonitor))};
                case  _  => println("(Q)uit, (L)ist files")
            }         
            
            
        } catch {
            case e: Exception => println("Error (Main): " + e);
        }
    }
    
}