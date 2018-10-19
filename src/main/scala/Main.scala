package scalasync

import scala.io.Source
import java.io.PrintWriter
import java.io.File
import java.io.FileInputStream
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import java.security.MessageDigest
import java.security.DigestInputStream
import awscala._, s3._

import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{FileSystems, Files}
import scala.collection.JavaConverters._
import java.nio.file.Paths.get

import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory;
import scala.concurrent.{Future, future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.util.Random
    
import scala.collection.mutable.HashMap    
    
object sync {
    
    def getFiles(dir: String): List[_] = {
        val pathToScan = FileSystems.getDefault.getPath(dir)
        Files.walk(pathToScan).iterator().asScala.filter(Files.isRegularFile(_)).toList
        
        // val d = new File(dir)
        // if(d.exists && d.isDirectory){
        //     d.listFiles.filter(_.isFile).toList
        // } else{
        //     List[File]()
        // }
        
    }
    
    def computeHash(path: String): String = {
        val buffer = new Array[Byte](8192)
        val md5 = MessageDigest.getInstance("MD5")
        
        val dis = new DigestInputStream(new FileInputStream(new File(path)), md5)
        try { 
            while (dis.read(buffer) != -1) { } 
        } finally { dis.close() }
        
        md5.digest.map("%02x".format(_)).mkString
    }
    
    def printList(inputList: TraversableOnce[_]): Unit = {
        inputList.foreach(println)
    }

    def uploadFile(fileName: String, s3bucket: Bucket)(implicit s3: S3) = Future {
        // try{
            var fileToUpload = new java.io.File(fileName)
            var S3FileName = fileName.trim.stripPrefix("./")
            println("Uploading " + fileName + "...")
            val result = s3bucket.put(S3FileName, fileToUpload) 
            if(result.key != null){
                result.key
            } else {
                println("Error uploading " + fileName)
            }
        // } catch {
        //     case e: Exception => println("Error: " + e);
        // }
    }
    
    //Bucket already exists error message:
    //com.amazonaws.services.s3.model.AmazonS3Exception: The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'eu-west-1' (Service: Amazon S3; Status Code: 400; Error Code: AuthorizationHeaderMalformed
    
    def getS3Bucket(bucketName: String, s3: S3): Bucket = {
        val buckets: Seq[Bucket] = s3.buckets
        var myBucket = None: Option[Bucket]
        var bucketFound = buckets.find(_.name == bucketName)

        
        if(bucketFound == None){
            myBucket = Some(s3.createBucket(bucketName))
        }
        else{
            myBucket = bucketFound
        }

        // println(s3.location(myBucket.get))
        myBucket.get
    }
    
    def checkForS3Bucket(bucketName: String, s3: S3): Boolean = {
        val buckets: Seq[Bucket] = s3.buckets
        var myBucket = None: Option[Bucket]
        var bucketFound = buckets.find(_.name == bucketName)

        
        if(bucketFound == None){
            return false
        }
        else{
            return true
        }
    }        
    
    def createS3Bucket(bucketName: String, s3: S3): Boolean = {
        val buckets: Seq[Bucket] = s3.buckets
        var myBucket = None: Option[Bucket]
        var bucketFound = buckets.find(_.name == bucketName)

        
        if(bucketFound == None){
            try{
                myBucket = Some(s3.createBucket(bucketName))
                return true
            }
            catch{
                case genericError: Exception => {
                    println("Error creating " + bucketName + " bucket: " + genericError);
                    return false                    
                }
                case s3Error: com.amazonaws.services.s3.model.AmazonS3Exception => {
                    println("Error creating " + bucketName + " bucket: " + s3Error);
                    return false
                }
            }
        }
        else{
            // myBucket = bucketFound
            println("Bucket already exists.")
            return true
        }
    }    
    
    class FileChecker(var directoryToMonitor: String, var syncMode: String, var bucketName: String, implicit val s3: S3) extends Runnable {
        val bucket = getS3Bucket(bucketName,s3)

        def loadS3Files: HashMap[String, String] = {
                var s3filesSet: HashMap[String, String] = scala.collection.mutable.HashMap()
                // println("S3 Files:")
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
                
                // Error: com.amazonaws.services.s3.model.AmazonS3Exception: The specified bucket does not exist (Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucket; Request ID: 4D828B3776B0E7E7; 
                // S3 Extended Request ID: IKcgmD9M53VF9kYMfaYrwJnEOKfaJv60zyAObBNLlIV7LzoUl9qFBUjFGLb6IvGBz92bEJyZ9vw=), S3 Extended Request ID: IKcgmD9M53VF9kYMfaYrwJnEOKfaJv60zyAObBNLlIV7LzoUl9qFBUjFGLb6IvGBz92bEJyZ9vw=
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
                    // println(fileName + " - Pending")
                    // uploadFile(fileName,bucket)
                    // .onComplete{
                    //     case Success(fileThatWasUploaded) => Unit // println("Successfully uploaded " + fileThatWasUploaded)
                    //     case Failure(e) => {
                    //         println("Error: Failed to upload " + fileName)
                    //         e.printStackTrace                        
                    //     }
                    // }
                }            
            }
        }

        def run {
            // try{
                while(true){
                    var s3filesSet = loadS3Files
                    var localfilesSet = loadLocalFiles

                    syncMode match{
                        case "push" => {
                            syncFilesToS3(s3filesSet,localfilesSet)
                            removeFilesFromS3(s3filesSet,localfilesSet)                            
                        }
                        case "pull" => {
                            println("Pull mode in development...")
                        }
                        case _ => println("Error: Unknown syncMode")
                    }
                    
                    Thread.sleep(10000)
                }
            // } catch {
            //     case interrupted: InterruptedException => Unit;
            //     case genericError: Exception => println("Error: " + genericError);
            //     case s3Error: com.amazonaws.services.s3.model.AmazonS3Exception => print("\nError (S3): " + s3Error);                
            // }
            
        }
    }
    
    def updateConfigFile(fileName: String, bucketName: String, syncMode: String, directoryToMonitor: String): Boolean = {
        try{
            val pw = new PrintWriter(new File(fileName), "UTF-8")
            pw.println("bucketName = " + bucketName)
            pw.println("syncMode = " + syncMode)
            pw.println("directoryToMonitor = " + directoryToMonitor)
            pw.close      
            return true
        }
        catch{
            case e: Exception => {
                println("Error (updateConfigFile): " + e);
                return false
            }
        }
    }
    
    def main(args: Array[String]) {

        // try{
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
                syncMode = config.getString("syncMode")
                directoryToMonitor = config.getString("directoryToMonitor")
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
                            // println("Using the bucket name of: " + bucketName)
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
        // } catch {
        //     case e: Exception => println("Error (Main): " + e);
        // }
    }
    
}