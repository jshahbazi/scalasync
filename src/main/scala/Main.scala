package scalasync

import scala.io.Source
import java.io.PrintWriter
import java.io.File
import java.io.FileInputStream
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
        try{
            var fileToUpload = new java.io.File(fileName)
            println("Uploading " + fileName + "...")
            val result = s3bucket.put(fileName, fileToUpload) 
            if(result.key != null){
                result.key
            } else {
                println("Error uploading " + fileName)
            }
        } catch {
            case e: Exception => println("Error: " + e);
        }
    }
    
    
    class FileChecker(var directoryToMonitor: String, var syncMode: String, var bucketName: String, implicit val s3: S3) extends Runnable {
        val bucket = getS3Bucket(bucketName)
        
        def getS3Bucket(bucketName: String): Bucket = {
            val buckets: Seq[Bucket] = s3.buckets
            var myBucket = None: Option[Bucket]
            var bucketFound = buckets.find(_.name == bucketName)

            if(bucketFound == None){
                myBucket = Some(s3.createBucket(bucketName))
            }

            myBucket.get
        }
        
        
        def loadS3Files: HashMap[String, String] = {
                var s3filesSet: HashMap[String, String] = scala.collection.mutable.HashMap()
                // println("S3 Files:")
                s3.ls(bucket, directoryToMonitor + "/").foreach {
                    case Left(directoryPrefix) => println(directoryPrefix)
                    case Right(s3ObjectSummary) => {
                        var fileName = s3ObjectSummary.getKey
                        var fileHash = s3ObjectSummary.getETag
                        // println("File: " + fileName + " - Hash: " + s3ObjectSummary.getETag)
                        s3filesSet += ((fileName,fileHash))
                    }
                }      
                s3filesSet
        }

        def loadLocalFiles: HashMap[String, String] = {
            var localfilesSet: HashMap[String, String] = scala.collection.mutable.HashMap()
            var filesInCurrentDirectory = getFiles(directoryToMonitor)
            filesInCurrentDirectory.foreach({ obj =>
                var fileName = obj.toString
                var fileHash = computeHash(fileName)
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
                    // println(fileName + " - Pending")
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
                            println("Pull mode in development...")
                        }
                        case _ => println("Error: Unknown syncMode")
                    }
                    
                    Thread.sleep(10000)
                }
            } catch {
                case interrupted: InterruptedException => Unit;
                case genericError: Exception => println("Error: " + genericError);
            }
            
        }
    }
    
    def main(args: Array[String]) {

        try{
            var directoryToMonitor = ""
            var syncMode = ""
            var bucketName = ""
            // var amznRegion = com.amazonaws.regions.Regions.US_EAST_1
            if (args.length == 0){
                directoryToMonitor = new java.io.File( "." ).getCanonicalPath()
                syncMode = "push"
                bucketName = "scalasync"
                // amznRegion = com.amazonaws.regions.Regions.US_EAST_1
            } else {
                // val dirExists = new java.io.File(args(0)).exists
                if(new java.io.File(args(0)).exists){
                    directoryToMonitor = new java.io.File(args(0)).getCanonicalPath()
                    println("Monitoring " + directoryToMonitor)
                }
                else {
                    throw new IllegalArgumentException("Directory does not exist: " + args(0))
                }
                if(!args(1).isEmpty){
                    syncMode = args(1)
                    println("Sync Mode: " + syncMode)
                }
                else {
                    syncMode = "push"
                    println("Sync Mode: " + syncMode)
                }
                if(!args(2).isEmpty){
                    bucketName = args(2)
                    println("S3 Bucket: " + bucketName)
                }
                else {
                    bucketName = "scalasync"
                    println("S3 Bucket: " + bucketName)
                }
                // if(args(3) == "US_EAST_1"){
                //     amznRegion = com.amazonaws.regions.Regions.US_EAST_1
                // }
                // else{
                //     amznRegion = com.amazonaws.regions.Regions.US_EAST_1
                // }
                // println("Amazon Region: " + amznRegion.toString)
            }            
            
         
            implicit val s3 = S3()
            s3.setRegion(com.amazonaws.regions.Region.getRegion(com.amazonaws.regions.Regions.US_EAST_1))
            // s3.setRegion(Region.getRegion(Regions.US_EAST_1))
            // S3.at(Region.NorthernVirginia)
            
            // implicit val s3 = S3.at(Region.NorthernVirginia)
            // s3.createBucket("jnstestbucket")
            
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
                    print("Enter your choice: (Q)uit, (L)ist files: ")
                    readLine().toUpperCase
                }).takeWhile(_.nonEmpty).foreach {
                case "Q" => println("Quitting...");return
                case "L" => {printList(getFiles(directoryToMonitor))};
                case _ => println("Invalid choice")
            }
            
            
        } catch {
            case e: Exception => println("Error (Main): " + e);
        }
    }
    
}