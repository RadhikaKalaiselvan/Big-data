// Databricks notebook source
import sys.process._
import scala.math 
import java.io.File

//author Radhika Kalaiselvan

// COMMAND ----------

//download the tar file
"wget -P /tmp http://qwone.com/~jason/20Newsgroups/20news-bydate.tar.gz" !



// COMMAND ----------

//download the stop words file
"wget -P /tmp https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt" !

// COMMAND ----------

//extract the tar
"gunzip  /tmp/20news-bydate.tar.gz" !

// COMMAND ----------

//extract the tar
"tar -xvf /tmp/20news-bydate.tar -C /tmp/"!


// COMMAND ----------

//load the stopwords and broadcast
val stopWordsInput =sc.textFile("file:/tmp/common-english-words-with-contractions.txt");
val stopWords = stopWordsInput.flatMap(x => x.split(",")).map(_.trim)
val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)

// COMMAND ----------

"ls -la /tmp/20news-bydate-train/"!

// COMMAND ----------

//This method is called for each folder
//loads all the files from the given folder
//remove stopwords and words of length less than 5
//find distinct words and add * as document ID inorder to find the Ni value


def processFoldersNew(Name: String): RDD[((String,String), Int)] = {
val rawdata = sc.textFile("file:/tmp/20news-bydate-train/"+Name)
  println("processing document "+Name+"...")

  // Split using a regular expression that extracts words
val words: RDD[String] = rawdata.flatMap(x => x.split("\\W+"))
  
val wordsWithStopWords=words.filter(x => x.length > 5)
val validWords=wordsWithStopWords.filter(!broadcastStopWords.value.contains(_))
  //validWords.take(10).foreach(println)
  

val distinctWords=validWords.distinct
//distinctWords.take(10).foreach(println)
val distinctwordPairs = distinctWords.map(word => ((word,"*"),1))
//distinctwordPairs.take(10)
  

val wordPairs = validWords.map(word => ((word,Name),1))
val wordCounts = wordPairs.reduceByKey((x,y)=>(x+y)) 
val oneDocumentValues=wordCounts.union(distinctwordPairs)
return oneDocumentValues
}



// COMMAND ----------

//call the above method for every folder
val folders=new File("/tmp/20news-bydate-train/").listFiles
val rdd = folders.map{
  x => processFoldersNew(x.getName())
                     }.reduce(_ union _)


// COMMAND ----------

//sample print
rdd.take(30).foreach(println)

// COMMAND ----------

//reduce by key inorder to find the word count.
val processedRdd=rdd.reduceByKey((x,y)=>(x+y)) 

// COMMAND ----------

val ni_Values=processedRdd.filter{ case((term:String,docID:String),count:Int)=> docID=="*"}

// COMMAND ----------

//adding the term as the key because we will be perfroeming join using the first key
val modifiedNiValues=ni_Values.map(x=>(x._1._1,x))

// COMMAND ----------

val tfValues=processedRdd.filter{case((term:String,docID:String),count:Int)=> docID!="*"}

// COMMAND ----------

//adding the term as the key because we will be perfroeming join using the first key
val modifiedTfValues=tfValues.map(x=>(x._1._1,x))

// COMMAND ----------

val mergedRdd=modifiedTfValues.join(modifiedNiValues)


// COMMAND ----------

//log (x) function in scala returns ln(x) so we have to multiply by 0.4342918 to get log(x)
def findWij(term:String,ni: Int,tf:Int): Double = {
  if(term=="fundamentalists"){
    println("tf ="+tf+" ni "+ni+" "+tf*scala.math.log(20/ni))
  }
  tf*scala.math.log(20/ni)*0.43429418
}

// COMMAND ----------

val invertedIndex=mergedRdd.map{case(term:String,(((term1:String,docName:String),tf:Int),((term2:String,charc:String),ni:Int)))=>
  {
    val wij=findWij(term1,ni,tf)
    ((term,wij),docName)
  }
                               
                               }

// COMMAND ----------

//sample print of inverted index
invertedIndex.take(100)

// COMMAND ----------

//set of test inputs
var testInput=Set("fundamentalists",
"theocracy",
"chilling",
"translation",
"philadelphia",
"university",
"atheistic",
"agnostic",
"inductive",
"illustrated",
"photosynthesis",
"objectively",
"fundamental",
"psychological",
"condescending",
"biblical",
"paleontology",
"government",
"prayers",
"personality")

// COMMAND ----------

//get all rows with given test input
  val topDocuments=invertedIndex.filter{case((term:String,wij:Double),docName:String)=> testInput.contains(term)}
  val topDoc=topDocuments.sortBy{case((term:String,wij:Double),docName:String) => (term,-wij)}
  topDoc.collect().foreach(println)


// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="fundamentalists"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="theocracy"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="chilling"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="translation"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="philadelphia"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="university"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="atheistic"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="agnostic"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="inductive"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="illustrated"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="photosynthesis"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="objectively"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="fundamental"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="psychological"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="condescending"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="biblical"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="paleontology"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="government"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="prayers"}.take(5).foreach(println)

// COMMAND ----------

topDoc.filter{case((term:String,wij:Double),docName:String)=> term=="personality"}.take(5).foreach(println)



