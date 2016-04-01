package com.firearasi

import org.apache.spark.{SparkConf,SparkContext}


object FirstSpark  {
	def main(args:Array[String]) = {
	
		
		if(args.length<1) {
			System.err.println("Not enough args!")
			
			System.exit(1)
		}
		
		
		
		val conf=new SparkConf().setAppName("FirstSpark")
		val sc=new SparkContext(conf)
		sc.setLogLevel("WARN")
		println("Printing ten highest words sparks of "+args(0))
		
		
	    val start = System.currentTimeMillis		

		val rawTf=sc.textFile(args(0))
		
		//println("rawTF")
		//rawTf.collect.foreach(println)	
		
		val pairs=rawTf.flatMap(_.toLowerCase.split("\\s+")).filter(_!="").map((_,1))
		
		//println("pairs")
		//pairs.collect.foreach(println)
		
		
		val wc=pairs.reduceByKey(_+_).sortBy(_._2,ascending=false)		
		println("wc")
		wc.take(10).foreach(println)
			
		
		
		
		
		val end= System.currentTimeMillis
		println("Elapsed time:"+(end-start).toString+" milliseconds")
	}
	
}


