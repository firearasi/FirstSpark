#!/bin/bash
spark-submit --class com.firearasi.FirstSpark --master spark://firearasi-HP:7077 target/FirstSpark-1.0-SNAPSHOT.jar $1
