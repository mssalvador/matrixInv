# matrixInv
Scala matrix invertering i Apache Spark. Bruges med WorkflowCleaning.

# Scala: 
Installere Intellij-IDE, og husk installer Scala-plugin. Guide til Scala findes her: https://docs.scala-lang.org/getting-started-intellij-track/getting-started-with-scala-in-intellij.html
* Husk at brug en version af Scala, som passer med den version som Apache Spark bruger. fx. 2.11.12

# SBT
Vigtig at installere SBT, der bruges til at bygge den jar-fil, som skal bruges i Pyspark. 
* Installering: Ubuntu - brug: "apt-get install sbt=0.13.17". 
* Unlad at installer en sbt version, højere end 0.13.x, da det IKKE kan bygge Scala v. 2.11.x kode. 
* Bygning af jar-fil. Stå i projektets root-directory og brug kommando: "sbt package". Dette bygger en jar-fil. Outputtet i terminalen viser stien til jar-filen. 

![alt text](https://github.com/mssalvador/matrixInv/blob/master/pictures/how_to_jar.png)

* Guide til SBT: https://www.scala-sbt.org/0.13/docs/index.html
