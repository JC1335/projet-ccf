# Projet CCF – 
Ce projet implémente l'algorithme des composants connexes en Scala avec Spark selon trois approches :

- ✅ RDD (Scala)
- ✅ DataFrame (Scala)
- ✅ GraphX (Scala)

## Structure du projet

src/
main/
scala/
CCF/
CCFRDD.scala
CCFDataFrame.scala
CCFGraphX.scala
Main.scala
data/
G1_1k.csv
G2_5k.csv



## Comment exécuter

- Prérequis : Docker, JDK 8+, IntelliJ avec Scala plugin
- `sbt run` ou via IntelliJ (`Main.scala`)

## Performances comparées

| Approche       | G1 (s) | G2 (s) | G3 (s) | G4 (s) |
|----------------|--------|--------|--------|--------|
| RDD (Scala)    | 2.61   | 2.57   | 2.60   | 2.82   |
| DataFrame (Scala) | 349.81 | 700.07 | 819.06 | 917.36 |
| GraphX (Scala) | 2.93   | 1.49   | 1.25   | 1.15   |
| RDD (PySpark)  | 84.54  | 162.01 | 156.46 | 158.00 |
| DataFrame (PySpark) | 135.07 | 100.33 | 269.29 | 108.96 |

## Auteurs

- 👤 Jean-Christophe HAMARD 1 Dina HOURLIER (Étudiants Master IA)


