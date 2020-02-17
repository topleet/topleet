# Topleet
Incremental Map-Reduce on Repository History

## Repository Structure

This repository uses sbt and contains the following sub-projects: 
* **Core** contains the minimal functionality, the native library
and the basic engine implementations.
* **Git** contains the the connector to Git repositories and the
corresponding library.
* **Spark** contains the implementation of the first Spark engine.
* **Apps** contains first applications cases (in migration).
* **Eval** contains the test and measurement utils (in migration).

## Getting Started

The scala object [Sandbox](apps/src/main/scala/org/topleet/apps/Sandbox.scala)
in the Apps project serves as a good template to start with. 

The code imports the *Natives* and *Gits* library providing all basic
methods for processing GitHub repositories. 
```scala
import org.topleet.libs.Natives._
import org.topleet.libs.Gits._
```
Second, the code initializes a parallel and incremental engine 
which is used as the underlying processing infrastructure
realizing the library calls.
```scala
import org.topleet.Engine
import org.topleet.engines.IncrementalParallelEngine

implicit val engine: Engine = IncrementalParallelEngine.create()
```
Hereafter, the actual processing can start with the initialization 
the first data structure of type `Leet`.
```scala
import org.topleet.git.SHA
import org.topleet.Leet

val shas: Leet[SHA, Single[SHA]] = git("jwtk/jjwt")
```
By using the provided `git` library method and passing a repository owner and
name, a data structure of type `Leet[SHA, Single[SHA]]` is created which reflects
the acyclic history of the Git repository. 
Each commit, which is identified by the first generic `SHA`, is assigned to its
own SHA given by the second generic `Single[SHA]`. 
The content and the graph structure of the data structure can already be 
examined by invoking `show()`, `index()`, `nodes()` and `edges()` on `shas`.

In most application, such initialization is followed by an extraction
of available resources on every commit. 
The Git specific resource access is enable by method `resources()`, 
just provided for data structures of type `Leet[SHA, Single[SHA]]`.
   
```scala
import org.topleet.git.Resource

val resources: Leet[SHA, Bag[(Path, Resource)]] = shas.resources()
```

This data structure now assigns commits to bags 
of path-resource tuples `Bag[(Path, Resource)]`. Such data structure
is a convenient starting point for any further processing 
involving `filter`, `join`, `cartesian`, `map`, `flatten`, `sum` or `length` (count) calls.
The following code counts the number 
of lines in java files without single line comments. (In [Sandbox](apps/src/main/scala/org/topleet/apps/Sandbox.scala), JDT is used to extract the cyclomatic complexity.)   

```scala
val loc: Leet[SHA, Single[Int]] = resources
      .filter { case (path, _) => path.endsWith(".java") }
      .map { case (_, resource) => bag(resource.read().split("\n")) }
      .flatten()
      .filter(line => !line.startsWith("//"))
      .length()
```

The final content of the data structure can be accessed using the `index()` call.
```scala
val iterator: Iterator[(SHA, Single[Int])] = loc.index()

for ((sha, metric) <- iterator)
  println("sha " + sha + " with values " + metric)
```
# Spark Engine
A Spark engine (including GraphPartition and NodeAlias optimizations)
can be created using the following constructor. A SparkContext and 
the assumed number of commits need to be handed as parameters.  
```scala
import org.topleet.engines.SparkEngine

val spark: SparkContext = ...
val ncommits: Int = ...
implicit val engine: Engine = SparkEngine.create(ncommits, spark)
```

# Outstanding Tasks
Next, we will fill the missing gaps in the implementations, improve the engines 
and prepare the first maven release.  

