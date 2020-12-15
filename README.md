# Topleet
Incremental Map-Reduce on Repository History

## Repository Structure

This repository uses sbt and contains the following sub-projects: 
* **Core** contains the minimal functionality, the native library
and the basic engine implementations.
* **Git** contains the connector to Git repositories and the
corresponding library.
* **Spark** contains the implementation of the first Spark engine.
* **Apps** contains first applications cases (in migration).
* **Eval** contains the test and measurement utils (in migration).

## Getting Started
Check out this repository or add the following dependencies to your sbt file (currently just available for scala 2.12).
```scala
scalaVersion := "2.12.10"
libraryDependencies += "org.topleet" %% "core" % "0.1.0"
libraryDependencies += "org.topleet" %% "git" % "0.1.0" // Optional Git
libraryDependencies += "org.topleet" %% "spark" % "0.1.0" // Optional Spark
```

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
Hereafter, the actual processing can start with the initialization of
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

# Graph View (`.show()`)

The content of the background graph can be visualized using
call `.show()` on any `Leet` data structure (produced by the `Gits` library). 
The call generates a html file that is opened by
the default browser. Parts of the behavior can be adapted in the call, e.g., orientation or edge labels.
Zooming and moving is possible. 

Showing the initial data structure of the previous sections by `shas.show()`
results in the following window opening in the browser ([Viewer](content/viewer1.html)). While hovering over nodes,
meta-data appears, and a link to the commit on GitHub can be navigated.  

![Alt text](content/graph1.png?raw=true "")

A data structure might get large, and the recent visualization might hit limitations
showing extraordinary large graphs. However, the graph viewer employs
a trick called "graph contraction" to only show edges and nodes
where change happen. 

The following graph shows an evolving cyclomatic complexity metric
summed over all files of a repository ([Viewer](content/viewer2.html)). 
Nodes that are connected by an
edge not changing the metric, are 'merged' into a single node (we refer to this as 'contracted').
This decreases the size of the visualized graph. The annotated meta-data of such 
nodes is special as it lists all contracted commits. In such list, the first commit (owner)
did the actual change, and the other commits just follow without further changes.

![Alt text](content/graph2.png?raw=true "")

# Misc
Presentation slides can be found [here](content/slides.pdf)
and the corresponding SANER paper can be found [here](content/paper.pdf)
with the following BibTeX entry. 
```
@inproceedings{DBLP:conf/wcre/HartelL20,
  author    = {Johannes H{\"{a}}rtel and
               Ralf L{\"{a}}mmel},
  title     = "{Incremental Map-Reduce on Repository History}",
  booktitle = {{SANER}},
  pages     = {320--331},
  publisher = {{IEEE}},
  year      = {2020}
}
```
