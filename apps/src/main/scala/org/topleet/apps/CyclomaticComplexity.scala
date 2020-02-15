package org.topleet.apps

import org.eclipse.jdt.core.dom.{AST, ASTParser, ASTVisitor, CatchClause, CompilationUnit, ConditionalExpression, DoStatement, EnhancedForStatement, ForStatement, IfStatement, SwitchCase, TryStatement, WhileStatement}
import org.topleet.{Engine, Leet}
import org.topleet.engines.IncrementalParallelEngine
import org.topleet.git.{Resource, SHA}

object CyclomaticComplexity {

  // The foreign function computing the MCC metric a resource.
  def computeMCC(resource: Resource): Int = {
    val parser = ASTParser.newParser(AST.JLS13)
    parser.setSource(resource.read().toCharArray)
    parser.setKind(ASTParser.K_COMPILATION_UNIT)

    val cu = parser.createAST(null).asInstanceOf[CompilationUnit]

    var result = 0
    cu.accept(new ASTVisitor() {
      override def visit(node: WhileStatement): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: SwitchCase): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: CatchClause): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: ConditionalExpression): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: TryStatement): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: ForStatement): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: EnhancedForStatement): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: DoStatement): Boolean = {
        result = result + 1
        super.visit(node)
      }

      override def visit(node: IfStatement): Boolean = {
        result = result + 1
        super.visit(node)
      }

    })

    result
  }

  // The relevant library imports.
  import org.topleet.libs.Natives._
  import org.topleet.libs.Gits._

  // Main.
  def main(args: Array[String]): Unit = {
    val output = run(IncrementalParallelEngine.create(), "jwtk/jjwt")

    // Plot the repository topology in the browser.
    output.show()

    // Iterate the data on the commits.
    for ((sha, mcCabe) <- output.index())
    // Note that this single is actually a Bag and its alias Map[Int, Int] here.
      println("Metric at commit " + sha + " is " + mcCabe)
  }

  // Application.
  def run(engine: Engine, address: Address): Leet[SHA, Single[Int]] = {

    val shas: Leet[SHA, Single[SHA]] = git("jwtk/jjwt")(engine)

    val resources: Leet[SHA, Bag[(Path, Resource)]] = shas.resources()

    val mcCabe: Leet[SHA, Bag[Int]] = resources
      .filter { case (path, _) => path.endsWith(".java") }
      .map { case (_, resource) => computeMCC(resource) }

    val output: Leet[SHA, Single[Int]] = mcCabe.sum()

    output
  }

}
