package org.topleet.apps

import org.eclipse.jdt.core.dom.{AST, ASTParser, ASTVisitor, CatchClause, CompilationUnit, ConditionalExpression, DoStatement, EnhancedForStatement, ForStatement, IfStatement, SwitchCase, TryStatement, WhileStatement}
import org.topleet.Engine
import org.topleet.engines.IncrementalParallelEngine
import org.topleet.git.{ Resource, SHA}
import org.topleet.Leet

object Sandbox {

  // The foreign function computing MCC on a resource.
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

  // Library imports.

  import org.topleet.libs.Natives._
  import org.topleet.libs.Gits._

  implicit val engine: Engine = IncrementalParallelEngine.create()

  def main(args: Array[String]): Unit = {
    // Processing "jwtk/jjwt"

    val shas: Leet[SHA, Single[SHA]] = git("jwtk/jjwt")

    val resources: Leet[SHA, Bag[(Path, Resource)]] = shas.resources()

    val mcCabe: Leet[SHA, Bag[Int]] = resources
      .filter { case (path, resource) => path.endsWith(".java") }
      .map { case (path, resource) => computeMCC(resource) }

    val sum: Leet[SHA, Single[Int]] = mcCabe.sum()

    sum.show()
  }
}
