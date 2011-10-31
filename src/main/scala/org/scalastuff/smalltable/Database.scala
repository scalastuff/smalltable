package org.scalastuff.smalltable

import scala.collection.JavaConversions._

import org.scalastuff.smalltable.conf.ColumnFamilyDefinitionEx
import org.scalastuff.smalltable.view.UpdateQuery
import org.slf4j.LoggerFactory

import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.FailoverPolicy
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.Keyspace
import scalaz.Scalaz._
import scalaz._

abstract class Database(keyspaceName: String) {
  val logger = LoggerFactory.getLogger(this.getClass)

  def clusterName = "Test Cluster"
  def hostname = "localhost:9160"
  def cassandraHostConfiguragtor = new CassandraHostConfigurator(hostname)
  def consistencyLevelPolicy = HFactory.createDefaultConsistencyLevelPolicy()
  def failoverPolicy = FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE
  def credentials = java.util.Collections.emptyMap[String, String]
  lazy val cluster = HFactory.createCluster(clusterName, cassandraHostConfiguragtor, credentials)
  lazy val keyspace = HFactory.createKeyspace(keyspaceName, cluster, consistencyLevelPolicy, failoverPolicy, credentials)
  def checkConfMode: conf.CheckConfigurationMode = conf.CheckConfigurationMode.Validate

  def keyspaceDefinition = cluster.describeKeyspace(keyspaceName)
  
  def execute[K](update: UpdateQuery[K], updates: UpdateQuery[K]*) = {
    val mutator = HFactory.createMutator(keyspace, update.rowKeySerializer)
    update.mutation(mutator)
    updates.foreach(_.mutation(mutator))
    
    mutator.execute()
  }
  
  def execute[R](query: Keyspace => R): R = query(keyspace)
  
  def truncate(cf: ColumnOrSuperColumnFamily) {
    cluster.truncate(keyspaceName, cf.name)
  }

  
  def check(cfDef: ColumnFamilyDefinitionEx[_]): ValidationNEL[String, Cluster => Cluster] = {

    if (keyspaceDefinition eq null)
      checkConfMode.onKeyspaceNotFound(cluster, keyspaceName)

    val strategy = checkConfMode.createCheckConfigurationStrategy(cfDef, keyspaceName)
    val existingCF = keyspaceDefinition.getCfDefs find (_.getName == cfDef.getName)

    val validatedCF =
      existingCF match {
        case None        => strategy.onCFNotFound()
        case Some(cfDef) => strategy.onCFFound(cfDef)
      }

    // if there are errors, add first line pointing to column family name and keyspace
    validatedCF.fail map { errors =>
      "Validation of Column Family '%s' in Keyspace '%s' has failed with following errors:".
        format(cfDef.getName, keyspaceName) <:: errors
    } //validation

    validatedCF
  }

  def configurationChecks: Traversable[ValidationNEL[String, Cluster => Cluster]]

  var hasErrors = false
  configurationChecks foreach { cfValidation: ValidationNEL[String, Cluster => Cluster] =>
    cfValidation match {
      case Success(updates) => updates(cluster)
      case Failure(errors)  => hasErrors = true; errors |>| { error => logger.error(error) }
    }
  }
  
  if (hasErrors)
    sys.error("Disconnected from the database due to previous errors")
}