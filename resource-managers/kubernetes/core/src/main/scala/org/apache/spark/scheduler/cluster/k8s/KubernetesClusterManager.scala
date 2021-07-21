/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.scheduler.cluster.k8s

import java.io.File
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import collection.JavaConverters._
import com.google.common.cache.CacheBuilder
import com.google.common.net.InetAddresses
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{Config, KubernetesClient}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, ExternalClusterManager, HDFSCacheTaskLocation, SchedulerBackend, TaskLocality, TaskScheduler, TaskSchedulerImpl, TaskSet, TaskSetManager}
import org.apache.spark.util.{SystemClock, ThreadUtils}

/**
 * This is a patched KubernetesClusterManager for host based data locality.
 * Host based data locality for the TIKI DSP Cluster works as follows:
 * Some of the k8s kubelets have a HDFS Datanode,
 * so we need to identify the Nodename on which the Executor Pods are spawned
 * and need to ensure that this Nodename is used for Dispatching the tasks correctly.
 */
private[spark] class KubernetesClusterManager extends ExternalClusterManager with Logging {

  private def emptyTasks = ArrayBuffer.empty[Int]

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    val clusterManager = this

    new TaskSchedulerImpl(sc) {
      val scheduler = this
      logDebug("Kubernetes TaskScheduler created")
      override def createTaskSetManager(taskSet: TaskSet,
                                        maxTaskFailures: Int
                                       ): TaskSetManager = {
        new TaskSetManager(sched = this, taskSet, maxTaskFailures) {

          def getPendingTasksForHost(executorIP: String): ArrayBuffer[Int] = {
            var pendingTasksforHost = pendingTasks.forHost.getOrElse(executorIP, ArrayBuffer())
            if (pendingTasksforHost.nonEmpty) {
              return pendingTasksforHost
            }
            val pod: Option[Pod] = this.getPodFromExecutorIP(executorIP)
            if (pod.isEmpty) {
              logDebug("Recieved NULL-Pod")
              return emptyTasks
            }
            logDebug("Recieved Pod named: " + pod.get.getMetadata.getName)
            val clusterNodeName = pod.get.getSpec.getNodeName
            val clusterNodeIP = pod.get.getStatus.getHostIP
            logDebug("Resolved Executor: " + pod.get.getMetadata.getName + " to Node: " +
              clusterNodeName + " with Node-IP: " + clusterNodeIP)
            pendingTasksforHost = pendingTasks.forHost.getOrElse(clusterNodeName, ArrayBuffer())
            if (pendingTasksforHost.isEmpty) {
              logDebug("Comparison with Nodename failed")
              pendingTasksforHost = pendingTasks.forHost.getOrElse(clusterNodeIP, ArrayBuffer())
            }
            if (pendingTasksforHost.nonEmpty) {
              logInfo(s"Got preferred task list $pendingTasks for executor host $executorIP" +
                s" using cluster node $clusterNodeName at $clusterNodeIP")
            }
            pendingTasksforHost
          }

          override def dequeueTaskHelper(execId: String,
                                         host: String,
                                         maxLocality: TaskLocality.Value,
                                         speculative: Boolean):
          Option[(Int, TaskLocality.Value, Boolean)] = {
            if (speculative && speculatableTasks.isEmpty) {
              return None
            }
            val pendingTaskSetToUse = if (speculative) pendingSpeculatableTasks else pendingTasks
            def dequeue(list: ArrayBuffer[Int]): Option[Int] = {
              val task = this.dequeueTaskFromList(execId, host, list, speculative)
              if (speculative && task.isDefined) {
                speculatableTasks -= task.get
              }
              task
            }

            dequeue(pendingTaskSetToUse.forExecutor
              .getOrElse(execId, ArrayBuffer())).foreach { index =>
              return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
            }

            if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
              dequeue(getPendingTasksForHost(host)).foreach { index =>
                return Some((index, TaskLocality.NODE_LOCAL, speculative))
              }
            }

            // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
            if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
              dequeue(pendingTaskSetToUse.noPrefs).foreach { index =>
                return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
              }
            }

            if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
              for {
                rack <- scheduler.getRackForHost(host)
                index <- dequeue(pendingTaskSetToUse.forRack.getOrElse(rack, ArrayBuffer()))
              } {
                return Some((index, TaskLocality.RACK_LOCAL, speculative))
              }
            }

            if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
              dequeue(pendingTaskSetToUse.all).foreach { index =>
                return Some((index, TaskLocality.ANY, speculative))
              }
            }
            None
          }

          override def addPendingTask(index: Int,
                                      resolveRacks: Boolean = true,
                                      speculatable: Boolean = false): Unit = {
            // A zombie TaskSetManager may reach here while handling failed task.
            if (isZombie) return
            val pendingTaskSetToAddTo = if (speculatable) pendingSpeculatableTasks else pendingTasks
            for (loc <- tasks(index).preferredLocations) {
              loc match {
                case e: ExecutorCacheTaskLocation =>
                  pendingTaskSetToAddTo.forExecutor
                    .getOrElseUpdate(e.executorId, new ArrayBuffer) += index
                case e: HDFSCacheTaskLocation =>
                  val exe = scheduler.getExecutorsAliveOnHost(loc.host)
                  exe match {
                    case Some(set) =>
                      for (e <- set) {
                        pendingTaskSetToAddTo.forExecutor
                          .getOrElseUpdate(e, new ArrayBuffer) += index
                      }
                      logInfo(s"Pending task $index has a cached location at ${e.host} " +
                        ", where there are executors " + set.mkString(","))
                    case None => logDebug(s"Pending task $index has a" +
                      " cached location at ${e.host} " +
                      ", but there are no executors alive there.")
                  }
                case _ =>
              }
              var location: String = loc.host
              if (InetAddresses.isInetAddress(loc.host)) {
                location = this.getPodFromExecutorIP(loc.host).get.getSpec.getNodeName
                logDebug("Found InetAdress as location: " + loc.host +
                         " and resolved it to nodename: " + location)
              } else {
                location = loc.host.split("\\.")(0)
                logDebug("Found netString as location: " + loc.host +
                  " ** Resolved it to NodeName: " + location)
              }
              pendingTaskSetToAddTo.forHost.getOrElseUpdate(location, new ArrayBuffer) += index

              if (resolveRacks) {
                scheduler.getRackForHost(loc.host).foreach { rack =>
                  pendingTaskSetToAddTo.forRack.getOrElseUpdate(rack, new ArrayBuffer) += index
                }
              }
            }

            if (tasks(index).preferredLocations == Nil) {
              pendingTaskSetToAddTo.noPrefs += index
            }

            pendingTaskSetToAddTo.all += index
          }
          def getPodFromExecutorIP(podIP: String): Option[Pod] = {
            logDebug("Requested to Resolve Pod with VIP: " + podIP)
            val kubernetesClient = clusterManager.getOrCreateKubernetesClient(sc, masterURL)

            val executor_pods: Seq[Pod] = kubernetesClient
              .pods()
              .withLabel(SPARK_APP_ID_LABEL, applicationId())
              .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
              .list().getItems().asScala
            val podReturn = executor_pods.find(pod => pod.getStatus.getPodIP == podIP)
            logDebug("Resolved Executor with VIP: " + podIP + " to "
              + podReturn.get.getMetadata.getName)
            podReturn
          }
        }
      }
    }
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {

    // If KUBERNETES_EXECUTOR_POD_NAME_PREFIX is not set, initialize it so that all executors have
    // the same prefix. This is needed for client mode, where the feature steps code that sets this
    // configuration is not used.
    //
    // If/when feature steps are executed in client mode, they should instead take care of this,
    // and this code should be removed.
    if (!sc.conf.contains(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)) {
      sc.conf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
        KubernetesConf.getResourceNamePrefix(sc.conf.get("spark.app.name")))
    }

    val kubernetesClient = getOrCreateKubernetesClient(sc, masterURL)

    if (sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).isDefined) {
      KubernetesUtils.loadPodFromTemplate(
        kubernetesClient,
        new File(sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).get),
        sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME))
    }

    val schedulerExecutorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-maintenance")

    ExecutorPodsSnapshot.setShouldCheckAllContainers(
      sc.conf.get(KUBERNETES_EXECUTOR_CHECK_ALL_CONTAINERS))
    val sparkContainerName = sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME)
      .getOrElse(DEFAULT_EXECUTOR_CONTAINER_NAME)
    ExecutorPodsSnapshot.setSparkContainerName(sparkContainerName)
    val subscribersExecutor = ThreadUtils
      .newDaemonThreadPoolScheduledExecutor(
        "kubernetes-executor-snapshots-subscribers", 2)
    val snapshotsStore = new ExecutorPodsSnapshotsStoreImpl(subscribersExecutor)

    val removedExecutorsCache = CacheBuilder.newBuilder()
      .expireAfterWrite(3, TimeUnit.MINUTES)
      .build[java.lang.Long, java.lang.Long]()
    val executorPodsLifecycleEventHandler = new ExecutorPodsLifecycleManager(
      sc.conf,
      kubernetesClient,
      snapshotsStore,
      removedExecutorsCache)

    val executorPodsAllocator = new ExecutorPodsAllocator(
      sc.conf,
      sc.env.securityManager,
      new KubernetesExecutorBuilder(),
      kubernetesClient,
      snapshotsStore,
      new SystemClock())

    val podsWatchEventSource = new ExecutorPodsWatchSnapshotSource(
      snapshotsStore,
      kubernetesClient)

    val eventsPollingExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-pod-polling-sync")
    val podsPollingEventSource = new ExecutorPodsPollingSnapshotSource(
      sc.conf, kubernetesClient, snapshotsStore, eventsPollingExecutor)

    new KubernetesClusterSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc,
      kubernetesClient,
      schedulerExecutorService,
      snapshotsStore,
      executorPodsAllocator,
      executorPodsLifecycleEventHandler,
      podsWatchEventSource,
      podsPollingEventSource)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }

  def initializeKubernetesClient(sc: SparkContext, masterURL: String): KubernetesClient = {

    val (authConfPrefix,
    apiServerUri,
    defaultServiceAccountToken,
    defaultServiceAccountCaCrt) = if (sc.conf.get(KUBERNETES_DRIVER_SUBMIT_CHECK)) {
      require(sc.conf.get(KUBERNETES_DRIVER_POD_NAME).isDefined,
        "If the application is deployed using spark-submit in cluster mode, the driver pod name " +
          "must be provided.")
      val serviceAccountToken =
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)).filter(_.exists)
      val serviceAccountCaCrt =
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)).filter(_.exists)
      (KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        sc.conf.get(KUBERNETES_DRIVER_MASTER_URL),
        serviceAccountToken,
        serviceAccountCaCrt)
    } else {
      (KUBERNETES_AUTH_CLIENT_MODE_PREFIX,
        KubernetesUtils.parseMasterUrl(masterURL),
        None,
        None)
    }

    SparkKubernetesClientFactory.createKubernetesClient(
      apiServerUri,
      Some(sc.conf.get(KUBERNETES_NAMESPACE)),
      authConfPrefix,
      SparkKubernetesClientFactory.ClientType.Driver,
      sc.conf,
      defaultServiceAccountToken,
      defaultServiceAccountCaCrt)
  }

  def getOrCreateKubernetesClient(sc: SparkContext, masterURL: String): KubernetesClient = {
    this.kc match {
      case Some(client) =>
        client
      case None =>
        val client = initializeKubernetesClient(sc, masterURL)
        this.kc = Some(client)
        client
    }
  }

  var kc: Option[KubernetesClient] = None
}
