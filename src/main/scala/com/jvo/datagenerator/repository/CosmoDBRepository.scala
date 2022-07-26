//package com.jvo.datagenerator.repository
//
//import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel, DataType, Database, Document, DocumentClientException, DocumentCollection, IncludedPath, Index, IndexingPolicy, PartitionKeyDefinition, RequestOptions, ResourceResponse}
//import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
//import rx.Observable
//
//import java.util.concurrent.CopyOnWriteArrayList
//import scala.util.{Failure, Success, Try}
//
//import com.microsoft.azure.cosmosdb.ResourceResponse
//import rx.Observable
//import java.util.concurrent.CountDownLatch
//// Read the created document// Read the created document
//
//import com.microsoft.azure.cosmosdb.PartitionKey
//import com.microsoft.azure.cosmosdb.ResourceResponse
//import rx.Observable
//import java.util
//import java.util.Collections
//// Read the document// Read the document
//
//object CosmoDBRepository {
//
//  private val databaseName = "test_data_storage"
//  private val cosmosdbUri = "https://datagenerator-cosmosdb-dev.documents.azure.com:443/"
//  private val cosmosdbKey = "bCkVzKE5kFXdSVa1G6FVtls7Rs5Xh08nU1yi7Jf556wIeMGW0xKWENN7ysPgagTfyVEYliAILJX5I3lWkOFTaQ=="
//
//  private val collectionName = "MyCollection"
//  private val partitionKeyPath = "/type"
//  private val containerName = "GeneratedDataContainer"
//  private val throughPut = 400
//
//  private val client = new AsyncDocumentClient.Builder()
//    .withServiceEndpoint(cosmosdbUri)
//    .withMasterKeyOrResourceToken(cosmosdbKey)
//    .withConnectionPolicy(ConnectionPolicy.GetDefault())
//    .withConsistencyLevel(ConsistencyLevel.Session)
//    .build()
//
//  private def createDatabaseIfNotExists(): Unit = {
//    val dbReadObs = client.readDatabase(s"/dbs/$databaseName", null)
//
//    val dbOperationObs = dbReadObs
//      .doOnNext(
//        (x: ResourceResponse[Database]) => println(s"Database $databaseName already exists. Self-Link: ${x.getResource.getSelfLink}")
//      )
//      .onErrorResumeNext(
//        (e: Throwable) =>
//          e match {
//            case x: DocumentClientException if x.getStatusCode == 404 => {
//              val db = new Database()
//              db.setId(databaseName)
//
//              client.createDatabase(db, null)
//            }
//            case _ => Observable.error(e)
//          }
//      )
//
//    dbOperationObs.toCompletable.await()
//  }
//
//
//  private def createCollection(): Unit = {
//    // Creating collection defenition
//    val collectionDefinition = new DocumentCollection
//    collectionDefinition.setId(collectionName)
//
//    val partitionKeyDefinition = new PartitionKeyDefinition
//    val paths = new java.util.ArrayList[String]
//    paths.add(partitionKeyPath)
//    partitionKeyDefinition.setPaths(paths)
//    collectionDefinition.setPartitionKey(partitionKeyDefinition)
//
//    // Set indexing policy to be range for string and number
//    val indexingPolicy = new IndexingPolicy
//    val includedPaths = new java.util.ArrayList[IncludedPath]
//    val includedPath = new IncludedPath
//    includedPath.setPath("/*")
//    val indexes = new java.util.ArrayList[Index]
//
//    val stringIndex = Index.Range(DataType.String)
//    stringIndex.set("precision", -1)
//    indexes.add(stringIndex)
//
//    val numberIndex = Index.Range(DataType.Number)
//    numberIndex.set("precision", -1)
//    indexes.add(numberIndex)
//
//    includedPath.setIndexes(indexes)
//    includedPaths.add(includedPath)
//    indexingPolicy.setIncludedPaths(includedPaths)
//    collectionDefinition.setIndexingPolicy(indexingPolicy)
//
//    //Create collection
//    val databaseLink = s"/dbs/$databaseName"
//    val collectionLink = s"$databaseLink/colls/$collectionName"
//
//    val colReadObs = client.readCollection(collectionLink, null)
//
//    val colOperationObs = colReadObs
//      .doOnNext(
//        (x: ResourceResponse[DocumentCollection]) => println(s"Collection $collectionName already exists. Self-Link: ${x.getResource.getSelfLink}")
//      )
//      .onErrorResumeNext(
//        (e: Throwable) =>
//          e match {
//            case x: DocumentClientException if x.getStatusCode == 404 => {
//              val multiPartitionRequestOptions = new RequestOptions
//              multiPartitionRequestOptions.setOfferThroughput(throughPut)
//
//              client.createCollection(databaseLink, collectionDefinition, multiPartitionRequestOptions)
//            }
//            case _ => Observable.error(e)
//          }
//      )
//
//    colOperationObs.toCompletable.await()
//  }
//
//  def main(args: Array[String]): Unit = {
//    // Creating database
//    //    val resultCreateDb = Try(createDatabaseIfNotExists())
//    //    resultCreateDb match {
//    //      case Success(_) => println(s"Operation completed successfully: creating database $databaseName if not exists.")
//    //      case Failure(exception) => print(s"Error while creating database: ${exception.getMessage}")
//    //    }
//
//    //    // Creating collection
//    //    val resultCreateCollection = Try(createCollection())
//    //    resultCreateCollection match {
//    //      case Success(_) => println(s"Operation completed successfully: creating collection $collectionName if not exists.")
//    //      case Failure(exception) => print(s"Error while creating collection: ${exception.getMessage}")
//    //    }
//
//    // Adding documents
//    val document: Document = addDocument()
//
////    val list = getDocument("test-document")
////    println(list)
//
//    client.close()
//  }
//
//  def addDocument() = {
//    val documentDefinition = new Document()
//    documentDefinition.setId("test-document-1")
//    documentDefinition.set("counter", 1)
//    documentDefinition.set("firstName", "John")
//    documentDefinition.set("lastName", "Snow")
//    documentDefinition.set("age", 25)
//    documentDefinition.set("city", "Winterfell")
//
//    val options = new RequestOptions()
//    options.setPartitionKey(new PartitionKey("AzureShit"))
//
//    val createdDocument: Document = client
//      .createDocument(getCollectionLink, documentDefinition, options, false)
//      .toBlocking()
//      .single()
//      .getResource()
//
//    val readDocumentObservable: Observable[ResourceResponse[Document]] = client.readDocument(getDocumentLink(createdDocument), options)
//
//    import java.util.concurrent.CountDownLatch
//    val completionLatch: CountDownLatch = new CountDownLatch(1)
//
//    val capturedResponse = new CopyOnWriteArrayList[Document]
//
//    readDocumentObservable.subscribe((resourceResponse: ResourceResponse[Document]) => {
//      val document: Document = resourceResponse.getResource()
//      completionLatch.countDown()
//      capturedResponse.add(document)
//    }).wait(1000)
//
//    println(capturedResponse)
//    capturedResponse.get(0)
//  }
//
//  def getDocument(documentId: String, partitionKey: String = "/type") = {
//
//    val options = new RequestOptions()
//    options.setPartitionKey(new PartitionKey(partitionKey))
//    val readDocumentObservable: Observable[ResourceResponse[Document]] = client.readDocument(getDocumentLink(documentId), options)
//
//    val capturedResponse = new CopyOnWriteArrayList[ResourceResponse[Document]]
//
//    readDocumentObservable.subscribe((resourceResponse: ResourceResponse[Document]) => {
//      capturedResponse.add(resourceResponse)
//    }).wait(1000)
//
//    capturedResponse
//
//  }
//
////  // Container create
////  @throws[Exception]
////  private def createContainerIfNotExists(): Unit = {
////    //  Create container if not exists
////    val containerProperties = new CosmosContainerProperties(containerName, "/lastName")
////    // Provision throughput
////    val throughputProperties = ThroughputProperties.createManualThroughput(400)
////    //  Create container with 200 RU/s
////    val containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties)
////    container = database.getContainer(containerResponse.getProperties.getId)
////    logger.info("Done.")
////  }
//
//  private def getCollectionLink = "dbs/" + databaseName + "/colls/" + collectionName
//
//  private def getDocumentLink(createdDocument: Document) = "dbs/" + databaseName + "/colls/" + collectionName + "/docs/" + createdDocument.getId
//
//  private def getDocumentLink(documentId: String) = "dbs/" + databaseName + "/colls/" + collectionName + "/docs/" + documentId
//}
