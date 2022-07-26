package com.jvo.datagenerator.utils

import com.devskiller.jfairy.Fairy
import com.devskiller.jfairy.producer.company.Company
import com.devskiller.jfairy.producer.person.{Person, PersonProperties}
import com.github.javafaker.{Commerce, Company, Faker}
import com.jvo.datagenerator.dto.entitydata.{DependentField, EntityMetadata}
import com.jvo.datagenerator.dto.SpecificDataFieldProperties
import com.jvo.datagenerator.services.keepers.DataKeeper
import com.jvo.datagenerator.services.keepers.RolesKeeper._
import com.vaadin.exampledata.{ExampleDataGenerator, FoodProductName}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}

import java.time.LocalDateTime
import java.util.Random
import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.math
import scala.util.matching.Regex

object DataGeneratorUtils {

  private val JFairyGenerator = Fairy.create()
  private val GenericDataGenerator = JFairyGenerator.baseProducer()
  private val TextGenerator = JFairyGenerator.textProducer()
  private val DateGenerator = JFairyGenerator.dateProducer()
  private val NetworkGenerator = JFairyGenerator.networkProducer()
  private val FakerGenerator = new Faker()
  private val FoodProductGenerator = new FoodProductName()

  private val personFieldPattern: Regex = "(PERSON_)\\w+".r
  private val purchaseFieldPattern: Regex = "(PURCHASE_)\\w+".r
  private val companyFieldPattern: Regex = "(COMPANY_)\\w+".r
  private val productFieldPattern: Regex = "(PRODUCT_)\\w+".r

  def getIdNumber(numberType: String): Long = {
    numberType match {
      case "int" => FakerGenerator.number().digits(8).toInt
      case "long" => FakerGenerator.number().digits(8).toLong
    }
  }

  def getPositiveNumber(numberType: String): Double = {
    numberType match {
      case "int" => FakerGenerator.number().randomDigitNotZero()
      case "long" => FakerGenerator.number().numberBetween(0, Long.MaxValue)
      case "double" => FakerGenerator.number().randomDouble(2, 0, Long.MaxValue)
    }
  }

  def mapToGenericRecord(entityMetadata: EntityMetadata): Record = {

    val entityBuilder = new GenericRecordBuilder(entityMetadata.schema)

    setGenericEntityFields(entityMetadata, entityBuilder)
    setGenericRecordSpecificFields(entityBuilder, entityMetadata.specificDataFields)
    setDependentFields(entityMetadata, entityBuilder)

    val record = entityBuilder.build()
    record
  }

  def mapToCompanyRecord(entityMetadata: EntityMetadata): Record = {

    val entityBuilder = new GenericRecordBuilder(entityMetadata.schema)

    setGenericEntityFields(entityMetadata, entityBuilder)
    setCompanyRecordSpecificFields(entityBuilder, entityMetadata.specificDataFields)
    setDependentFields(entityMetadata, entityBuilder)

    val record = entityBuilder.build()
    record
  }

  def mapToPersonRecord(entityMetadata: EntityMetadata): Record = {

    val entityBuilder = new GenericRecordBuilder(entityMetadata.schema)

    setGenericEntityFields(entityMetadata, entityBuilder)
    setPersonRecordSpecificFields(entityBuilder, entityMetadata.specificDataFields)
    setDependentFields(entityMetadata, entityBuilder)

    val record = entityBuilder.build()
    record
  }

  def mapToProductRecord(entityMetadata: EntityMetadata): Record = {

    val entityBuilder = new GenericRecordBuilder(entityMetadata.schema)

    setGenericEntityFields(entityMetadata, entityBuilder)
    setProductRecordSpecificFields(entityBuilder, entityMetadata.specificDataFields)
    setDependentFields(entityMetadata, entityBuilder)

    val record = entityBuilder.build()
    record
  }

  def mapToPurchaseRecord(entityMetadata: EntityMetadata): Record = {
    val entityBuilder = new GenericRecordBuilder(entityMetadata.schema)

    setGenericEntityFields(entityMetadata, entityBuilder)
    setPurchaseRecordSpecificFields(entityBuilder, entityMetadata.specificDataFields)
    setDependentFields(entityMetadata, entityBuilder)

    val record = entityBuilder.build()
    record
  }

  def updateRecord(record: Record, entityMetadata: EntityMetadata): Record = {

    val staticFields = entityMetadata.dependentFields.map(_.field.name()).toBuffer
    staticFields.addOne(entityMetadata.idFieldName)

    val fieldsToChange: mutable.Seq[Schema.Field] = entityMetadata.schema.getFields.asScala
      .filterNot(field => staticFields.contains(field.name()))

    val genericFieldsToChange: Seq[Schema.Field] = fieldsToChange
      .filterNot((field: Schema.Field) => entityMetadata.specificDataFields.exists(_.field == field))
      .toSeq

    val recordBuilder = new GenericRecordBuilder(entityMetadata.schema)

    staticFields.foreach((fieldName: String) => recordBuilder.set(fieldName, record.get(fieldName)))
    setGenericEntityFields(genericFieldsToChange, recordBuilder)

    setSpecificFieldsByEntityRole(entityMetadata, recordBuilder)

    recordBuilder.build()

  }

  private def setSpecificFieldsByEntityRole(entityMetadata: EntityMetadata, recordBuilder: GenericRecordBuilder): Unit = {

    entityMetadata.role match {
      case PersonRole => setPersonRecordSpecificFields(recordBuilder, entityMetadata.specificDataFields)
      case CompanyRole => setCompanyRecordSpecificFields(recordBuilder, entityMetadata.specificDataFields)
      case ProductRole => setProductRecordSpecificFields(recordBuilder, entityMetadata.specificDataFields)
      case PurchaseRole => setPurchaseRecordSpecificFields(recordBuilder, entityMetadata.specificDataFields)
      case GenericRole => setGenericRecordSpecificFields(recordBuilder, entityMetadata.specificDataFields)
    }
  }

  private def setDependentFields(entityMetadata: EntityMetadata, entityBuilder: GenericRecordBuilder): Unit = {
    entityMetadata.dependentFields.foreach {
      dependentField: DependentField => setDependentFieldValue(entityBuilder, dependentField)
    }
  }

  private def setGenericEntityFields(fields: Seq[Schema.Field], entityBuilder: GenericRecordBuilder): Unit = {
    fields.foreach {
      field: Schema.Field => setGenericFieldValue(entityBuilder, field)
    }
  }

  private def setGenericEntityFields(entityMetadata: EntityMetadata, entityBuilder: GenericRecordBuilder): Unit = {
    val genericEntityFields = getGenericEntityFields(entityMetadata)
    genericEntityFields.foreach {
      field: Schema.Field => setGenericFieldValue(entityBuilder, field)
    }
  }

  def getGenericEntityFields(entityMetadata: EntityMetadata): Seq[Schema.Field] = {
    val entityFields: Seq[Schema.Field] = entityMetadata.schema.getFields.asScala.toSeq
    val genericEntityFields: Seq[Schema.Field] = entityFields.filterNot(entityField => isDependentOrSpecificField(entityMetadata, entityField))
    genericEntityFields
  }

  private def isDependentOrSpecificField(entityMetadata: EntityMetadata, entityField: Schema.Field) = {
    entityMetadata.dependentFields.exists(_.field.name() == entityField.name()) ||
      entityMetadata.specificDataFields.exists(_.field.name() == entityField.name())
  }

  private def setGenericFieldValue(builder: GenericRecordBuilder, field: Schema.Field): Unit = {
    val fieldSchema = field.schema()

    val value = getNormalizedFieldType(fieldSchema) match {
      case "INT" =>
        GenericDataGenerator.randomBetween(1, Int.MaxValue)
      case "LONG" =>
        GenericDataGenerator.randomBetween(1, Long.MaxValue)
      case "FLOAT" =>
        GenericDataGenerator.randomBetween(1, Float.MaxValue).toFloat
      case "DOUBLE" =>
        GenericDataGenerator.randomBetween(1, Double.MaxValue)
      case _ =>
        TextGenerator.randomString(10)
    }
    builder.set(field.name(), value)
  }

  private def getNormalizedFieldType(schema: Schema) = {
    schema.getType.toString.toUpperCase
  }

  def setProductRecordSpecificFields(entityBuilder: GenericRecordBuilder, specificFields: Set[SpecificDataFieldProperties]): Unit = {

    val product = FakerGenerator.commerce()

    specificFields.foreach { specificDataField =>
      val value: Any = getProductFieldValue(product, specificDataField)
      entityBuilder.set(specificDataField.field.name(), value)
    }
  }

  private def getProductFieldValue(product: Commerce, specificDataField: SpecificDataFieldProperties) = {
    specificDataField.fieldRole match {
      case ProductNameRole => if (GenericDataGenerator.randomBetween(1, 10) > 7) product.productName() else getFoodProductName
      case ProductPriceRole => product.price(2, 200).toDouble
      case ProductCategoryRole => product.department()
      case ProductDescriptionRole => s"This product made of ${product.material()} and has ${product.color()}"
      case _ => println(s"No role found: ${specificDataField.field.name()}")
    }
  }

  def setCompanyRecordSpecificFields(entityBuilder: GenericRecordBuilder, specificFields: Set[SpecificDataFieldProperties]): Unit = {

    val company: com.github.javafaker.Company = FakerGenerator.company()
    val fairyCompany: com.devskiller.jfairy.producer.company.Company = JFairyGenerator.company()

    specificFields.foreach { specificDataField =>
      val value: Any = getValueForCompanyField(company, fairyCompany, specificDataField)
      entityBuilder.set(specificDataField.field.name(), value)
    }
  }

  private def getValueForCompanyField(company: com.github.javafaker.Company,
                                      fairyCompany: com.devskiller.jfairy.producer.company.Company,
                                      specificDataField: SpecificDataFieldProperties): Any = {
    specificDataField.fieldRole match {
      case CompanyNameRole => company.name()
      case CompanyPhoneRole => FakerGenerator.phoneNumber().phoneNumber()
      case CompanyWebsiteRole => company.url()
      case CompanyEmailRole => fairyCompany.getEmail
      case CompanyAddressRole => FakerGenerator.address().fullAddress()
      case _ => println(s"No role found: ${specificDataField.field.name()}")
    }
  }

  def setPersonRecordSpecificFields(entityBuilder: GenericRecordBuilder, specificFields: Set[SpecificDataFieldProperties]): Unit = {

    val person = JFairyGenerator.person(PersonProperties.minAge(16))

    specificFields.foreach { specificDataField =>
      val value: Any = getValueForPersonField(person, specificDataField)
      entityBuilder.set(specificDataField.field.name(), value)
    }
  }

  private def getValueForPersonField(person: Person, specificDataField: SpecificDataFieldProperties) = {
    specificDataField.fieldRole match {
      case PersonFirstNameRole => person.getFirstName
      case PersonLastNameRole => person.getLastName
      case PersonMiddleNameRole => person.getMiddleName
      case PersonFullNameRole => person.getFullName
      case PersonAgeRole => person.getAge
      case PersonDateOfBirthRole => person.getDateOfBirth
      case PersonPhoneRole => person.getMobileTelephoneNumber
      case PersonEmailRole => person.getEmail
      case PersonAddressRole => person.getAddress.getAddressLine1
      case _ => println(s"No role found: ${specificDataField.field.name()}")
    }
  }

  def setPurchaseRecordSpecificFields(entityBuilder: GenericRecordBuilder, specificFields: Set[SpecificDataFieldProperties]): Unit = {

    val product = FakerGenerator.commerce()

    specificFields.foreach { specificDataField =>
      val value: Any = getValueForPurchaseField(product, specificDataField)
      entityBuilder.set(specificDataField.field.name(), value)
    }
  }

  private def getValueForPurchaseField(product: Commerce, specificDataField: SpecificDataFieldProperties) = {
    specificDataField.fieldRole match {
      case PurchasePriceRole => product.price(0.5, 99.99)
      case PurchaseQuantityRole => GenericDataGenerator.randomBetween(1, 10)
      case PurchaseDiscountRole => GenericDataGenerator.randomBetween(0.1, 9.99)
      case PurchaseSumRole => GenericDataGenerator.randomBetween(0.5, 299.99)
      case _ => println(s"No role found: ${specificDataField.field.name()}")
    }
  }

  def setDependentFieldValue(entityBuilder: GenericRecordBuilder, dependentFieldProps: DependentField): Unit = {

    val keysAmount = DataKeeper.getSizeByKey(dependentFieldProps.dependencyEntity)
    val keyIndex = FakerGenerator.number().numberBetween(0, keysAmount - 1)

    DataKeeper.getValueForKeyByIndex(dependentFieldProps.dependencyEntity, keyIndex)
      .flatMap(dependencyRecord => Option(dependencyRecord.get(dependentFieldProps.dependencyField)))
      .foreach(dependencyFieldValue => entityBuilder.set(dependentFieldProps.field.name(), dependencyFieldValue))
  }

  def getFoodProductName: String = {
    FoodProductGenerator.getValue(new Random(), FakerGenerator.number().randomDigitNotZero(), LocalDateTime.now())
  }

  def setGenericRecordSpecificFields(entityBuilder: GenericRecordBuilder, specificFields: Set[SpecificDataFieldProperties]): Unit = {

    val personRelatedFields = specificFields.filter(_.fieldRole.matches(personFieldPattern.regex))
    val productRelatedFields = specificFields.filter(_.fieldRole.matches(productFieldPattern.regex))
    val companyRelatedFields = specificFields.filter(_.fieldRole.matches(companyFieldPattern.regex))
    val purchaseRelatedFields = specificFields.filter(_.fieldRole.matches(purchaseFieldPattern.regex))

    val otherFields: Set[SpecificDataFieldProperties] = specificFields.diff(personRelatedFields ++ productRelatedFields ++ companyRelatedFields ++ purchaseRelatedFields)

    setPersonRecordSpecificFields(entityBuilder, personRelatedFields)
    setProductRecordSpecificFields(entityBuilder, productRelatedFields)
    setCompanyRecordSpecificFields(entityBuilder, companyRelatedFields)
    setPurchaseRecordSpecificFields(entityBuilder, purchaseRelatedFields)

    otherFields.foreach {
      fieldProperties => setGenericFieldValue(entityBuilder, fieldProperties.field)
    }
  }

}
